package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/accumulator"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	inputQueue            middleware.Middleware
	outputExchanges       []middleware.Middleware
	communicationExchange middleware.Middleware
	accumulator           *accumulator.Accumulator
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchanges := make([]middleware.Middleware, config.AggregationAmount)
	for i := range config.AggregationAmount {
		exchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, []string{outputExchangeRouteKeys[i]}, connSettings)
		if err != nil {
			inputQueue.Close()
			for j := 0; j < i; j++ {
				outputExchanges[j].Close()
			}
			return nil, err
		}
		outputExchanges[i] = exchange
	}

	communicationExchangeRouteKeys := []string{config.SumPrefix}
	communicationExchange, err := middleware.CreateExchangeMiddleware(config.SumPrefix, communicationExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		for _, exchange := range outputExchanges {
			exchange.Close()
		}
		return nil, err
	}

	return &Sum{
		inputQueue:            inputQueue,
		outputExchanges:       outputExchanges,
		communicationExchange: communicationExchange,
		accumulator:           accumulator.NewAccumulator(),
	}, nil
}

func (sum *Sum) Run() {
	go sum.communicationExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleCommunication(msg, ack, nack)
	})

	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	innerMessage, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()
		return
	}

	if innerMessage.IsEOFMessage() {
		if err := sum.notifyEOF(innerMessage.ClientId, innerMessage.TotalFruitSend); err != nil {
			slog.Error("While notifying EOF to other sum nodes", "err", err)
			nack()
		}
		ack()
		return
	}

	if err := sum.handleDataMessage(innerMessage.ClientId, innerMessage.FruitRecords); err != nil {
		slog.Error("While handling data message", "err", err)
		nack()
	}
	ack()
}

func (sum *Sum) handleEndOfRecordMessage(clientId string, totalFruitSend int) error {
	slog.Info("Received End Of Records message")

	aggregatorCounter := make([]int, len(sum.outputExchanges))
	fruitCounter, _ := sum.accumulator.GetClientFruitCounter(clientId)
	for _, fruitCounter := range fruitCounter {
		fruitRecord := []fruititem.FruitItem{fruitCounter.FruitItem}
		innerMessage := inner.NewDataMessage(clientId, fruitRecord)
		innerMessage.TotalFruitSend = fruitCounter.Count

		message, err := inner.SerializeMessage(innerMessage)
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}

		h := fnv.New32a()
		h.Write([]byte(fruitCounter.FruitItem.Fruit))
		selected_exchange := h.Sum32() % uint32(len(sum.outputExchanges))

		if err := sum.outputExchanges[selected_exchange].Send(*message); err != nil {
			return err
		}

		aggregatorCounter[selected_exchange] += fruitCounter.Count
	}

	for _, exchange := range sum.outputExchanges {
		eofMessage := inner.InnerMessage{
			ClientId:       clientId,
			IsEOF:          true,
			TotalFruitSend: totalFruitSend,
			FruitRecords:   []fruititem.FruitItem{},
		}

		message, err := inner.SerializeMessage(eofMessage)
		if err != nil {
			slog.Debug("While serializing EOF message", "err", err)
			return err
		}

		if err := exchange.Send(*message); err != nil {
			slog.Debug("While sending EOF message", "err", err)
			return err
		}
	}

	sum.accumulator.RemoveClientFruitItems(clientId)
	return nil
}

func (sum *Sum) handleDataMessage(clientId string, fruitRecords []fruititem.FruitItem) error {
	if sum.accumulator.AddFruitItems(clientId, fruitRecords) {
		return nil
	}

	// Si no pude agregar los registros, es porque el cliente ya habia enviado EOF,
	// por ende enviamos directamente el registro al exchange de salida
	for _, fruitRecord := range fruitRecords {
		fruititems := []fruititem.FruitItem{fruitRecord}
		innerMessage := inner.NewDataMessage(clientId, fruititems)
		message, err := inner.SerializeMessage(innerMessage)
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}

		h := fnv.New32a()
		h.Write([]byte(fruitRecord.Fruit))
		selected_exchange := h.Sum32() % uint32(len(sum.outputExchanges))

		if err := sum.outputExchanges[selected_exchange].Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	return nil
}

func (sum *Sum) handleCommunication(msg middleware.Message, ack func(), nack func()) {
	slog.Info("Received message from communication exchange")
	innerMessage, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()
		return
	}

	if innerMessage.IsCleanupMessage() {
		slog.Info("Received clean up message, removing client done data")
		sum.accumulator.CleanDoneClient(innerMessage.ClientId)
		ack()
		return
	}

	if innerMessage.IsEOFMessage() {
		slog.Info("Received EOF message")
		if err := sum.handleEndOfRecordMessage(innerMessage.ClientId, innerMessage.TotalFruitSend); err != nil {
			slog.Error("While handling end of record message", "err", err)
			nack()
			return
		}
		ack()
		return
	}

	slog.Error("Received communication message, but no handler for it")
	nack()
}

func (sum *Sum) notifyEOF(clientId string, totalFruitSend int) error {
	slog.Info("Notifying other sum nodes about EOF")
	eofMessage := inner.NewEOFMessage(clientId, totalFruitSend)
	message, err := inner.SerializeMessage(eofMessage)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.communicationExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}
