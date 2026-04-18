package sum

import (
	"fmt"
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
	outputExchange        middleware.Middleware
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

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	communicationExchangeRouteKeys := []string{config.SumPrefix}

	// for i := range config.SumAmount {
	// 	if i == config.Id {
	// 		continue
	// 	}
	// 	communicationExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.SumPrefix, i)
	// }

	communicationExchange, err := middleware.CreateExchangeMiddleware(config.SumPrefix, communicationExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:            inputQueue,
		outputExchange:        outputExchange,
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
	defer ack()

	innerMessage, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()
		return
	}

	if innerMessage.IsEOF {
		// if err := sum.handleEndOfRecordMessage(innerMessage.ClientId, innerMessage.TotalFruitSend); err != nil {
		// 	slog.Error("While handling end of record message", "err", err)
		// 	nack()
		// }
		if err := sum.notifyEOF(innerMessage.ClientId, innerMessage.TotalFruitSend); err != nil {
			slog.Error("While notifying EOF to other sum nodes", "err", err)
			nack()
		}
		return
	}

	if err := sum.handleDataMessage(innerMessage.ClientId, innerMessage.FruitRecords); err != nil {
		slog.Error("While handling data message", "err", err)
		nack()
	}
}

func (sum *Sum) handleEndOfRecordMessage(clientId string, totalFruitSend int) error {
	slog.Info("Received End Of Records message")

	//TODO: Cuando haya varios nodos, podria pasar que reciba EOF y no tener el cliente
	fruitItems, _ := sum.accumulator.GetClientFruitItems(clientId)
	totalCount, _ := sum.accumulator.GetClientFruitItemsCount(clientId)
	for _, fruitItem := range fruitItems {
		fruitRecord := []fruititem.FruitItem{fruitItem}
		innerMessage := inner.NewInnerMessage(clientId, fruitRecord, false)
		message, err := inner.SerializeMessage(innerMessage)
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		if err := sum.outputExchange.Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	eofMessage := inner.InnerMessage{
		ClientId:       clientId,
		IsEOF:          true,
		TotalFruitSend: totalCount,
		FruitRecords:   []fruititem.FruitItem{},
	}
	message, err := inner.SerializeMessage(eofMessage)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
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
		fruitRecord := []fruititem.FruitItem{fruitRecord}
		innerMessage := inner.NewInnerMessage(clientId, fruitRecord, false)
		message, err := inner.SerializeMessage(innerMessage)
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		if err := sum.outputExchange.Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	return nil
}

func (sum *Sum) handleCommunication(msg middleware.Message, ack func(), nack func()) {
	slog.Info("Received message from communication exchange")
	defer ack()
	innerMessage, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()
		return
	}

	if innerMessage.IsCleanUp {
		slog.Info("Received clean up message, removing client done data")
		sum.accumulator.CleanDoneClient(innerMessage.ClientId)
		return
	}

	if !innerMessage.IsEOF {
		slog.Error("Received non EOF message in sum communication exchange")
		//TODO: Deberia nackear el mensaje? O simplemente ignorarlo? deberia devolver error?
		return
	}

	if err := sum.handleEndOfRecordMessage(innerMessage.ClientId, innerMessage.TotalFruitSend); err != nil {
		slog.Error("While handling end of record message", "err", err)
		nack()
	}
}

func (sum *Sum) notifyEOF(clientId string, totalFruitSend int) error {
	slog.Info("Notifying other sum nodes about EOF")
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
	if err := sum.communicationExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}
