package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

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
	id                    int
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
		id:                    config.Id,
		inputQueue:            inputQueue,
		outputExchanges:       outputExchanges,
		communicationExchange: communicationExchange,
		accumulator:           accumulator.NewAccumulator(),
	}, nil
}

func (sum *Sum) Run() {
	defer sum.Close()

	go sum.handleSignal()

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
			return
		}
		ack()
		return
	}

	if err := sum.handleDataMessage(innerMessage.ClientId, innerMessage.FruitRecords); err != nil {
		slog.Error("While handling data message", "err", err)
		nack()
		return
	}
	ack()
}

func (sum *Sum) handleDataMessage(clientId string, fruitRecords []fruititem.FruitItem) error {
	clientDone, totalAdded, itemsToFlush := sum.accumulator.AddFruitItems(clientId, fruitRecords)
	if clientDone {
		if err := sum.sendCountUpdateToPeers(clientId, totalAdded); err != nil {
			return err
		}
	}
	if itemsToFlush != nil {
		return sum.handleEndOfRecordMessage(clientId, totalAdded, itemsToFlush)
	}
	return nil
}

func (sum *Sum) notifyEOF(clientId string, totalFruitSended int) error {
	slog.Info("Notifying other sum nodes about EOF")
	eofMessage := inner.NewEOFMessage(clientId, totalFruitSended)
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

func (sum *Sum) handleEndOfRecordMessage(clientId string, totalFruitSend int, itemsToFlush []fruititem.FruitItem) error {
	slog.Info("Received End Of Records message")

	for _, fruitItem := range itemsToFlush {
		fruitRecord := []fruititem.FruitItem{fruitItem}
		innerMessage := inner.NewDataMessage(clientId, fruitRecord)

		message, err := inner.SerializeMessage(innerMessage)
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}

		h := fnv.New32a()
		h.Write([]byte(fruitItem.Fruit))
		selected_exchange := h.Sum32() % uint32(len(sum.outputExchanges))

		if err := sum.outputExchanges[selected_exchange].Send(*message); err != nil {
			return err
		}
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

	sum.accumulator.CleanClient(clientId)
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

	if innerMessage.IsEOFMessage() {
		slog.Info("Received EOF message")

		if err := sum.handleEOFfromCommunication(innerMessage.ClientId, innerMessage.TotalFruitSend); err != nil {
			slog.Error("While handling end of record message", "err", err)
			nack()
			return
		}
		ack()
		return
	}

	if innerMessage.IsCommunicationMessage() && innerMessage.SenderId == sum.id {
		slog.Info("Received own communication message, ignoring")
		ack()
		return
	}

	if err := sum.handleCommunicationMessage(innerMessage.ClientId, innerMessage.TotalFruitSend); err != nil {
		slog.Error("Received communication message, but no handler for it")
		nack()
		return
	}

	ack()
}

func (sum *Sum) handleEOFfromCommunication(clientId string, totalFruitSended int) error {
	slog.Info("Handling EOF message from communication exchange")
	totalReceived, itemsToFlush := sum.accumulator.MarkClientAsDone(clientId, totalFruitSended)

	if err := sum.sendCountUpdateToPeers(clientId, totalReceived); err != nil {
		return err
	}

	if itemsToFlush != nil {
		return sum.handleEndOfRecordMessage(clientId, totalFruitSended, itemsToFlush)
	}
	return nil
}

func (sum *Sum) handleCommunicationMessage(clientId string, peerCount int) error {
	slog.Info("Handling communication message from communication exchange")
	itemsToFlush := sum.accumulator.AddPeerCount(clientId, peerCount)

	if itemsToFlush != nil {
		return sum.handleEndOfRecordMessage(clientId, 0, itemsToFlush) // no deberia ser relevante el segundo paramtero
	}
	return nil
}

func (sum *Sum) sendCountUpdateToPeers(clientId string, totalAdded int) error {
	slog.Info("Notifying other sum nodes about new count")
	communicationMessage := inner.NewCommunicationMessage(clientId, sum.id, totalAdded)
	message, err := inner.SerializeMessage(communicationMessage)
	if err != nil {
		slog.Debug("While serializing communication message", "err", err)
		return err
	}
	if err := sum.communicationExchange.Send(*message); err != nil {
		slog.Debug("While sending communication message", "err", err)
		return err
	}
	return nil
}

func (sum *Sum) Close() {
	sum.inputQueue.Close()
	for _, exchange := range sum.outputExchanges {
		exchange.Close()
	}
	sum.communicationExchange.Close()
}

func (sum *Sum) handleSignal() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	sum.Close()
}
