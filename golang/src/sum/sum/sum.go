package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

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
	accumulator           *Accumulator
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
		accumulator:           NewAccumulator(),
	}, nil
}

func (sum *Sum) Run() {
	defer sum.Close()

	go sum.handleSignal()

	go sum.communicationExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
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

	if err := innerMessage.Execute(sum); err != nil {
		slog.Error("While executing message", "err", err)
		nack()
		return
	}

	ack()
}

func (sum *Sum) HandleDataMessage(clientId string, fruitRecords []fruititem.FruitItem) error {
	clientDone, totalAdded, itemsToFlush := sum.accumulator.AddFruitItems(clientId, fruitRecords)
	if clientDone {
		if err := sum.sendCountUpdateToPeers(clientId, totalAdded); err != nil {
			return err
		}
	}
	if itemsToFlush != nil {
		return sum.sendProcessedData(clientId, itemsToFlush)
	}
	return nil
}

func (sum *Sum) HandleEOFMessage(clientId string, totalFruitSended int) error {
	slog.Info("Notifying other sum nodes about EOF")
	eofMessage := inner.NewBroadcastEOFMessage(clientId, totalFruitSended)
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

func (sum *Sum) sendProcessedData(clientId string, itemsToFlush []fruititem.FruitItem) error {
	slog.Info("Sending processed data to aggregation nodes")

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
		eofMessage := inner.NewEOFMessage(clientId, len(itemsToFlush))

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

func (sum *Sum) HandleBroadcastEOFMessage(clientId string, totalFruitSended int) error {
	slog.Info("Handling EOF message from communication exchange")
	totalReceived, itemsToFlush := sum.accumulator.MarkClientAsDone(clientId, totalFruitSended)

	if err := sum.sendCountUpdateToPeers(clientId, totalReceived); err != nil {
		return err
	}

	if itemsToFlush != nil {
		return sum.sendProcessedData(clientId, itemsToFlush)
	}
	return nil
}

func (sum *Sum) HandleCommunicationMessage(clientId string, peerCount int) error {
	slog.Info("Handling communication message from communication exchange")
	itemsToFlush := sum.accumulator.AddPeerCount(clientId, peerCount)

	if itemsToFlush != nil {
		return sum.sendProcessedData(clientId, itemsToFlush)
	}
	return nil
}

// Funcion necesaria para implementar CommunicationHandler, no se usa directamente
func (sum *Sum) Id() int {
	return sum.id
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
