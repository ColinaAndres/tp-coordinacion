package sum

import (
	"fmt"
	"log/slog"

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
	inputQueue     middleware.Middleware
	outputExchange middleware.Middleware
	fruitItemMap   map[string]map[string]fruititem.FruitItem
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

	return &Sum{
		inputQueue:     inputQueue,
		outputExchange: outputExchange,
		fruitItemMap:   map[string]map[string]fruititem.FruitItem{},
	}, nil
}

func (sum *Sum) Run() {
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
		if err := sum.handleEndOfRecordMessage(); err != nil {
			slog.Error("While handling end of record message", "err", err)
			nack()
		}
		return
	}

	if err := sum.handleDataMessage(innerMessage.ClientId, innerMessage.FruitRecords); err != nil {
		slog.Error("While handling data message", "err", err)
		nack()
	}
}

func (sum *Sum) handleEndOfRecordMessage() error {
	slog.Info("Received End Of Records message")
	for key := range sum.fruitItemMap {
		fruitRecord := []fruititem.FruitItem{sum.fruitItemMap[key]}
		message, err := inner.SerializeMessage(fruitRecord)
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		if err := sum.outputExchange.Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	eofMessage := []fruititem.FruitItem{}
	message, err := inner.SerializeMessage(eofMessage)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}

func (sum *Sum) handleDataMessage(clientId string, fruitRecords []fruititem.FruitItem) error {
	if _, ok := sum.fruitItemMap[clientId]; !ok {
		sum.fruitItemMap[clientId] = map[string]fruititem.FruitItem{}
	}

	clientMap := sum.fruitItemMap[clientId]

	for _, fruitRecord := range fruitRecords {
		_, ok := clientMap[fruitRecord.Fruit]
		if ok {
			clientMap[fruitRecord.Fruit] = clientMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			clientMap[fruitRecord.Fruit] = fruitRecord
		}
	}
	return nil
}
