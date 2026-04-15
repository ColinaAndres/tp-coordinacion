package aggregation

import (
	"fmt"
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/accumulator"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	outputQueue   middleware.Middleware
	inputExchange middleware.Middleware
	sumExchange   middleware.Middleware
	accumulator   *accumulator.Accumulator
	topSize       int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	sumExchangeRouteKeys := []string{config.SumPrefix}
	sumExchange, err := middleware.CreateExchangeMiddleware(config.SumPrefix, sumExchangeRouteKeys, connSettings)
	if err != nil {
		outputQueue.Close()
		inputExchange.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:   outputQueue,
		inputExchange: inputExchange,
		sumExchange:   sumExchange,
		accumulator:   accumulator.NewAccumulator(),
		topSize:       config.TopSize,
	}, nil
}

func (aggregation *Aggregation) Run() {
	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	innerMessage, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()
		return
	}

	if innerMessage.IsEOF {
		if err := aggregation.handleEndOfRecordsMessage(innerMessage.ClientId); err != nil {
			slog.Error("While handling end of record message", "err", err)
			nack()
		}
		return
	}

	aggregation.handleDataMessage(innerMessage.ClientId, innerMessage.FruitRecords)
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientId string) error {
	slog.Info("Received End Of Records message")

	fruitTopRecords := aggregation.buildFruitTop(clientId)
	innerMessageWithTop := inner.NewInnerMessage(clientId, fruitTopRecords, false)
	message, err := inner.SerializeMessage(innerMessageWithTop)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}

	eofMessage := inner.NewInnerMessage(clientId, []fruititem.FruitItem{}, true)
	message, err = inner.SerializeMessage(eofMessage)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}

	aggregation.accumulator.RemoveClientFruitItems(clientId)
	return nil
}

func (aggregation *Aggregation) handleDataMessage(clientId string, fruitRecords []fruititem.FruitItem) {
	aggregation.accumulator.AddFruitItems(clientId, fruitRecords)
}

func (aggregation *Aggregation) buildFruitTop(clientId string) []fruititem.FruitItem {
	// TODO: podria devolver nil y false si no existe el cliente, revisar si es necesario
	fruitItems, _ := aggregation.accumulator.GetClientFruitItems(clientId)
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
