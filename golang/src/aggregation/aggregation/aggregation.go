package aggregation

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruitmap"
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
	fruitMaps     map[string]*fruitmap.FruitMap
	clientsEOF    map[string]int
	sumAmount     int
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

	return &Aggregation{
		outputQueue:   outputQueue,
		inputExchange: inputExchange,
		fruitMaps:     map[string]*fruitmap.FruitMap{},
		clientsEOF:    map[string]int{},
		sumAmount:     config.SumAmount,
		topSize:       config.TopSize,
	}, nil
}

func (aggregation *Aggregation) Run() {
	defer aggregation.Close()

	go aggregation.handleSignal()

	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	innerMessage, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()
		return
	}

	if innerMessage.IsEOFMessage() {
		if err := aggregation.handleEndOfRecordsMessage(innerMessage.ClientId, innerMessage.TotalFruitSend); err != nil {
			slog.Error("While handling end of record message", "err", err)
			nack()
			return
		}
		ack()
		return
	}

	aggregation.handleDataMessage(innerMessage.ClientId, innerMessage.FruitRecords, innerMessage.TotalFruitSend)
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientId string, totalFruitSend int) error {
	slog.Info("Received End Of Records message", "clientId", clientId, "total de registros", totalFruitSend)

	clientEOFCount, ok := aggregation.clientsEOF[clientId]
	if !ok {
		clientEOFCount = 0
		aggregation.clientsEOF[clientId] = clientEOFCount
	}

	aggregation.clientsEOF[clientId] = clientEOFCount + 1

	if aggregation.clientsEOF[clientId] == aggregation.sumAmount {
		return aggregation.sendTop(clientId)
	}
	return nil
}

func (aggregation *Aggregation) handleDataMessage(clientId string, fruitRecords []fruititem.FruitItem, totalFruitSend int) {
	clientMap, ok := aggregation.fruitMaps[clientId]
	if !ok {
		clientMap = fruitmap.NewFruitMap()
		aggregation.fruitMaps[clientId] = clientMap
	}
	clientMap.Add(fruitRecords)
}

func (aggregation *Aggregation) buildFruitTop(clientId string) []fruititem.FruitItem {
	fruitMap, ok := aggregation.fruitMaps[clientId]
	if !ok {
		return []fruititem.FruitItem{}
	}
	return fruitMap.Top(aggregation.topSize)
}

func (aggregation *Aggregation) sendTop(clientId string) error {
	fruitTopRecords := aggregation.buildFruitTop(clientId)
	innerMessageWithTop := inner.NewDataMessage(clientId, fruitTopRecords)
	message, err := inner.SerializeMessage(innerMessageWithTop)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}

	eofMessage := inner.NewEOFMessage(clientId, 0)
	message, err = inner.SerializeMessage(eofMessage)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}

	delete(aggregation.fruitMaps, clientId)
	return nil
}

func (aggregation *Aggregation) Close() {
	aggregation.outputQueue.Close()
	aggregation.inputExchange.Close()
}

func (aggregation *Aggregation) handleSignal() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	aggregation.Close()
}
