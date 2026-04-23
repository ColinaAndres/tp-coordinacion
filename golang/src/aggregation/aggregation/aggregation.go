package aggregation

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/topaggregator"
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
	topAggregator *topaggregator.TopAggregator
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

	topAggregator := topaggregator.NewTopAggregator(config.SumAmount, config.TopSize)

	return &Aggregation{
		outputQueue:   outputQueue,
		inputExchange: inputExchange,
		topAggregator: topAggregator,
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
	slog.Info("Received EOF message")
	top := aggregation.topAggregator.RegisterEOF(clientId)
	if top != nil {
		return aggregation.sendTop(clientId, top)
	}
	return nil
}

func (aggregation *Aggregation) handleDataMessage(clientId string, fruitRecords []fruititem.FruitItem, totalFruitSend int) {
	aggregation.topAggregator.Add(clientId, fruitRecords)
}

func (aggregation *Aggregation) sendTop(clientId string, top []fruititem.FruitItem) error {
	innerMessageWithTop := inner.NewDataMessage(clientId, top)
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

	aggregation.topAggregator.Clean(clientId)
	return nil
}

func (aggregation *Aggregation) Close() {
	//clean all?
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
