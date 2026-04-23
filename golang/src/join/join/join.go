package join

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/topaggregator"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue    middleware.Middleware
	outputQueue   middleware.Middleware
	topAggregator *topaggregator.TopAggregator
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	topAggregagator := topaggregator.NewTopAggregator(config.AggregationAmount, config.TopSize)

	return &Join{
		inputQueue:    inputQueue,
		outputQueue:   outputQueue,
		topAggregator: topAggregagator,
	}, nil
}

func (join *Join) Run() {
	defer join.Close()

	go join.handleSignal()

	join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	innerMessage, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()
		return
	}

	if innerMessage.IsEOFMessage() {
		if err := join.handleEndOfRecordsMessage(innerMessage.ClientId); err != nil {
			slog.Error("While handling end of records message", "err", err)
			nack()
			return
		}
		ack()
		return
	}

	join.handleDataMessage(innerMessage.ClientId, innerMessage.FruitRecords)
	ack()
}

func (join *Join) handleDataMessage(clientId string, fruitRecords []fruititem.FruitItem) {
	join.topAggregator.Add(clientId, fruitRecords)
}

func (join *Join) handleEndOfRecordsMessage(clientId string) error {
	slog.Info("Received EOF message")
	top := join.topAggregator.RegisterEOF(clientId)
	if top != nil {
		return join.sendTop(clientId, top)
	}
	return nil
}

func (join *Join) sendTop(clientId string, top []fruititem.FruitItem) error {
	innerMessageWithTop := inner.NewDataMessage(clientId, top)
	message, err := inner.SerializeMessage(innerMessageWithTop)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := join.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}
	return nil
}

func (join *Join) Close() {
	join.inputQueue.Close()
	join.outputQueue.Close()
}

func (join *Join) handleSignal() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	join.Close()
}
