package aggregation

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"

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
	outputQueue           middleware.Middleware
	inputExchange         middleware.Middleware
	sumExchange           middleware.Middleware
	aggrExchange          middleware.Middleware
	communicationExchange middleware.Middleware
	fruitMaps             map[string]*FruitMap
	stateManager          *StateManager
	sumAmount             int
	topSize               int
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

	communicationExchangeRouteKeys := []string{fmt.Sprintf("%s.%d", config.AggregationPrefix, config.Id)}
	communicationExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, communicationExchangeRouteKeys, connSettings)
	if err != nil {
		outputQueue.Close()
		inputExchange.Close()
		sumExchange.Close()
		return nil, err
	}

	aggExchangeRouteKeys := make([]string, config.AggregationAmount-1)
	for i := range aggExchangeRouteKeys {
		if i >= config.Id {
			aggExchangeRouteKeys[i] = fmt.Sprintf("%s.%d", config.AggregationPrefix, i+1)
		} else {
			aggExchangeRouteKeys[i] = fmt.Sprintf("%s.%d", config.AggregationPrefix, i)
		}
	}

	aggrExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, aggExchangeRouteKeys, connSettings)
	if err != nil {
		outputQueue.Close()
		inputExchange.Close()
		sumExchange.Close()
		communicationExchange.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:   outputQueue,
		inputExchange: inputExchange,
		fruitMaps:     map[string]*FruitMap{},
		sumAmount:     config.SumAmount,
		clientsEOF:    map[string]int,
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

	aggregation.ClientsEOF[clientId]++
	if aggregation.ClientsEOF[clientId] == aggregation.sumAmount {
		return aggregation.sendTop(clientId)
	}
	return nil
}

func (aggregation *Aggregation) handleDataMessage(clientId string, fruitRecords []fruititem.FruitItem, totalFruitSend int) {
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

	aggregation.accumulator.RemoveClientFruitItems(clientId)
	return nil
}

func (aggregation *Aggregation) Close() {
	aggregation.outputQueue.Close()
	aggregation.inputExchange.Close()
	aggregation.sumExchange.Close()
	aggregation.communicationExchange.Close()
	aggregation.aggrExchange.Close()
}

func (aggregation *Aggregation) handleSignal() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	aggregation.Close()
}
