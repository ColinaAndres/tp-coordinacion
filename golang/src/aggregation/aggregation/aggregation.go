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
	outputQueue          middleware.Middleware
	inputExchange        middleware.Middleware
	sumExchange          middleware.Middleware
	comunicationExchange middleware.Middleware
	accumulator          *accumulator.Accumulator
	states               map[string]*QueryState
	sumAmount            int
	topSize              int
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

	comunicationExchangeRouteKeys := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	comunicationExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, aggrExchangeRouteKeys, connSettings)
	if err != nil {
		outputQueue.Close()
		inputExchange.Close()
		sumExchange.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:          outputQueue,
		inputExchange:        inputExchange,
		sumExchange:          sumExchange,
		comunicationExchange: comunicationExchange,
		accumulator:          accumulator.NewAccumulator(),
		states:               map[string]*QueryState{},
		sumAmount:            config.SumAmount,
		topSize:              config.TopSize,
	}, nil
}

func (aggregation *Aggregation) Run() {

	go aggregation.comunicationExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleComunication(msg, ack, nack)
	})

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
		if err := aggregation.handleEndOfRecordsMessage(innerMessage.ClientId, innerMessage.TotalFruitSend); err != nil {
			slog.Error("While handling end of record message", "err", err)
			nack()
		}
		return
	}

	aggregation.handleDataMessage(innerMessage.ClientId, innerMessage.FruitRecords, innerMessage.TotalFruitSend)
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientId string, totalFruitSend int) error {
	slog.Info("Received End Of Records message")

	//TODO: encapsulable para quedarse con el mayor de total en caso de errores raros
	state := aggregation.getState(clientId)
	state.EOFcount++
	state.TargetCounts += totalFruitSend

	if !(state.EOFcount == aggregation.sumAmount && state.ReceivedCount == state.TargetCounts) {
		return nil
	}

	cleanUpMessage := inner.InnerMessage{
		ClientId:       clientId,
		FruitRecords:   []fruititem.FruitItem{},
		IsEOF:          false,
		IsCleanUp:      true,
		TotalFruitSend: 0,
	}

	message, err := inner.SerializeMessage(cleanUpMessage)
	if err != nil {
		slog.Debug("While serializing clean up message", "err", err)
		return err
	}
	if err := aggregation.sumExchange.Send(*message); err != nil {
		slog.Debug("While sending clean up message", "err", err)
		return err
	}

	fruitTopRecords := aggregation.buildFruitTop(clientId)
	innerMessageWithTop := inner.NewInnerMessage(clientId, fruitTopRecords, false)
	message, err = inner.SerializeMessage(innerMessageWithTop)
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

func (aggregation *Aggregation) handleDataMessage(clientId string, fruitRecords []fruititem.FruitItem, totalFruitSend int) {
	aggregation.accumulator.AddFruitItems(clientId, fruitRecords)
	state := aggregation.getState(clientId)
	state.ReceivedCount += totalFruitSend
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

func (aggregation *Aggregation) getState(clientId string) *QueryState {
	state, exists := aggregation.states[clientId]
	if !exists {
		state = &QueryState{}
		aggregation.states[clientId] = state
	}
	return state
}

func (aggregation *Aggregation) handleComunication(msg middleware.Message, ack func(), nack func()) {
	slog.Info("Received comunication message")

	comunicationMessage, err := inner.aggregatorMessageDeserialize(&msg)
	if err != nil {
		slog.Error("While deserializing comunication message", "err", err)
		nack()
		return
	}

	aggregation.handleClientCountUpdate(comunicationMessage.ClientId, comunicationMessage.ClientCount)

}

func (aggregation *Aggregation) handleClientCountUpdate(clientId string, clientCount int) {
	state := aggregation.getState(clientId)
	state.ReceivedCount += clientCount

	if state.ReceivedCount == state.TargetCounts {
		aggregation.sendTop(clientId)
	}
}
