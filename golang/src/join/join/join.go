package join

import (
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/accumulator"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
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
	inputQueue        middleware.Middleware
	outputQueue       middleware.Middleware
	accumulator       *accumulator.Accumulator
	states            map[string]*JoinState
	aggregationAmount int
	topSize           int
}

type JoinState struct {
	EOFCount int
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

	return &Join{
		inputQueue:        inputQueue,
		outputQueue:       outputQueue,
		accumulator:       accumulator.NewAccumulator(),
		states:            map[string]*JoinState{},
		aggregationAmount: config.AggregationAmount,
		topSize:           config.TopSize,
	}, nil
}

func (join *Join) Run() {
	join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	innerMessage, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()
		return
	}

	if innerMessage.IsEOF {
		if err := join.handleEndOfRecordsMessage(innerMessage.ClientId); err != nil {
			slog.Error("While handling end of records message", "err", err)
			nack()
		}
		return
	}

	join.handleDataMessage(innerMessage.ClientId, innerMessage.FruitRecords)
}

func (join *Join) handleDataMessage(clientId string, fruitRecords []fruititem.FruitItem) {
	join.accumulator.AddFruitItems(clientId, fruitRecords)
}

func (join *Join) handleEndOfRecordsMessage(clientId string) error {
	slog.Info("Received EOF message from client", "clientId", clientId)
	state := join.getState(clientId)
	state.EOFCount++
	if state.EOFCount != join.aggregationAmount {
		return nil
	}
	delete(join.states, clientId)

	fruitItems, _ := join.accumulator.RemoveClientFruitItems(clientId)
	fruitTopRecords := buildFruitTop(fruitItems, join.topSize)

	innerMessageWithTop := inner.NewInnerMessage(clientId, fruitTopRecords, false)
	message, err := inner.SerializeMessage(innerMessageWithTop)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := join.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}

	eofMessage := inner.NewInnerMessage(clientId, []fruititem.FruitItem{}, true)
	message, err = inner.SerializeMessage(eofMessage)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := join.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}

	return nil
}

func buildFruitTop(fruitItems []fruititem.FruitItem, topSize int) []fruititem.FruitItem {
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})

	finalTopSize := min(topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}

func (join *Join) getState(clientId string) *JoinState {
	state, exists := join.states[clientId]
	if !exists {
		state = &JoinState{}
		join.states[clientId] = state
	}

	return state
}
