package aggregation

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"

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
	outputQueue           middleware.Middleware
	inputExchange         middleware.Middleware
	sumExchange           middleware.Middleware
	aggrExchange          middleware.Middleware
	communicationExchange middleware.Middleware
	accumulator           *accumulator.Accumulator
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
		outputQueue:           outputQueue,
		inputExchange:         inputExchange,
		sumExchange:           sumExchange,
		communicationExchange: communicationExchange,
		aggrExchange:          aggrExchange,
		accumulator:           accumulator.NewAccumulator(),
		stateManager:          NewStateManager(config.SumAmount),
		sumAmount:             config.SumAmount,
		topSize:               config.TopSize,
	}, nil
}

func (aggregation *Aggregation) Run() {
	go aggregation.handleSignal()

	go aggregation.communicationExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
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

	if innerMessage.IsEOFMessage() {
		if err := aggregation.handleEndOfRecordsMessage(innerMessage.ClientId, innerMessage.TotalFruitSend); err != nil {
			slog.Error("While handling end of record message", "err", err)
			nack()
		}
		return
	}

	aggregation.handleDataMessage(innerMessage.ClientId, innerMessage.FruitRecords, innerMessage.TotalFruitSend)
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientId string, totalFruitSend int) error {
	slog.Info("Received End Of Records message", "clientId", clientId, "total de registros", totalFruitSend)

	aggregation.stateManager.MarkEOF(clientId, totalFruitSend)
	if !aggregation.stateManager.DoneWaiting(clientId) {
		return nil
	}

	aggregation.notifyClientCountUpdate(clientId, aggregation.stateManager.GetReceivedCount(clientId))

	if aggregation.stateManager.DoneReceiving(clientId) {
		slog.Info("LO recibido es igual al total en end of record handler, enviando top")
		if err := aggregation.sendTop(clientId); err != nil {
			slog.Error("While sending top message", "err", err)
			return err
		}
	}
	// if !(state.EOFcount == aggregation.sumAmount && state.ReceivedCount == state.TargetCounts) {
	// 	return nil
	// }

	// cleanUpMessage := inner.InnerMessage{
	// 	ClientId:       clientId,
	// 	FruitRecords:   []fruititem.FruitItem{},
	// 	IsEOF:          false,
	// 	IsCleanUp:      true,
	// 	TotalFruitSend: 0,
	// }

	// message, err := inner.SerializeMessage(cleanUpMessage)
	// if err != nil {
	// 	slog.Debug("While serializing clean up message", "err", err)
	// 	return err
	// }
	// if err := aggregation.sumExchange.Send(*message); err != nil {
	// 	slog.Debug("While sending clean up message", "err", err)
	// 	return err
	// }

	// fruitTopRecords := aggregation.buildFruitTop(clientId)
	// innerMessageWithTop := inner.NewInnerMessage(clientId, fruitTopRecords, false)
	// message, err = inner.SerializeMessage(innerMessageWithTop)
	// if err != nil {
	// 	slog.Debug("While serializing top message", "err", err)
	// 	return err
	// }
	// if err := aggregation.outputQueue.Send(*message); err != nil {
	// 	slog.Debug("While sending top message", "err", err)
	// 	return err
	// }

	// eofMessage := inner.NewInnerMessage(clientId, []fruititem.FruitItem{}, true)
	// message, err = inner.SerializeMessage(eofMessage)
	// if err != nil {
	// 	slog.Debug("While serializing EOF message", "err", err)
	// 	return err
	// }
	// if err := aggregation.outputQueue.Send(*message); err != nil {
	// 	slog.Debug("While sending EOF message", "err", err)
	// 	return err
	// }

	// aggregation.accumulator.RemoveClientFruitItems(clientId)
	return nil
}

func (aggregation *Aggregation) handleDataMessage(clientId string, fruitRecords []fruititem.FruitItem, totalFruitSend int) {
	aggregation.accumulator.AddFruitItems(clientId, fruitRecords)
	aggregation.stateManager.AddLocalCount(clientId, totalFruitSend)

	if aggregation.stateManager.DoneReceiving(clientId) {
		slog.Info("NO DEBERIA ENTRAR AQUI, ES EL CASO BORDE")
		aggregation.notifyClientCountUpdate(clientId, totalFruitSend)
		if err := aggregation.sendTop(clientId); err != nil {
			slog.Error("While sending top message", "err", err)
		}
	}
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

func (aggregation *Aggregation) handleComunication(msg middleware.Message, ack func(), nack func()) {
	slog.Info("Received comunication message")

	comunicationMessage, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing comunication message", "err", err)
		nack()
		return
	}

	aggregation.handleClientCountUpdate(comunicationMessage.ClientId, comunicationMessage.TotalFruitSend)

}

func (aggregation *Aggregation) handleClientCountUpdate(clientId string, clientCount int) {
	slog.Info("Handling client count update", "clientId", clientId, "clientCount", clientCount, "my count", aggregation.stateManager.GetReceivedCount(clientId))
	aggregation.stateManager.AddPeersCount(clientId, clientCount)

	if aggregation.stateManager.DoneReceiving(clientId) {
		aggregation.sendTop(clientId)
	}
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

func (aggregation *Aggregation) notifyClientCountUpdate(clientId string, totalFruitReceived int) error {
	slog.Info("Notificacion de cliente", "clientId", clientId, "totalFruitReceived", totalFruitReceived)
	comunicationMessage := inner.NewCommunicationMessage(clientId, totalFruitReceived)
	message, err := inner.SerializeMessage(comunicationMessage)
	if err != nil {
		slog.Debug("While serializing comunication message", "err", err)
		return err
	}

	if err := aggregation.aggrExchange.Send(*message); err != nil {
		slog.Debug("While sending comunication message", "err", err)
		return err
	}

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
