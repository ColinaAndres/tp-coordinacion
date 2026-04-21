package messagehandler

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"

	"fmt"
	"math/rand"
	"time"
)

const (
	defaultSerializedAmount = 0
)

type MessageHandler struct {
	clientId        string
	fruitSerialized int
}

func NewMessageHandler() MessageHandler {
	return MessageHandler{clientId: createClientID()}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	data := []fruititem.FruitItem{fruitRecord}
	innerMessage := inner.NewDataMessage(messageHandler.clientId, data)
	msg, err := inner.SerializeMessage(innerMessage)
	if err != nil {
		return nil, err
	}
	messageHandler.fruitSerialized++
	return msg, nil
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	innerMessage := inner.NewEOFMessage(messageHandler.clientId, messageHandler.fruitSerialized)

	msg, err := inner.SerializeMessage(innerMessage)
	if err != nil {
		return nil, err
	}

	messageHandler.fruitSerialized = defaultSerializedAmount
	return msg, nil
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	innerMessage, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}

	if innerMessage.ClientId != messageHandler.clientId {
		return nil, nil
	}
	return innerMessage.FruitRecords, nil
}

// Crea un ID tipo timestamp con cierto desfase para evitar colisiones entre clientes
func createClientID() string {
	now := time.Now().UnixNano()
	randomPart := rand.Intn(10000)
	return fmt.Sprintf("%d-%d", now, randomPart)
}
