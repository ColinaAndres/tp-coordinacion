package messagehandler

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"

	"fmt"
	"math/rand"
	"time"
)

type MessageHandler struct {
	ClientId string
}

func NewMessageHandler() MessageHandler {
	return MessageHandler{ClientId: createClientID()}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	data := []fruititem.FruitItem{fruitRecord}
	innerMessage := inner.NewInnerMessage(messageHandler.ClientId, data, false)
	return inner.SerializeMessage(innerMessage)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	innerMessage := inner.NewInnerMessage(messageHandler.ClientId, []fruititem.FruitItem{}, true)
	return inner.SerializeMessage(innerMessage)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	innerMessage, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}

	if innerMessage.ClientId != messageHandler.ClientId {
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
