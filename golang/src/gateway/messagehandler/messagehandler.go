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

// MessageHandler se encarga de serializar y deserializar
// los mensajes que se envían y reciben a través del middleware,
// manteniendo un contador de cuántos registros se han serializado para incluirlo en el mensaje EOF.
type MessageHandler struct {
	clientId          string
	recordsSerialized int
}

func NewMessageHandler() MessageHandler {
	return MessageHandler{clientId: createClientID()}
}

// SerializeDataMessage toma un registro de fruta y lo convierte en un mensaje serializado
// y suma 1 al contador de registros serializados.
func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	data := []fruititem.FruitItem{fruitRecord}
	innerMessage := inner.NewDataMessage(messageHandler.clientId, data)
	msg, err := inner.SerializeMessage(innerMessage)
	if err != nil {
		return nil, err
	}
	messageHandler.recordsSerialized++
	return msg, nil
}

// SerializeEOFMessage crea un mensaje EOF serializado con el contador de registros serializados y resetea el contador a 0.
func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	innerMessage := inner.NewEOFMessage(messageHandler.clientId, messageHandler.recordsSerialized)

	msg, err := inner.SerializeMessage(innerMessage)
	if err != nil {
		return nil, err
	}

	messageHandler.recordsSerialized = defaultSerializedAmount
	return msg, nil
}

// DeserializeResultMessage toma un mensaje recibido y lo convierte en un slice de registros de fruta,
// Si el mensaje no es un DataMessage o el clientId no coincide con el del handler, devuelve nil.
func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	innerMessage, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}

	// Como no puedo cambiar el gateway, toca hacer el casteo a mano, no puedo hacer Execute
	dataMsg, ok := innerMessage.(*inner.DataMessage)
	if !ok {
		return nil, nil
	}

	if dataMsg.ClientId() != messageHandler.clientId {
		return nil, nil
	}

	return dataMsg.FruitRecords(), nil
}

// Crea un ID tipo timestamp con cierto desfase para evitar colisiones entre clientes
func createClientID() string {
	now := time.Now().UnixNano()
	randomPart := rand.Intn(10000)
	return fmt.Sprintf("%d-%d", now, randomPart)
}
