package inner

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

type MessageKind string

const (
	MessageKindUnknown       MessageKind = "unknown"
	MessageKindData          MessageKind = "data"
	MessageKindEOF           MessageKind = "eof"
	MessageKindCommunication MessageKind = "communication"
	MessageKindCleanup       MessageKind = "cleanup"
)

type InnerMessage struct {
	ClientId       string
	SenderId       int
	IsEOF          bool
	IsCleanUp      bool
	TotalFruitSend int
	FruitRecords   []fruititem.FruitItem
}

func (message InnerMessage) Kind() MessageKind {
	switch {
	case message.IsCleanUp:
		return MessageKindCleanup
	case message.IsEOF:
		return MessageKindEOF
	case len(message.FruitRecords) == 0:
		return MessageKindCommunication
	case len(message.FruitRecords) > 0:
		return MessageKindData
	default:
		return MessageKindUnknown
	}
}

func (message InnerMessage) IsDataMessage() bool {
	return message.Kind() == MessageKindData
}

func (message InnerMessage) IsEOFMessage() bool {
	return message.Kind() == MessageKindEOF
}

func (message InnerMessage) IsCommunicationMessage() bool {
	return message.Kind() == MessageKindCommunication
}

func (message InnerMessage) IsCleanupMessage() bool {
	return message.Kind() == MessageKindCleanup
}

// Este constructor es util para no cargar a mano el total enviado,
// Si no es de utilidad y se quisiera cargar a mano, se puede crear el struct directamente
func NewInnerMessage(clientId string, fruitRecords []fruititem.FruitItem, isEOF bool) InnerMessage {
	return InnerMessage{
		ClientId:       clientId,
		IsEOF:          isEOF,
		IsCleanUp:      false,
		TotalFruitSend: len(fruitRecords),
		FruitRecords:   fruitRecords,
	}
}

func NewDataMessage(clientId string, fruitRecords []fruititem.FruitItem) InnerMessage {
	return NewInnerMessage(clientId, fruitRecords, false)
}

func NewEOFMessage(clientId string, totalFruitSend int) InnerMessage {
	return InnerMessage{
		ClientId:       clientId,
		IsEOF:          true,
		IsCleanUp:      false,
		TotalFruitSend: totalFruitSend,
		FruitRecords:   []fruititem.FruitItem{},
	}
}

func NewCommunicationMessage(clientId string, senderId int, clientCount int) InnerMessage {
	return InnerMessage{
		ClientId:       clientId,
		SenderId:       senderId,
		IsEOF:          false,
		IsCleanUp:      false,
		TotalFruitSend: clientCount,
		FruitRecords:   []fruititem.FruitItem{},
	}
}
