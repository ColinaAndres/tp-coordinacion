package inner

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

type InnerMessage struct {
	ClientId       string
	IsEOF          bool
	IsCleanUp      bool
	TotalFruitSend int
	FruitRecords   []fruititem.FruitItem
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

func NewComunicationMessage(clientId string, clientCount int) InnerMessage {
	return InnerMessage{
		ClientId:       clientId,
		IsEOF:          false,
		IsCleanUp:      false,
		TotalFruitSend: clientCount,
		FruitRecords:   []fruititem.FruitItem{},
	}
}
