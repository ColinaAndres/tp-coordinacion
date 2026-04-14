package inner

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

type InnerMessage struct {
	ClientId     string
	FruitRecords []fruititem.FruitItem
	isEOF        bool
}

func NewInnerMessage(clientId string, fruitRecords []fruititem.FruitItem, isEOF bool) InnerMessage {
	return InnerMessage{
		ClientId:     clientId,
		FruitRecords: fruitRecords,
		isEOF:        isEOF,
	}
}
