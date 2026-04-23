package inner

import "github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"

type DataHandler interface {
	HandleDataMessage(clientId string, fruitItems []fruititem.FruitItem) error
}

type DataMessage struct {
	clientId   string
	fruitItems []fruititem.FruitItem
}

func NewDataMessage(clientId string, fruitItems []fruititem.FruitItem) *DataMessage {
	return &DataMessage{clientId: clientId, fruitItems: fruitItems}
}

func (m *DataMessage) Execute(handler any) error {
	return handler.(DataHandler).HandleDataMessage(m.clientId, m.fruitItems)
}

func (m *DataMessage) ClientId() string {
	return m.clientId
}

func (m *DataMessage) FruitRecords() []fruititem.FruitItem {
	return m.fruitItems
}
