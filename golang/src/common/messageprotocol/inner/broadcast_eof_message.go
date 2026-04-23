package inner

type BroadcastEOFHandler interface {
	HandleBroadcastEOFMessage(clientId string, totalFruitSend int) error
}

type BroadcastEOFMessage struct {
	clientId       string
	totalFruitSend int
}

func NewBroadcastEOFMessage(clientId string, totalFruitSend int) *BroadcastEOFMessage {
	return &BroadcastEOFMessage{clientId: clientId, totalFruitSend: totalFruitSend}
}

func (m *BroadcastEOFMessage) Execute(handler any) error {
	return handler.(BroadcastEOFHandler).HandleBroadcastEOFMessage(m.clientId, m.totalFruitSend)
}
