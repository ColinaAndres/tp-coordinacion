package inner

type EOFHandler interface {
	HandleEOFMessage(clientId string, totalFruitSend int) error
}

type EOFMessage struct {
	clientId       string
	totalFruitSend int
}

func NewEOFMessage(clientId string, totalFruitSend int) *EOFMessage {
	return &EOFMessage{clientId: clientId, totalFruitSend: totalFruitSend}
}

func (m *EOFMessage) Execute(handler any) error {
	return handler.(EOFHandler).HandleEOFMessage(m.clientId, m.totalFruitSend)
}
