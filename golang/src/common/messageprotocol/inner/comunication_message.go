package inner

type CommunicationHandler interface {
	HandleCommunicationMessage(clientId string, senderId int, count int) error
}

type CommunicationMessage struct {
	clientId string
	senderId int
	count    int
}

func NewCommunicationMessage(clientId string, senderId int, count int) *CommunicationMessage {
	return &CommunicationMessage{clientId: clientId, senderId: senderId, count: count}
}

func (m *CommunicationMessage) Execute(handler any) error {
	return handler.(CommunicationHandler).HandleCommunicationMessage(m.clientId, m.senderId, m.count)
}
