package inner

type CommunicationHandler interface {
	HandleCommunicationMessage(clientId string, count int) error
	Id() int
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
	if handler.(CommunicationHandler).Id() == m.senderId {
		return nil
	}
	return handler.(CommunicationHandler).HandleCommunicationMessage(m.clientId, m.count)
}
