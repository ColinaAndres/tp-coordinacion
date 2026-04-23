package inner

import (
	"encoding/json"
	"fmt"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type envelope struct {
	Kind    string `json:"kind"`
	Payload []byte `json:"payload"`
}

type eofPayload struct {
	ClientId       string `json:"client_id"`
	TotalFruitSend int    `json:"total_fruit_send"`
}

type dataPayload struct {
	ClientId   string                `json:"client_id"`
	FruitItems []fruititem.FruitItem `json:"fruit_items"`
}

type communicationPayload struct {
	ClientId string `json:"client_id"`
	SenderId int    `json:"sender_id"`
	Count    int    `json:"count"`
}

func SerializeMessage(msg InnerMessage) (*middleware.Message, error) {
	var kind string
	var payload any

	switch m := msg.(type) {
	case *EOFMessage:
		kind = MessageKindEOF
		payload = eofPayload{ClientId: m.clientId, TotalFruitSend: m.totalFruitSend}
	case *DataMessage:
		kind = MessageKindData
		payload = dataPayload{ClientId: m.clientId, FruitItems: m.fruitItems}
	case *CommunicationMessage:
		kind = MessageKindCommunication
		payload = communicationPayload{ClientId: m.clientId, SenderId: m.senderId, Count: m.count}
	default:
		return nil, fmt.Errorf("unknown message type: %T", msg)
	}

	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	body, err := json.Marshal(envelope{Kind: kind, Payload: rawPayload})
	if err != nil {
		return nil, err
	}

	return &middleware.Message{Body: string(body)}, nil
}

func DeserializeMessage(msg *middleware.Message) (InnerMessage, error) {
	var env envelope
	if err := json.Unmarshal([]byte(msg.Body), &env); err != nil {
		return nil, err
	}

	switch env.Kind {
	case MessageKindEOF:
		var payload eofPayload
		if err := json.Unmarshal(env.Payload, &payload); err != nil {
			return nil, err
		}
		return NewEOFMessage(payload.ClientId, payload.TotalFruitSend), nil

	case MessageKindData:
		var payload dataPayload
		if err := json.Unmarshal(env.Payload, &payload); err != nil {
			return nil, err
		}
		return NewDataMessage(payload.ClientId, payload.FruitItems), nil

	case MessageKindCommunication:
		var payload communicationPayload
		if err := json.Unmarshal(env.Payload, &payload); err != nil {
			return nil, err
		}
		return NewCommunicationMessage(payload.ClientId, payload.SenderId, payload.Count), nil
	default:
		return nil, fmt.Errorf("unknown message kind: %s", env.Kind)
	}
}
