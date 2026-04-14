package inner

import (
	"encoding/json"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func serializeJson(data any) ([]byte, error) {
	return json.Marshal(data)
}

func deserializeJson(data []byte, value any) error {
	return json.Unmarshal(data, value)
}

func SerializeMessage(record InnerMessage) (*middleware.Message, error) {
	body, err := serializeJson(record)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}
	return &message, nil
}

func DeserializeMessage(message *middleware.Message) (*InnerMessage, error) {
	var innerMessage InnerMessage
	err := deserializeJson([]byte(message.Body), &innerMessage)
	if err != nil {
		return nil, err
	}
	return &innerMessage, nil
}
