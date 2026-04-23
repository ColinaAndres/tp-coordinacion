package inner

const (
	MessageKindUnknown       = "unknown"
	MessageKindData          = "data"
	MessageKindEOF           = "eof"
	MessageKindCommunication = "communication"
)

type InnerMessage interface {
	Execute(handler any) error
}
