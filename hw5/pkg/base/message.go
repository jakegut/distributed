package base

type Address string

type Message interface {
	From() Address

	To() Address

	Hash() uint64

	Equals(Message) bool
}

// A helper type for Message type.
// You could include this as an embedding field.
// Example:
// ```go
// type MyMessage {
//     CoreMessage
//     ... // other fields
// }
// ```
//
// The good thing to do this is that it could help you implement From and To methods.
type CoreMessage struct {
	from Address
	to   Address
}

func MakeCoreMessage(from, to Address) CoreMessage {
	return CoreMessage{
		from: from,
		to:   to,
	}
}

func (m *CoreMessage) From() Address {
	return m.from
}

func (m *CoreMessage) To() Address {
	return m.to
}

func (m *CoreMessage) Equals(other *CoreMessage) bool {
	return m.from == other.from && m.to == other.to
}
