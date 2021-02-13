package pingpong

import (
	"coms4113/hw5/pkg/base"
)

type PingMessage struct {
	base.CoreMessage

	// value
	Id int
}

func (p *PingMessage) Hash() uint64 {
	return base.Hash("ping-message", *p)
}

func (p *PingMessage) Equals(other base.Message) bool {
	otherP, ok := other.(*PingMessage)

	return ok && p.CoreMessage.Equals(&otherP.CoreMessage) && p.Id == otherP.Id
}

type PongMessage struct {
	base.CoreMessage

	// value
	Id int
}

func (p *PongMessage) Hash() uint64 {
	return base.Hash("pong-message", *p)
}

func (p *PongMessage) Equals(other base.Message) bool {
	otherP, ok := other.(*PongMessage)
	if !ok {
		return false
	}

	return p.CoreMessage.Equals(&otherP.CoreMessage) && p.Id == otherP.Id
}

type PingCommand struct {
	To base.Address
	Id int
}
