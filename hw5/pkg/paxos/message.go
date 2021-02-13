package paxos

import "coms4113/hw5/pkg/base"

type ProposeRequest struct {
	base.CoreMessage
	N int

	SessionId int
}

func (p *ProposeRequest) Hash() uint64 {
	return base.Hash("propose-request", *p)
}

func (p *ProposeRequest) Equals(o base.Message) bool {
	other, ok := o.(*ProposeRequest)
	if !ok {
		return false
	}

	return p.CoreMessage.Equals(&other.CoreMessage) && p.N == other.N && p.SessionId == other.SessionId
}

type ProposeResponse struct {
	base.CoreMessage
	Ok  bool
	N_p int
	N_a int
	V_a interface{}

	SessionId int
}

func (p *ProposeResponse) Hash() uint64 {
	return base.Hash("propose-response", *p)
}

func (p *ProposeResponse) Equals(o base.Message) bool {
	other, ok := o.(*ProposeResponse)
	if !ok {
		return false
	}

	return p.CoreMessage.Equals(&other.CoreMessage) && p.Ok == other.Ok &&
		p.N_a == other.N_a && p.V_a == other.V_a && p.SessionId == other.SessionId
}

type AcceptRequest struct {
	base.CoreMessage
	N int
	V interface{}

	SessionId int
}

func (a *AcceptRequest) Hash() uint64 {
	return base.Hash("accept-request", *a)
}

func (a *AcceptRequest) Equals(o base.Message) bool {
	other, ok := o.(*AcceptRequest)
	if !ok {
		return false
	}

	return a.CoreMessage.Equals(&other.CoreMessage) && a.N == other.N && a.V == other.V &&
		a.SessionId == other.SessionId
}

type AcceptResponse struct {
	base.CoreMessage
	Ok  bool
	N_p int

	SessionId int
}

func (a *AcceptResponse) Hash() uint64 {
	return base.Hash("accept-response", *a)
}

func (a *AcceptResponse) Equals(o base.Message) bool {
	other, ok := o.(*AcceptResponse)
	if !ok {
		return false
	}

	return a.CoreMessage.Equals(&other.CoreMessage) && a.Ok == other.Ok && a.N_p == other.N_p &&
		a.SessionId == other.SessionId
}

type DecideRequest struct {
	base.CoreMessage
	V interface{}

	SessionId int
}

func (d *DecideRequest) Hash() uint64 {
	return base.Hash("decide-request", *d)
}

func (d *DecideRequest) Equals(o base.Message) bool {
	other, ok := o.(*DecideRequest)
	if !ok {
		return false
	}

	return d.CoreMessage.Equals(&other.CoreMessage) && d.V == other.V &&
		d.SessionId == other.SessionId
}

// Using this type is optional. There is no need to respond to a Decide Message.
type DecideResponse struct {
	base.CoreMessage
	Ok bool

	SessionId int
}

func (d *DecideResponse) Hash() uint64 {
	return base.Hash("decide-response", *d)
}

func (d *DecideResponse) Equals(o base.Message) bool {
	other, ok := o.(*DecideResponse)
	if !ok {
		return false
	}

	return d.CoreMessage.Equals(&other.CoreMessage) && d.Ok == other.Ok &&
		d.SessionId == other.SessionId
}
