package pingpong

import (
	"coms4113/hw5/pkg/base"
)

type Client struct {
	base.CoreNode
	address base.Address
	server  base.Address
	ack     int
	goal    int

	// timer fields
	isRetry bool
	// Singleton List
	pingTimer *PingTimer
}

func NewClient(address base.Address, server base.Address, goal int, isRetry bool) *Client {
	c := &Client{
		address: address,
		server:  server,
		ack:     0,
		goal:    goal,
		isRetry: isRetry,
	}

	if isRetry {
		t := &PingTimer{
			remain: RetryTime,
		}
		c.pingTimer = t
	}

	return c
}

func (c *Client) Equals(other base.Node) bool {
	otherClient, ok := other.(*Client)

	if !ok || c.ack != otherClient.ack || c.goal != otherClient.goal ||
		c.address != otherClient.address || c.server != otherClient.server {
		return false
	}

	if (c.pingTimer == nil) != (otherClient.pingTimer == nil) {
		return false
	}

	if c.pingTimer != nil && c.pingTimer.remain != otherClient.pingTimer.remain {
		return false
	}

	return true
}

func (c *Client) MessageHandler(message base.Message) []base.Node {
	pong, ok := message.(*PongMessage)
	if !ok {
		return []base.Node{c}
	}

	newClient := c.copy()

	// Only acknowledge next number
	if pong.Id == newClient.ack+1 {
		newClient.ack = pong.Id
	}

	if pong.Id == newClient.goal {
		return []base.Node{newClient}
	}

	ping := &PingMessage{
		CoreMessage: base.MakeCoreMessage(c.address, pong.From()),
		Id:          pong.Id + 1,
	}

	newClient.SetSingleResponse(ping)

	return []base.Node{newClient}
}

func (c *Client) Attribute() interface{} {
	return c.ack
}

func (c *Client) Copy() base.Node {
	return c.copy()
}

func (c *Client) copy() *Client {
	var pingTimer *PingTimer
	if c.pingTimer != nil {
		pingTimer = &PingTimer{
			remain: c.pingTimer.remain,
		}
	}

	newClient := &Client{
		address:   c.address,
		server:    c.server,
		ack:       c.ack,
		goal:      c.goal,
		isRetry:   c.isRetry,
		pingTimer: pingTimer,
	}

	return newClient
}

func (c *Client) SendCommand(s *base.State, command base.Command) {
	switch command.(type) {
	case PingCommand:
		pingCmd := command.(PingCommand)
		m := &PingMessage{
			CoreMessage: base.MakeCoreMessage(c.address, pingCmd.To),
			Id:          pingCmd.Id,
		}
		s.Receive([]base.Message{m})
	}
}

func (c *Client) NextTimer() base.Timer {
	if !c.isRetry {
		return nil
	}

	return c.pingTimer
}

func (c *Client) TriggerTimer() []base.Node {
	if !c.isRetry || c.pingTimer == nil {
		return nil
	}

	newClient := c.copy()

	if newClient.ack == 5 {
		newClient.pingTimer = nil
		return []base.Node{newClient}
	}

	newClient.pingTimer.Reset()

	m := &PingMessage{
		CoreMessage: base.MakeCoreMessage(c.address, c.server),
		Id:          c.ack + 1,
	}

	newClient.SetSingleResponse(m)

	return []base.Node{newClient}
}

type clientHashBody struct {
	address         base.Address
	server          base.Address
	goal            int
	ack             int
	isRetry         bool
	pingTimerRemain int
}

func (c *Client) Hash() uint64 {
	var pingTimerRemain int
	if c.pingTimer != nil {
		pingTimerRemain = c.pingTimer.remain
	}

	body := clientHashBody{
		address:         c.address,
		server:          c.server,
		goal:            c.goal,
		ack:             c.ack,
		isRetry:         c.isRetry,
		pingTimerRemain: pingTimerRemain,
	}

	return base.Hash("client", body)
}
