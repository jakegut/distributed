package pingpong

import (
	"coms4113/hw5/pkg/base"
	"testing"
)

func TestHashAndEqual(t *testing.T) {
	s1 := base.NewState(0, true, true)

	server := NewServer(false, true)
	s1.AddNode("server", server, nil)

	client := NewClient("client", "server", 5, true)
	s1.AddNode("client", client, nil)

	client.SendCommand(s1, PingCommand{
		To: "server",
		Id: 1,
	})

	client.SendCommand(s1, PingCommand{
		To: "server",
		Id: 2,
	})

	client.SendCommand(s1, PingCommand{
		To: "server",
		Id: 3,
	})

	s2 := base.NewState(100, true, true)

	client = NewClient("client", "server", 5, true)
	s2.AddNode("client", client, nil)

	server = NewServer(false, true)
	s2.AddNode("server", server, nil)

	client.SendCommand(s2, PingCommand{
		To: "server",
		Id: 3,
	})

	client.SendCommand(s2, PingCommand{
		To: "server",
		Id: 1,
	})

	client.SendCommand(s2, PingCommand{
		To: "server",
		Id: 2,
	})

	if !s1.Equals(s2) {
		t.Fail()
	}
}

func TestStateInherit(t *testing.T) {
	s := base.NewState(0, true, true)

	server := NewServer(false, true)
	s.AddNode("server", server, nil)

	client := NewClient("client", "server", 5, false)
	s.AddNode("client", client, nil)

	client.SendCommand(s, PingCommand{
		To: "server",
		Id: 1,
	})

	s1 := s.Inherit(base.TriggerEvent("client", nil))
	s1.Nodes()["client"].(*Client).SendCommand(s1, PingCommand{
		To: "server",
		Id: 1,
	})

	s2 := s1.Inherit(base.HandleEvent(s.Network[0]))
	s2.HandleMessage(0, true)

	s3 := s.Inherit(base.HandleEvent(s.Network[0]))
	s3.HandleMessage(0, true)
	s4 := s3.Inherit(base.TriggerEvent("client", nil))
	s4.Nodes()["client"].(*Client).SendCommand(s4, PingCommand{
		To: "server",
		Id: 1,
	})

	if s2.Hash() != s4.Hash() || !s2.Equals(s4) {
		t.Fail()
	}

}

func TestNextStates(t *testing.T) {
	s := base.NewState(0, true, true)

	server := NewServer(false, true)
	s.AddNode("server", server, nil)

	client := NewClient("client", "server", 5, true)
	s.AddNode("client", client, nil)

	client.SendCommand(s, PingCommand{
		To: "server",
		Id: 1,
	})

	nextStates := s.NextStates()

	// 6 states: (1) client timer, (2) message drop off, (3) message arrival normally + server handles in a normal mode
	// (4) message arrival normally + server handles in a crazy mode,
	// (5) message arrival with duplicate + server handles in a normal mode
	// (6) message arrival with duplicate + server handles in a crazy mode
	if len(nextStates) != 6 {
		t.Fail()
	}

	findIf := func(states []*base.State, predicate func(*base.State) bool) int {
		for i, state := range states {
			if predicate(state) {
				return i
			}
		}

		return -1
	}

	// verify drop off
	dropOff := func(s *base.State) bool {
		if len(s.Network) != 0 {
			return false
		}

		server := s.Nodes()["server"].(*Server)
		return server.ServerAttribute.counter == 0
	}

	if findIf(nextStates, dropOff) == -1 {
		t.Error("fail at drop off")
		t.Fail()
	}

	// verify normal handle
	normal := func(s *base.State) bool {
		if len(s.Network) != 1 {
			return false
		}

		message, ok := s.Network[0].(*PongMessage)
		if !ok || message.Id != 1 {
			return false
		}

		server := s.Nodes()["server"].(*Server)
		return server.ServerAttribute.counter == 1
	}
	if findIf(nextStates, normal) == -1 {
		t.Error("fail at normal handle + normal mode")
		t.Fail()
	}

	// verify normal handle + crazy
	crazy := func(s *base.State) bool {
		if len(s.Network) != 1 {
			return false
		}

		message, ok := s.Network[0].(*PongMessage)
		if !ok || message.Id != 2 {
			return false
		}

		server := s.Nodes()["server"].(*Server)
		return server.ServerAttribute.counter == 1
	}
	if findIf(nextStates, crazy) == -1 {
		t.Error("fail at normal handle + crazy mode")
		t.Fail()
	}

	// verify duplicate handle + crazy mode
	duplicateCrazy := func(s *base.State) bool {
		if len(s.Network) != 2 {
			return false
		}

		ping, ok := s.Network[0].(*PingMessage)
		if !ok || ping.Id != 1 {
			return false
		}

		pong, ok := s.Network[1].(*PongMessage)
		if !ok || pong.Id != 2 {
			return false
		}

		server := s.Nodes()["server"].(*Server)
		return server.ServerAttribute.counter == 1
	}
	if findIf(nextStates, duplicateCrazy) == -1 {
		t.Error("fail at duplicate handle + crazy mode")
		t.Fail()
	}

	// verify timerTick
	timerTick := func(s *base.State) bool {
		if len(s.Network) != 2 {
			return false
		}

		ping, ok := s.Network[0].(*PingMessage)
		if !ok || ping.Id != 1 {
			return false
		}

		ping, ok = s.Network[1].(*PingMessage)
		if !ok || ping.Id != 1 {
			return false
		}

		server := s.Nodes()["server"].(*Server)
		return server.ServerAttribute.counter == 0
	}

	if findIf(nextStates, timerTick) == -1 {
		t.Error("fail at timer tick")
		t.Fail()
	}
}

func TestPartition(t *testing.T) {
	s := base.NewState(0, true, true)

	server := NewServer(true, true)
	s.AddNode("server", server, []base.Address{"client"})

	client := NewClient("client", "server", 5, false)
	s.AddNode("client", client, nil)

	client.SendCommand(s, PingCommand{
		To: "server",
		Id: 1,
	})

	newStates := s.NextStates()

	if len(newStates) != 1 {
		t.Fail()
		return
	}

	s1 := newStates[0]

	if s1.Event.Action != base.Partition {
		t.Fail()
		return
	}

	if len(s1.Network) != 0 {
		t.Fail()
		return
	}

	server = s1.Nodes()["server"].(*Server)
	if server.counter != 0 {
		t.Fail()
		return
	}
}
