package pingpong

import (
	"coms4113/hw5/pkg/base"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestBasic(t *testing.T) {
	s := base.NewState(0, false, false)

	server := NewServer(false, false)
	s.AddNode("server", server, nil)

	client := NewClient("client", "server", 5, false)
	s.AddNode("client", client, nil)

	client.SendCommand(s, PingCommand{
		To: "server",
		Id: 1,
	})

	if len(s.Network) != 1 {
		t.Fail()
	}

	newStates := s.NextStates()
	if len(newStates) != 1 {
		t.Fail()
	}

	cur := newStates
	for len(cur) > 0 {
		newStates := make([]*base.State, 0)
		for _, s := range cur {
			for _, newState := range s.NextStates() {
				newStates = append(newStates, newState)
			}
		}

		if len(newStates) == 0 {
			if !IsFinal(cur[0]) {
				t.Fail()
			}
		}

		cur = newStates
	}

}

func TestBfsFind(t *testing.T) {
	s := base.NewState(0, false, false)

	server := NewServer(false, false)
	s.AddNode("server", server, nil)

	client := NewClient("client", "server", 5, true)
	s.AddNode("client", client, nil)

	validate := func(new *base.State) bool {
		if new.Prev == nil {
			return true
		}

		newClient := new.GetNode("client").(*Client)
		oldClient := new.Prev.GetNode("client").(*Client)

		if newClient.ack == oldClient.ack+1 {
			return new.Event.Action == base.Handle || new.Event.Action == base.HandleDuplicate
		}

		if newClient.ack > oldClient.ack+1 {
			return false
		}

		return true
	}

	result := base.BfsFind(s, validate, IsFinal, 15)
	if result.Targets == nil {
		t.Fail()
	}

	fmt.Printf("Number of Explored States: %d\n", result.N)
	fmt.Printf("Depth of Targets State: %d\n", result.Targets[0].Depth)

	_, path := base.FindPath(result.Targets[0])
	base.PrintPath(path)
}

func TestBfsFindAll1(t *testing.T) {
	s := base.NewState(0, false, false)

	server := NewServer(false, false)
	s.AddNode("server", server, nil)

	client := NewClient("client", "server", 5, true)
	s.AddNode("client", client, nil)

	validate := func(new *base.State) bool {
		if new.Prev == nil {
			return true
		}

		newClient := new.GetNode("client").(*Client)
		oldClient := new.Prev.GetNode("client").(*Client)

		if newClient.ack == oldClient.ack+1 {
			return new.Event.Action == base.Handle || new.Event.Action == base.HandleDuplicate
		}

		if newClient.ack > oldClient.ack+1 {
			return false
		}

		return true
	}

	result := base.BfsFindAll(s, validate, IsFinal, 15)

	if !result.Success {
		t.Error("invalid implementation")
		t.Fail()
	}

	if result.Targets == nil {
		t.Fail()
	}

	fmt.Printf("Number of Explored States: %d\n", result.N)
	fmt.Printf("Depth of Success State: %d\n", len(result.Targets))

	fmt.Println("The last success state")
	_, path := base.FindPath(result.Targets[len(result.Targets)-1])
	base.PrintPath(path)
}

func TestBfsFindAll2(t *testing.T) {
	s := base.NewState(0, true, true)

	server := NewServer(false, false)
	s.AddNode("server", server, nil)

	client := NewClient("client", "server", 5, true)
	s.AddNode("client", client, nil)

	validate := func(new *base.State) bool {
		if new.Prev == nil {
			return true
		}

		newClient := new.GetNode("client").(*Client)
		oldClient := new.Prev.GetNode("client").(*Client)

		if newClient.ack == oldClient.ack+1 {
			return new.Event.Action == base.Handle || new.Event.Action == base.HandleDuplicate
		}

		if newClient.ack > oldClient.ack+1 {
			return false
		}

		return true
	}

	result := base.BfsFindAll(s, validate, IsFinal, 11)

	if !result.Success {
		t.Error("invalid implementation")
		t.Fail()
	}

	if result.Targets == nil {
		t.Fail()
	}

	fmt.Printf("Number of Explored States: %d\n", result.N)
	fmt.Printf("Depth of Success State: %d\n", len(result.Targets))

	fmt.Println("The last success state")
	_, path := base.FindPath(result.Targets[len(result.Targets)-1])
	base.PrintPath(path)
}

func TestBfsFindAll3(t *testing.T) {
	s := base.NewState(0, false, false)

	server := NewServer(true, true)
	s.AddNode("server", server, nil)

	client := NewClient("client", "server", 5, true)
	s.AddNode("client", client, nil)

	validate := func(new *base.State) bool {
		if new.Prev == nil {
			return true
		}

		newClient := new.GetNode("client").(*Client)
		oldClient := new.Prev.GetNode("client").(*Client)

		if newClient.ack == oldClient.ack+1 {
			return new.Event.Action == base.Handle || new.Event.Action == base.HandleDuplicate
		}

		if newClient.ack > oldClient.ack+1 {
			return false
		}

		return true
	}

	result := base.BfsFindAll(s, validate, IsFinal, 13)

	if !result.Success {
		t.Error("invalid implementation")
		t.Fail()
	}

	if result.Targets == nil {
		t.Fail()
	}

	fmt.Printf("Number of Explored States: %d\n", result.N)
	fmt.Printf("Depth of Success State: %d\n", len(result.Targets))

	fmt.Println("The last success state")
	_, path := base.FindPath(result.Targets[len(result.Targets)-1])
	base.PrintPath(path)
}

func TestRandomWalkFindAll(t *testing.T) {
	s := base.NewState(0, true, false)

	server := NewServer(false, false)
	s.AddNode("server", server, nil)

	client := NewClient("client", "server", 5, true)
	s.AddNode("client", client, nil)

	validate := func(new *base.State) bool {
		if new.Prev == nil {
			return true
		}

		newClient := new.GetNode("client").(*Client)
		oldClient := new.Prev.GetNode("client").(*Client)

		if newClient.ack == oldClient.ack+1 {
			return new.Event.Action == base.Handle || new.Event.Action == base.HandleDuplicate
		}

		if newClient.ack > oldClient.ack+1 {
			return false
		}

		return true
	}

	result := base.BatchRandomWalkValidate(s, validate, IsFinal, 20, 1000)
	if !result.Success {
		t.Fail()
	}
}

func TestRandomWalkFind(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	s := base.NewState(0, true, false)

	server := NewServer(false, false)
	s.AddNode("server", server, nil)

	client := NewClient("client", "server", 5, true)
	s.AddNode("client", client, nil)

	validate := func(new *base.State) bool {
		if new.Prev == nil {
			return true
		}

		newClient := new.GetNode("client").(*Client)
		oldClient := new.Prev.GetNode("client").(*Client)

		if newClient.ack == oldClient.ack+1 {
			return new.Event.Action == base.Handle || new.Event.Action == base.HandleDuplicate
		}

		if newClient.ack > oldClient.ack+1 {
			return false
		}

		return true
	}

	result := base.BatchRandomWalkFind(s, validate, IsFinal, 26, 5000)

	if result.Success {
		fmt.Printf("Number of Explored States: %d\n", result.N)
		fmt.Printf("Depth of Targets State: %d\n", result.Targets[0].Depth)

		_, path := base.FindPath(result.Targets[0])
		base.PrintPath(path)
	}
}
