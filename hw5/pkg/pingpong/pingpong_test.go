package pingpong

import (
	"coms4113/hw5/pkg/base"
	"testing"
)

func TestServerEqual(t *testing.T) {
	server1 := NewServer(true, false)
	server1.counter = 1
	server2 := NewServer(true, false)
	server2.counter = 2

	if server1.Equals(server2) {
		t.Fail()
	}

	server2.counter = 1
	if server1.Hash() != server2.Hash() {
		t.Fail()
	}

	if !server1.Equals(server2) {
		t.Fail()
	}
}

func TestClientEqual(t *testing.T) {
	client1 := NewClient("client", "server", 10, true)
	client1.ack = 1
	client2 := NewClient("client", "server", 10, true)
	client2.ack = 2

	if client1.Hash() == client2.Hash() {
		t.Fail()
	}
	if client1.Equals(client2) {
		t.Fail()
	}

	client2.ack = 1
	client2.goal = 11
	if client1.Hash() == client2.Hash() {
		t.Fail()
	}
	if client1.Equals(client2) {
		t.Fail()
	}

	client2.goal = 10
	client2.address = "xxx"
	if client1.Hash() == client2.Hash() {
		t.Fail()
	}
	if client1.Equals(client2) {
		t.Fail()
	}

	client2.address = "client"
	client2.pingTimer.remain = 5
	if client1.Hash() == client2.Hash() {
		t.Fail()
	}
	if client1.Equals(client2) {
		t.Fail()
	}

	client1.pingTimer.remain = 5
	if client1.Hash() != client2.Hash() {
		t.Fail()
	}

	if !client1.Equals(client2) {
		t.Fail()
	}
}

func TestMessageEqual(t *testing.T) {
	m1 := &PingMessage{
		CoreMessage: base.MakeCoreMessage("client", "server"),
		Id:          5,
	}

	m2 := &PingMessage{
		CoreMessage: base.MakeCoreMessage("client", "server"),
		Id:          5,
	}

	if m1.Hash() != m2.Hash() {
		t.Fail()
	}

	if !m1.Equals(m2) {
		t.Fail()
	}

	m1.Id = 4
	if m1.Hash() == m2.Hash() {
		t.Fail()
	}
	if m1.Equals(m2) {
		t.Fail()
	}

	m3 := &PingMessage{
		CoreMessage: base.MakeCoreMessage("client?", "server"),
		Id:          5,
	}

	if m1.Hash() == m3.Hash() {
		t.Fail()
	}
	if m1.Equals(m3) {
		t.Fail()
	}
}
