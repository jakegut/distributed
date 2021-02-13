package paxos

import (
	"coms4113/hw5/pkg/base"
	"fmt"
	"testing"
)

func startServers(address []base.Address) []Server {
	var servers []Server
	for i, val := range address {
		response := make([]bool, len(address))
		s := Server{
			base.CoreNode{},
			ServerAttribute{
				peers: address,
				me:    i,
				proposer: Proposer{
					InitialValue: val,
					Responses:    response,
				},
			},
		}
		servers = append(servers, s)
	}
	return servers
}

func compareMessages(candidate []base.Message, expected []base.Message) bool {
	if len(candidate) != len(expected) {
		return false
	}
	flag := 0
	for _, message := range candidate {
		for i, expectedMessage := range expected {
			if expectedMessage.Equals(message) {
				flag |= 1 << i
				break
			}
		}
	}

	return flag == (1<<len(expected))-1
}

func TestUnit(t *testing.T) {
	address := []base.Address{"peer1", "peer2", "peer3"}

	fmt.Printf("Test: Proposer - Send Propose Request ...\n")

	servers := startServers(address)

	fakeState := &base.State{}
	servers[0].StartPropose()
	fakeState.Network = nil

	expectedMessages := []base.Message{
		&ProposeRequest{
			CoreMessage: base.MakeCoreMessage(address[0], address[0]),
			N:           1,
			SessionId:   1,
		},
		&ProposeRequest{
			CoreMessage: base.MakeCoreMessage(address[0], address[1]),
			N:           1,
			SessionId:   1,
		},
		&ProposeRequest{
			CoreMessage: base.MakeCoreMessage(address[0], address[2]),
			N:           1,
			SessionId:   1,
		},
	}
	expectedNode := Server{
		base.CoreNode{
			Response: expectedMessages,
		},
		ServerAttribute{
			peers: address,
			me:    0,
			n_p:   0,
			proposer: Proposer{
				N:             1,
				Phase:         Propose,
				V:             address[0],
				SuccessCount:  0,
				ResponseCount: 0,
				Responses:     []bool{false, false, false},
				SessionId:     1,
				InitialValue:  address[0],
			},
		},
	}

	if !compareMessages(servers[0].Response, expectedMessages) {
		t.Fatalf("response messages do not match\n")
	}

	if !servers[0].Equals(&expectedNode) {
		t.Fatalf("unexpected result\n")
	}

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Acceptor - Handle Propose Request ...\n")

	// acceptor accepted Propose request
	servers[0] = *servers[0].MessageHandler(expectedMessages[0])[0].(*Server)
	servers[1] = *servers[1].MessageHandler(expectedMessages[1])[0].(*Server)
	servers[2] = *servers[2].MessageHandler(expectedMessages[2])[0].(*Server)

	expectedNode = Server{
		base.CoreNode{
			Response: []base.Message{
				&ProposeResponse{
					CoreMessage: base.MakeCoreMessage(address[1], address[0]),
					Ok:          true,
					N_p:         1,
					SessionId:   1,
				},
			},
		},
		ServerAttribute{
			peers: address,
			me:    1,
			n_p:   1,
			proposer: Proposer{
				Responses:    []bool{false, false, false},
				InitialValue: address[1],
			},
		},
	}

	if !servers[1].Equals(&expectedNode) {
		t.Fatalf("unexpected result\n")
	}

	// acceptor rejected the duplicate Propose request
	newNode := *servers[2].MessageHandler(expectedMessages[2])[0].(*Server)

	expectedNode = Server{
		base.CoreNode{
			Response: []base.Message{
				&ProposeResponse{
					CoreMessage: base.MakeCoreMessage(address[2], address[0]),
					Ok:          false,
					N_p:         1,
					SessionId:   1,
				},
			},
		},
		ServerAttribute{
			peers: address,
			me:    2,
			n_p:   1,
			proposer: Proposer{
				Responses:    []bool{false, false, false},
				InitialValue: address[2],
			},
		},
	}

	if !newNode.Equals(&expectedNode) {
		t.Fatalf("unexpected result\n")
	}

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Proposer - Handle Propose Response ...\n")

	// handle propose response from itself
	newNode = *servers[0].MessageHandler(servers[0].Response[0])[0].(*Server)
	// handle propose response from servers[1]
	newNodes := newNode.MessageHandler(servers[1].Response[0])

	// Expect two child nodes: (1) A node enters Accept phase after receiving 2 Propose-OK, (2) Another node
	// continues to be Propose phase and wait for the 3rd response.
	expectedNodes := make([]Server, 0, 2)

	expectedMessages = []base.Message{
		&AcceptRequest{
			CoreMessage: base.MakeCoreMessage(address[0], address[0]),
			N:           1,
			V:           address[0],
			SessionId:   1,
		},
		&AcceptRequest{
			CoreMessage: base.MakeCoreMessage(address[0], address[1]),
			N:           1,
			V:           address[0],
			SessionId:   1,
		},
		&AcceptRequest{
			CoreMessage: base.MakeCoreMessage(address[0], address[2]),
			N:           1,
			V:           address[0],
			SessionId:   1,
		},
	}

	expectedNode = Server{
		base.CoreNode{
			Response: expectedMessages,
		},
		ServerAttribute{
			peers: address,
			me:    0,
			n_p:   1,
			n_a:   0,
			v_a:   nil,
			proposer: Proposer{
				N:             1,
				Phase:         Accept,
				V:             address[0],
				SuccessCount:  0,
				ResponseCount: 0,
				Responses:     []bool{false, false, false},
				SessionId:     1,
				InitialValue:  address[0],
			},
		},
	}
	expectedNodes = append(expectedNodes, expectedNode)

	expectedNode = *servers[0].copy()
	expectedNode.proposer.Responses = []bool{true, true, false}
	expectedNode.proposer.SuccessCount = 2
	expectedNode.proposer.ResponseCount = 2
	expectedNode.proposer.V = address[0]
	expectedNodes = append(expectedNodes, expectedNode)

	if len(newNodes) != 2 {
		t.Fatalf("unexpected result\n")
	}

	var ptr *base.Node

	if newNodes[0].Equals(&expectedNodes[0]) && newNodes[1].Equals(&expectedNodes[1]) {

		if !compareMessages(newNodes[0].(*Server).Response, expectedMessages) {
			t.Fatalf("The messages in new node do not match expected messages\n")
		}
		ptr = &newNodes[0]

	} else if newNodes[0].Equals(&expectedNodes[1]) && newNodes[1].Equals(&expectedNodes[0]) {

		if !compareMessages(newNodes[1].(*Server).Response, expectedMessages) {
			t.Fatalf("The messages in new node do not match expected messages\n")
		}
		ptr = &newNodes[1]

	} else {
		t.Fatalf("unexpected result\n")
	}

	fmt.Printf("  ... Passed\n")

	servers[0] = *(*ptr).(*Server)

	fmt.Printf("Test: Acceptor - Handle Accept Request ...\n")

	// acceptor accepted Accept request
	servers[0] = *servers[0].MessageHandler(expectedMessages[0])[0].(*Server)
	servers[1] = *servers[1].MessageHandler(expectedMessages[1])[0].(*Server)
	servers[2] = *servers[2].MessageHandler(expectedMessages[2])[0].(*Server)

	expectedNode = *servers[1].copy()
	expectedNode.n_p = 1
	expectedNode.n_a = 1
	expectedNode.v_a = address[0]
	expectedNode.Response = []base.Message{
		&AcceptResponse{
			CoreMessage: base.MakeCoreMessage(address[1], address[0]),
			Ok:          true,
			N_p:         1,
			SessionId:   1,
		},
	}

	if !servers[1].Equals(&expectedNode) {
		t.Fatalf("unexpected result\n")
	}

	// acceptor rejected Accept request
	anotherMessage := &AcceptRequest{
		CoreMessage: base.MakeCoreMessage(address[0], address[2]),
		N:           0,
		V:           address[0],
		SessionId:   1,
	}

	newNode = *servers[2].MessageHandler(anotherMessage)[0].(*Server)

	expectedNode = *servers[2].copy()
	expectedNode.Response = []base.Message{
		&AcceptResponse{
			CoreMessage: base.MakeCoreMessage(address[2], address[0]),
			Ok:          false,
			N_p:         1,
			SessionId:   1,
		},
	}

	if !newNode.Equals(&expectedNode) {
		t.Fatalf("unexpected result\n")
	}

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Proposer - Handle Accept Response ...\n")

	// servers[0] handle accept response from itself
	newNode = *servers[0].MessageHandler(servers[0].Response[0])[0].(*Server)
	// servers[0] handle accept response from servers[2]
	newNodes = newNode.MessageHandler(servers[2].Response[0])

	expectedMessages = []base.Message{
		&DecideRequest{
			CoreMessage: base.MakeCoreMessage(address[0], address[0]),
			V:           address[0],
			SessionId:   1,
		},
		&DecideRequest{
			CoreMessage: base.MakeCoreMessage(address[0], address[1]),
			V:           address[0],
			SessionId:   1,
		},
		&DecideRequest{
			CoreMessage: base.MakeCoreMessage(address[0], address[2]),
			V:           address[0],
			SessionId:   1,
		},
	}

	expectedNodes = []Server{
		{
			base.CoreNode{
				Response: expectedMessages,
			},
			ServerAttribute{
				peers:       address,
				me:          0,
				n_p:         1,
				n_a:         1,
				v_a:         address[0],
				agreedValue: nil,
				proposer: Proposer{
					N:             1,
					Phase:         Decide,
					V:             address[0],
					SuccessCount:  0,
					ResponseCount: 0,
					Responses:     []bool{false, false, false},
					SessionId:     1,
					InitialValue:  address[0],
				},
			},
		},
	}
	expectedNode = *servers[0].copy()
	expectedNode.proposer.Responses = []bool{true, false, true}
	expectedNode.proposer.SuccessCount = 2
	expectedNode.proposer.ResponseCount = 2
	expectedNodes = append(expectedNodes, expectedNode)

	if len(newNodes) != 2 {
		t.Fatalf("unexpected result\n")
	}

	if newNodes[0].Equals(&expectedNodes[0]) && newNodes[1].Equals(&expectedNodes[1]) {

		if !compareMessages(newNodes[0].(*Server).Response, expectedMessages) {
			t.Fatalf("The messages in new node do not match expected messages\n")
		}
		ptr = &newNodes[0]

	} else if newNodes[0].Equals(&expectedNodes[1]) && newNodes[1].Equals(&expectedNodes[0]) {

		if !compareMessages(newNodes[1].(*Server).Response, expectedMessages) {
			t.Fatalf("The messages in new node do not match expected messages\n")
		}
		ptr = &newNodes[1]

	} else {
		t.Fatalf("unexpected result\n")
	}

	fmt.Printf("  ... Passed\n")

	servers[0] = *(*ptr).(*Server)

	fmt.Printf("Test: Acceptor - Handle Decide Request ...\n")

	servers[2] = *servers[2].MessageHandler(expectedMessages[2])[0].(*Server)

	if servers[2].agreedValue != address[0] {
		t.Fatalf("unexpected result\n")
	}
	fmt.Printf("  ... Passed\n")
}
