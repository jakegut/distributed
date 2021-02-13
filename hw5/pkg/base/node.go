package base

// An interface to represent a node in a distributed system.
// A node is treated as an immutable object in a state. Every time a node is to be modified,
// one should always copy a new one and modify the new one.
type Node interface {
	// Handle an incoming message and return a list of new nodes (given a node is immutable).
	// The new nodes reflect the modification after handling a message. After handling a message,
	// a node may return message(s) back to the network. This can be achieved by `HandlerResponse`
	// member function, please check about it as well.

	// Why a node may return multiple new nodes? Because a node could make an decision during
	// handling a message. Take Paxos proposer as an example: if a node sends out 3 proposal
	// and just received the second OK reply. It has two choice: (1) continue to wait for the
	// next reply, or (2) directly enter the Accept phase. The difference between (1) and (2)
	// could result from waiting time. Since the absolute time does not matter in a state
	// machine, we need to consider both cases are potential next step.
	MessageHandler(message Message) []Node

	// Get the next timer that will be triggered in this node
	NextTimer() Timer

	// Trigger the next timer and return a list of new nodes. Check `MessageHandler` for why
	// we need to return a list of nodes.
	TriggerTimer() []Node

	// When a node handles an event (message + timer), it will return a list of new nodes.
	// Each new node may have messages sent back to the network. Please make sure these messages
	// can be fetched by calling `HandlerResponse` method. Please check `state.go` for more details.
	HandlerResponse() []Message

	// Return the attribute of a node.
	Attribute() interface{}

	Copy() Node

	Hash() uint64

	Equals(other Node) bool
}

// A helper type to help users to implement some functions of Node interface.
// Just embed it in your Node implementation.
// You don't have to deep copy this field when deep copying of a node, because the field
// in CodeNode is considered transient.
type CoreNode struct {
	Response []Message
}

func (node *CoreNode) HandlerResponse() []Message {
	res := node.Response
	node.Response = nil
	return res
}

func (node *CoreNode) SetResponse(response []Message) {
	node.Response = make([]Message, 0, len(response))
	node.Response = append(node.Response, response...)
}

func (node *CoreNode) SetSingleResponse(response Message) {
	node.Response = []Message{response}
}
