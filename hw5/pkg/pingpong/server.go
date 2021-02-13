package pingpong

import (
	"coms4113/hw5/pkg/base"
)

type Server struct {
	base.CoreNode
	ServerAttribute
}

func NewServer(unstable, crazy bool) *Server {
	return &Server{
		ServerAttribute: ServerAttribute{
			counter:  0,
			unstable: unstable,
			crazy:    crazy,
		},
	}
}

type ServerAttribute struct {
	counter int
	// config
	unstable bool
	crazy    bool
}

func (s *Server) Equals(other base.Node) bool {
	otherServer, ok := other.(*Server)
	return ok && s.counter == otherServer.counter
}

func (s *Server) MessageHandler(message base.Message) []base.Node {
	ping, ok := message.(*PingMessage)
	if !ok {
		return []base.Node{s}
	}
	newNodes := make([]base.Node, 0, 3)

	newNode := s.copy()
	newNode.counter++

	pong := &PongMessage{
		CoreMessage: base.MakeCoreMessage(ping.To(), ping.From()),
		Id:          ping.Id,
	}

	newNode.SetSingleResponse(pong)
	newNodes = append(newNodes, newNode)

	if s.unstable {
		newNode := s.copy()
		newNode.counter++
		newNodes = append(newNodes, newNode)
	}

	if s.crazy {
		newNode := s.copy()
		newNode.counter++
		pong := &PongMessage{
			CoreMessage: base.MakeCoreMessage(ping.To(), ping.From()),
			// Here is why it is crazy!
			Id: ping.Id + 1,
		}
		newNode.SetSingleResponse(pong)
		newNodes = append(newNodes, newNode)
	}

	return newNodes
}

func (s *Server) Attribute() interface{} {
	return s.ServerAttribute
}

func (s *Server) Copy() base.Node {
	return s.copy()
}

func (s *Server) copy() *Server {
	return &Server{
		ServerAttribute: s.ServerAttribute,
	}
}

func (s *Server) NextTimer() base.Timer {
	return nil
}

func (s *Server) TriggerTimer() []base.Node {
	return nil
}

func (s *Server) Hash() uint64 {
	return base.Hash("server", s.ServerAttribute)
}
