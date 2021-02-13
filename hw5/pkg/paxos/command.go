package paxos

import "coms4113/hw5/pkg/base"

type ProposeCommand struct {
	Value interface{}
}

func (server *Server) SendCommand(s *base.State, command_ base.Command) {
	command, ok := command_.(ProposeCommand)
	if !ok {
		return
	}

	server.proposer.InitialValue = command.Value
	server.StartPropose()
	s.Receive(server.HandlerResponse())
}
