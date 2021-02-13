package base

type Client interface {
	// User calling this method will require a node to do some action.
	// In this homework, it will be used by test cases to start a test.
	SendCommand(s *State, command Command)
}
