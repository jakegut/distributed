package base

// An interface for timer in a node
type Timer interface {

	// Check how long will this timer be triggered.
	RemainingTime() int

	// Let the timer elapse. All the timers in a node should be called Wait with the same input.
	Wait(t int)
}
