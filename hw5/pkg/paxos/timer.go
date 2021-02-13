package paxos

type TimeoutTimer struct {
}

func (timer *TimeoutTimer) RemainingTime() int {
	return 0
}

func (timer *TimeoutTimer) Wait(t int) {
	return
}
