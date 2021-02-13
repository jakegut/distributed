package pingpong

import "math"

const RetryTime = 10

type PingTimer struct {
	remain int
}

func (timer *PingTimer) RemainingTime() int {
	return timer.remain
}

func (timer *PingTimer) Wait(t int) {
	timer.remain = int(math.Max(0, float64(timer.remain-t)))
}

func (timer *PingTimer) Reset() {
	timer.remain = RetryTime
}
