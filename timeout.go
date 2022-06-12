package hashedwheeltimer

import "sync/atomic"

const (
	TIMEOUT_STATE_INIT = iota
	TIMEOUT_STATE_CANCELLED
	TIMEOUT_STATE_EXPIRED
)

type Timeout interface {
	Timer() *HashedWheelTimer
	Task() TimeTask
	IsExpired() bool
	IsCancelled() bool
	Cancel() bool
	Deadline() int64
}

type HashedWheelTimeout struct {
	timer           *HashedWheelTimer
	task            TimeTask
	deadline        int64
	state           int64
	bucket          *HashedWheelBucket
	next            *HashedWheelTimeout
	prev            *HashedWheelTimeout
	remainingRounds int
}

func NewHashedWheelTimeout(timer *HashedWheelTimer, task TimeTask, deadline int64) Timeout {
	return &HashedWheelTimeout{
		timer:    timer,
		task:     task,
		state:    TIMEOUT_STATE_INIT,
		deadline: deadline,
	}
}

func (timeout *HashedWheelTimeout) Timer() *HashedWheelTimer {
	return timeout.timer
}

func (timeout *HashedWheelTimeout) Cancel() bool {
	if !atomic.CompareAndSwapInt64(&timeout.state, TIMEOUT_STATE_INIT, TIMEOUT_STATE_CANCELLED) {
		return false
	}
	timeout.timer.cancelledTimeouts.Offer(timeout)
	return true
}

func (timeout *HashedWheelTimeout) IsExpired() bool {
	return atomic.LoadInt64(&timeout.state) == TIMEOUT_STATE_EXPIRED
}

func (timeout *HashedWheelTimeout) Task() TimeTask {
	return timeout.task
}

func (timeout *HashedWheelTimeout) IsCancelled() bool {
	return atomic.LoadInt64(&timeout.state) == TIMEOUT_STATE_CANCELLED
}

func (timeout *HashedWheelTimeout) Run() bool {
	return timeout.task.Run(timeout)
}

func (timeout *HashedWheelTimeout) Deadline() int64 {
	return timeout.deadline
}

func (timeout *HashedWheelTimeout) remove() {
	bucket := timeout.bucket
	if bucket == nil {
		return
	}
	bucket.remove(timeout)
}

func (timeout *HashedWheelTimeout) Expire() {
	if !atomic.CompareAndSwapInt64(&timeout.state, TIMEOUT_STATE_INIT, TIMEOUT_STATE_EXPIRED) {
		return
	}
	timeout.Run()
}

func (timeout *HashedWheelTimeout) State() int64 {
	return atomic.LoadInt64(&timeout.state)
}
