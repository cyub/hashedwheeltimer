package hashedwheeltimer

import (
	"sync/atomic"
	"time"

	"github.com/cyub/hashedwheeltimer/queue"
)

const (
	TIMER_STATE_INIT = iota
	TIMER_STATE_STARTED
	TIMER_STATE_SHUTDOWN

	MaxInt64 = int64(^uint64(0) >> 1)
)

type HashedWheelTimer struct {
	tickDuration      time.Duration
	mask              int
	startTime         int64
	wheel             []*HashedWheelBucket
	timeouts          *queue.MpscUnboundedArrayQueue
	cancelledTimeouts *queue.MpscUnboundedArrayQueue
	state             int64
	tick              int
}

func NewHashedWheelTimer(tickDuration time.Duration, ticksPerWheel int) *HashedWheelTimer {
	if tickDuration < time.Millisecond || tickDuration > 365*24*time.Hour {
		tickDuration = 100 * time.Millisecond
	}
	if ticksPerWheel < 1 || ticksPerWheel > 2^20 {
		ticksPerWheel = 512
	}

	hwt := &HashedWheelTimer{
		tickDuration:      tickDuration,
		wheel:             createWheel(ticksPerWheel),
		timeouts:          queue.NewMpscUnboundedArrayQueue(128),
		cancelledTimeouts: queue.NewMpscUnboundedArrayQueue(128),
	}
	hwt.mask = len(hwt.wheel) - 1
	return hwt
}

func New() *HashedWheelTimer {
	return NewHashedWheelTimer(100*time.Millisecond, 512)
}

func (hwt *HashedWheelTimer) NewTimeout(task TimeTask, delay time.Duration) Timeout {
	hwt.start()
	deadline := time.Now().UnixNano() + int64(delay) - hwt.startTime
	if delay > 0 && deadline < 0 {
		deadline = MaxInt64
	}
	timeout := NewHashedWheelTimeout(hwt, task, deadline)
	hwt.timeouts.Offer(timeout)
	return timeout
}

func (hwt *HashedWheelTimer) start() {
	hwt.startTime = time.Now().UnixNano()
	if hwt.startTime == 0 {
		hwt.startTime = 1
	}

	switch atomic.LoadInt64(&hwt.state) {
	case TIMER_STATE_INIT:
		if atomic.CompareAndSwapInt64(&hwt.state, TIMER_STATE_INIT, TIMER_STATE_STARTED) {
			go hwt.run()
		}
	case TIMER_STATE_STARTED:
		return
	case TIMER_STATE_SHUTDOWN:
	default:
		panic("Invalid state")
	}
}

func (hwt *HashedWheelTimer) Stop() {
	state := atomic.LoadInt64(&hwt.state)
	if state == TIMER_STATE_SHUTDOWN {
		return
	}
	if atomic.CompareAndSwapInt64(&hwt.state, TIMER_STATE_STARTED, TIMER_STATE_SHUTDOWN) {
		return
	}
	atomic.StoreInt64(&hwt.state, TIMER_STATE_SHUTDOWN) // it shoud be TIMER_STATE_INIT, force to shutdown
}

func (hwt *HashedWheelTimer) run() {
	for atomic.LoadInt64(&hwt.state) == TIMER_STATE_STARTED {
		deadline := hwt.waitForNextTick()
		hwt.processCancelledTasks()
		hwt.transferTimeoutsToBuckets()

		bucket := hwt.wheel[hwt.tick&hwt.mask]
		bucket.expireTimeouts(deadline)
		hwt.tick++
	}
	hwt.processCancelledTasks()
}

func (hwt *HashedWheelTimer) waitForNextTick() time.Duration {
	deadline := hwt.tickDuration * time.Duration(hwt.tick+1)
	for {
		currentTime := time.Duration(time.Now().UnixNano() - hwt.startTime)
		sleepTime := (deadline - currentTime + 999999) / 1000000
		if sleepTime <= 0 { // reached the tick
			return currentTime
		}
		time.Sleep(sleepTime)
	}
}

func (hwt *HashedWheelTimer) processCancelledTasks() {
	var (
		timeout *HashedWheelTimeout
		ok      bool
	)
	for {
		timeout, ok = hwt.cancelledTimeouts.Poll().(*HashedWheelTimeout)
		if !ok {
			break
		}
		timeout.remove()
	}
}

func (hwt *HashedWheelTimer) transferTimeoutsToBuckets() {
	var (
		timeout *HashedWheelTimeout
		ok      bool
	)
	for i := 0; i < 100000; i++ {
		timeout, ok = hwt.timeouts.Poll().(*HashedWheelTimeout)
		if !ok {
			break
		}

		if timeout.State() == TIMEOUT_STATE_CANCELLED {
			continue
		}

		calculated := int(timeout.deadline / int64(hwt.tickDuration))
		timeout.remainingRounds = (calculated - int(hwt.tick)) / len(hwt.wheel)
		ticks := max(calculated, int(hwt.tick))

		stopIndex := ticks & hwt.mask
		hwt.wheel[stopIndex].addTimeout(timeout)
	}
}

func createWheel(ticksPerWheel int) []*HashedWheelBucket {
	var normalizedTicksPerWheel = 1
	for normalizedTicksPerWheel < ticksPerWheel {
		normalizedTicksPerWheel = normalizedTicksPerWheel << 1
	}

	buckets := make([]*HashedWheelBucket, normalizedTicksPerWheel)
	for i := 0; i < normalizedTicksPerWheel; i++ {
		buckets[i] = NewHashedWheelBucket()
	}
	return buckets
}

func max(a int, b int) int {
	if a >= b {
		return a
	}
	return b
}
