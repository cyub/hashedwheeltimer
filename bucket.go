package hashedwheeltimer

import (
	"fmt"
	"time"
)

type HashedWheelBucket struct {
	head *HashedWheelTimeout
	tail *HashedWheelTimeout
}

func NewHashedWheelBucket() *HashedWheelBucket {
	return &HashedWheelBucket{}
}

func (bucket *HashedWheelBucket) addTimeout(timeout *HashedWheelTimeout) {
	timeout.bucket = bucket
	if bucket.head == nil {
		bucket.head = timeout
		bucket.tail = timeout
	} else {
		bucket.tail.next = timeout
		timeout.prev = bucket.tail
		bucket.tail = timeout
	}
}

func (bucket *HashedWheelBucket) expireTimeouts(deadline time.Duration) {
	var timeout = bucket.head

	for timeout != nil {
		next := timeout.next
		if timeout.remainingRounds <= 0 {
			next = bucket.remove(timeout)
			if time.Duration(timeout.deadline) <= deadline {
				timeout.Expire()
			} else { // should never reach here
				panic(fmt.Sprintf("expireTimeouts timeout.deadline (%d) > deadline (%d)", timeout.deadline, deadline))
			}
		} else if timeout.IsCancelled() {
			next = bucket.remove(timeout)
		} else {
			timeout.remainingRounds--
		}
		timeout = next
	}
}

func (bucket *HashedWheelBucket) remove(timeout *HashedWheelTimeout) *HashedWheelTimeout {
	next := timeout.next
	if timeout.prev != nil {
		timeout.prev.next = next
	}

	if timeout.next != nil {
		timeout.next.prev = timeout.prev
	}

	if timeout == bucket.head {
		if timeout == bucket.tail {
			bucket.head = nil
			bucket.tail = nil
		} else {
			bucket.head = next
		}
	} else if timeout == bucket.tail {
		bucket.tail = timeout.prev
	}

	// clear timeout
	timeout.prev = nil
	timeout.next = nil
	timeout.bucket = nil
	return next
}
