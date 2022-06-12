package queue

import (
	"testing"
)

func TestOfferPoll(t *testing.T) {
	muaq := NewMpscUnboundedArrayQueue(8)

	var i int
	for i = 1; i <= 5; i++ {
		muaq.Offer(i)
	}

	for {
		d := muaq.Poll()
		if d == nil {
			break
		}
		t.Log("poll: ", d)
	}

	for i = 1; i <= 15; i++ {
		muaq.Offer(i)
	}

	for {
		d := muaq.Poll()
		if d == nil {
			break
		}
		t.Log("poll: ", d)
	}
}
