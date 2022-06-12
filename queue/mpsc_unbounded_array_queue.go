package queue

import (
	"sync/atomic"
)

const (
	CONTINUE_TO_P_INDEX_CAS = iota
	RETRY
	QUEUE_FULL
	QUEUE_RESIZE

	UNBOUNDED_CAPACITY = -1
	MaxInt64           = int64(^uint64(0) >> 1)
)

var (
	JUMP_PLACEHOLDER            interface{} = new(placeholder)
	BUFFER_CONSUMED_PLACEHOLDER interface{} = new(placeholder)
)

type MpscUnboundedArrayQueue struct {
	producerIndex  int64
	producerLimit  int64
	producerMask   int64
	producerBuffer []interface{}

	consumerIndex  int64
	consumerMask   int64
	consumerBuffer []interface{}
}

func NewMpscUnboundedArrayQueue(capacity int) *MpscUnboundedArrayQueue {
	if capacity < 2 {
		capacity = 128
	}
	p2capacity := 1
	for p2capacity < capacity {
		p2capacity = p2capacity << 1
	}

	mask := int64((p2capacity - 1) << 1)
	buffer := make([]interface{}, p2capacity+1)
	return &MpscUnboundedArrayQueue{
		producerIndex:  0,
		producerLimit:  mask,
		producerMask:   mask,
		producerBuffer: buffer,
		consumerIndex:  0,
		consumerMask:   mask,
		consumerBuffer: buffer,
	}
}

type placeholder struct{}

func (muaq *MpscUnboundedArrayQueue) Offer(e interface{}) bool {
	var (
		mask   int64
		buffer []interface{}
		pIndex int64
	)

	for {
		producerLimit := muaq.producerLimit
		pIndex = atomic.LoadInt64(&muaq.producerIndex)
		if pIndex&1 == 1 {
			continue
		}

		mask = muaq.producerMask
		buffer = muaq.producerBuffer

		if producerLimit <= pIndex {
			switch muaq.offerSlowPath(mask, pIndex, producerLimit) {
			case CONTINUE_TO_P_INDEX_CAS:
			case RETRY:
				continue
			case QUEUE_FULL: // unbounded queue never reach here
				return false
			case QUEUE_RESIZE:
				muaq.resize(mask, buffer, pIndex, e)
				return true
			}
		}

		if atomic.CompareAndSwapInt64(&muaq.producerIndex, pIndex, pIndex+2) {
			break
		}
	}

	offset := muaq.modifiedCalcCircularRefElementOffset(pIndex, mask)
	buffer[offset] = e
	return true
}

func (muaq *MpscUnboundedArrayQueue) offerSlowPath(mask, pIndex, producerLimit int64) int {
	cIndex := atomic.LoadInt64(&muaq.consumerIndex)
	bufferCapacity := muaq.getCurrentBufferCapacity(mask)
	if bufferCapacity > pIndex-cIndex {
		if !atomic.CompareAndSwapInt64(&muaq.producerLimit, producerLimit, cIndex+bufferCapacity) {
			return RETRY
		}
		return CONTINUE_TO_P_INDEX_CAS
	}

	if (muaq.availableInQueue(pIndex, cIndex)) <= 0 {
		return QUEUE_FULL
	} else if atomic.CompareAndSwapInt64(&muaq.producerIndex, pIndex, pIndex+1) {
		return QUEUE_RESIZE
	} else {
		return RETRY
	}
}

func (muaq *MpscUnboundedArrayQueue) resize(oldMask int64, oldBuffer []interface{}, pIndex int64, e interface{}) {
	newBufferLen := muaq.getNextBufferSize(oldBuffer)
	newBuffer := make([]interface{}, newBufferLen)
	muaq.producerBuffer = newBuffer

	// update mask
	newMask := (newBufferLen - 2) << 1
	muaq.producerMask = int64(newMask)

	offsetInOld := muaq.modifiedCalcCircularRefElementOffset(pIndex, oldMask)
	offsetInNew := muaq.modifiedCalcCircularRefElementOffset(pIndex, int64(newMask))

	// put element into new buffer
	newBuffer[offsetInNew] = e
	// new buffer linked to old buffer
	oldBuffer[muaq.nextArrayOffset(oldMask)] = newBuffer

	cIndex := atomic.LoadInt64(&muaq.consumerIndex)
	availableInQueue := muaq.availableInQueue(pIndex, cIndex)
	producerLimit := newMask
	if availableInQueue < int64(newMask) {
		producerLimit = int(availableInQueue)
	}

	// update producerLimit
	atomic.StoreInt64(&muaq.producerLimit, pIndex+int64(producerLimit))
	// update producerIndex
	atomic.StoreInt64(&muaq.producerIndex, pIndex+2)
	// set jump placeholder
	oldBuffer[offsetInOld] = JUMP_PLACEHOLDER
}

func (muaq *MpscUnboundedArrayQueue) nextArrayOffset(mask int64) int {
	return muaq.modifiedCalcCircularRefElementOffset(mask+2, MaxInt64)
}

func (muaq *MpscUnboundedArrayQueue) modifiedCalcCircularRefElementOffset(index, mask int64) int {
	return int(index & mask >> 1)
}

func (muaq *MpscUnboundedArrayQueue) getNextBufferSize(buffer []interface{}) int {
	return len(buffer)
}

func (muaq *MpscUnboundedArrayQueue) getCurrentBufferCapacity(mask int64) int64 {
	return mask
}

func (muaq *MpscUnboundedArrayQueue) availableInQueue(pIndex, cIndex int64) int64 {
	return MaxInt64
}

func (muaq *MpscUnboundedArrayQueue) Poll() interface{} {
	buffer := muaq.consumerBuffer
	index := atomic.LoadInt64(&muaq.consumerIndex)
	mask := muaq.consumerMask
	offset := muaq.modifiedCalcCircularRefElementOffset(index, mask)

	var e interface{}
	e = buffer[offset]
	if e == nil {
		if index != atomic.LoadInt64(&muaq.producerIndex) {
			for {
				e = buffer[offset]
				if e != nil {
					break
				}
			}
		} else {
			return nil
		}
	}

	if e == JUMP_PLACEHOLDER {
		nextBuffer := muaq.nextBuffer(buffer, mask)
		return muaq.newBufferPoll(nextBuffer, index)
	}

	buffer[offset] = nil
	atomic.StoreInt64(&muaq.consumerIndex, index+2)
	return e
}

func (muaq *MpscUnboundedArrayQueue) nextBuffer(buffer []interface{}, mask int64) []interface{} {
	offset := muaq.nextArrayOffset(mask)
	nextBuffer := buffer[offset].([]interface{})
	muaq.consumerBuffer = nextBuffer

	muaq.consumerMask = int64((len(nextBuffer) - 2) << 1)
	// mark current buffer all consumed
	buffer[offset] = BUFFER_CONSUMED_PLACEHOLDER
	return nextBuffer
}

func (muaq *MpscUnboundedArrayQueue) newBufferPoll(nextBuffer []interface{}, index int64) interface{} {
	offset := muaq.modifiedCalcCircularRefElementOffset(index, muaq.consumerMask)
	e := nextBuffer[offset]
	nextBuffer[offset] = nil
	atomic.StoreInt64(&muaq.consumerIndex, index+2)
	return e
}

func (muaq *MpscUnboundedArrayQueue) Capacity() int {
	return UNBOUNDED_CAPACITY
}

func (muaq *MpscUnboundedArrayQueue) IsEmpty() bool {
	return (atomic.LoadInt64(&muaq.consumerIndex)-atomic.LoadInt64(&muaq.producerIndex))/2 == 0
}

func (muaq *MpscUnboundedArrayQueue) Size() int64 {
	var (
		size   int64
		before int64
		after  = atomic.LoadInt64(&muaq.consumerIndex)
	)

	for {
		before = after
		currentProducerIndex := atomic.LoadInt64(&muaq.producerIndex)
		after = atomic.LoadInt64(&muaq.consumerIndex)
		if before == after {
			size = currentProducerIndex - after/2
			break
		}
	}

	if size < 0 {
		return 0
	}
	if size > MaxInt64 {
		return MaxInt64
	}
	return size
}
