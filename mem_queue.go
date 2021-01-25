package barriermq

import "time"

type state int

// MemQueue in memory queue
type MemQueue struct {
	TotalCount uint64
	InitTime   time.Time
	Msg        chan interface{}
	state      state
}

var _ Queue = new(MemQueue)

// NewMemQueue ...
func NewMemQueue(_size int64) *MemQueue {
	mq := &MemQueue{
		TotalCount: 0,
		InitTime:   time.Now(),
		state:      stateOpen,
	}
	if _size > 0 {
		mq.Msg = make(chan interface{}, _size)
	}
	return mq
}

// Consumer ...
func (mq *MemQueue) Consumer() <-chan interface{} {
	return mq.Msg
}
