package barriermq

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-diskqueue"
)

// BackendQueue represents the behavior for the secondary message
// storage system
type BackendQueue interface {
	Put([]byte) error
	ReadChan() <-chan []byte // this is expected to be an *unbuffered* channel
	Close() error
	Delete() error
	Depth() int64
	Empty() error
}

// DiskQueue ...
type DiskQueue struct {
	TotalCount uint64
	Queue      BackendQueue
	close      chan int
}

var _ Queue = new(DiskQueue)

// NewDiskQueue ...
func NewDiskQueue(option Options) *DiskQueue {
	t := &DiskQueue{}
	if option.Logf == nil {
		option.Logf = func(level diskqueue.LogLevel, f string, args ...interface{}) {
			fmt.Printf(f, args...)
		}
	}
	t.Queue = diskqueue.New(
		option.Name,
		option.DataPath,
		// ctx.nsqd.getOpts().MaxBytesPerFile,
		option.MaxBytesPerFile,
		// int32(minValidMsgLength),
		option.MinMsgSize,
		// int32(ctx.nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
		option.MaxMsgSize,
		// ctx.nsqd.getOpts().SyncEvery,
		option.SyncEvery,
		// ctx.nsqd.getOpts().SyncTimeout,
		time.Millisecond*time.Duration(option.SyncTimeout),
		// dqLogf,
		option.Logf,
	)
	return t
}
func (q *DiskQueue) Put(body []byte) error {
	atomic.AddUint64(&q.TotalCount, 1)
	return q.Queue.Put(body)
}

// Consumer void
func (mq *DiskQueue) Consumer() <-chan interface{} {
	// void
	return nil
}
