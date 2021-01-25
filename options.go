package barriermq

import (
	"time"

	"github.com/nsqio/go-diskqueue"
)

// Options ...
type Options struct {
	// server part
	MemQueueSize int64
	FullWaitTime time.Duration
	// time to try requeue message
	RequeueTime time.Duration
	// disk queue part
	Name            string
	DataPath        string
	MaxBytesPerFile int64
	MinMsgSize      int32
	MaxMsgSize      int32
	SyncEvery       int64
	SyncTimeout     time.Duration
	Logf            diskqueue.AppLogFunc
	NewMsg          func() Message
}
