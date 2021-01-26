package barriermq

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/nsqio/go-diskqueue"
)

var (
	open      uint32 = 0b00
	closed    uint32 = 0b01
	serviceID uint64
	idPrefix  string
	m         sync.RWMutex
	services  map[string]*Service
	// ErrExists ...
	ErrExists error = errors.New("Service duplicate, plz check your config")
)

func init() {
	m.Lock()
	defer m.Unlock()
	idPrefix = uuid.New().String()
	services = make(map[string]*Service)
}

// Service ...
type Service struct {
	ID uint64
	// in memory message queue
	memoryMsgQueue *MemQueue
	// first level - no locker memory queue
	nonBlockQueue *NLockQueue
	// diskMsgQueue
	diskMsgQueue *DiskQueue
	total        uint64
	requeueCount uint64
	close        chan int
	closeDone    chan int
	// background worker wakeup sig
	wakeup chan int
	// tag to decide whether to send wakeup sig
	done   int32
	option Options
	// handler fn lock
	_lock   sync.RWMutex
	newMsg  func() Message
	handler Handler
	logf    diskqueue.AppLogFunc
	status  uint32 // 0b00 open ob01 closed
}

// NewService .
func NewService(option Options) (*Service, error) {
	m.Lock()
	if _, ok := services[option.Name]; ok {
		return nil, ErrExists
	}
	dq := NewDiskQueue(option)
	s := &Service{
		ID:             atomic.AddUint64(&serviceID, 1),
		memoryMsgQueue: NewMemQueue(option),
		nonBlockQueue:  NewNlockQueue(option),
		diskMsgQueue:   dq,
		option:         option,
		wakeup:         make(chan int, 1),
		close:          make(chan int, 1),
		closeDone:      make(chan int, 1),
		newMsg:         option.NewMsg,
		logf:           option.Logf,
		status:         open,
	}
	if s.logf == nil {
		s.logf = func(level diskqueue.LogLevel, f string, args ...interface{}) {
			fmt.Printf(f, args...)
		}
	}
	services[option.Name] = s
	m.Unlock()
	s.background()
	s.logf(diskqueue.INFO, "%s %s", option.Name, "service start success")
	return s, nil
}

// RequeueMessage requeue 消息一律重新投递进磁盘
func (s *Service) RequeueMessage(msg Message) error {
	timer := time.NewTimer(s.option.RequeueTime)
	select {
	case <-timer.C:
	}
	atomic.AddUint64(&s.requeueCount, 1)
	return s.diskMsgQueue.Put(msg.Encode())
}

// RegistHandler ...
func (s *Service) RegistHandler(fn Handler) {
	s._lock.Lock()
	defer s._lock.Unlock()
	s.handler = fn
	s.nonBlockQueue.RegistHandler(fn)
}

// FindService ...
func FindService(name string) *Service {
	m.RLock()
	s := services[name]
	m.RUnlock()
	return s
}

/*
 * Put put mesage
 * 消息投递尝试优先级：
 * 1. 内存型无锁化队列投递
 * 2. 内存型有锁队列投递（chan）
 * 3. 本地追加落盘缓存
 */
func (s *Service) Put(data Message) {
	if atomic.LoadUint32(&s.status)&closed == 1 {
		return
	}
	atomic.AddUint64(&s.total, 1)
	// 无锁队列尝试写入
	if s.nonBlockQueue.Push(data) {
		return
	}
	select {
	case s.memoryMsgQueue.Msg <- data:
		atomic.AddUint64(&s.memoryMsgQueue.TotalCount, 1)
	default:
		err := s.diskMsgQueue.Put(data.Encode())
		if err != nil {
			s.logf(diskqueue.ERROR, "%s", err.Error())
		}
	}
}

func (s *Service) background() {
	go func() {
		s.logf(diskqueue.INFO, "%s %s", s.option.Name, "background job start")
		for {
			var _msg Message
			select {
			case <-s.close:
				s.logf(diskqueue.INFO, "%s %s", s.option.Name, "background job exit success ...")
				s.closeDone <- 1
				runtime.Goexit()
			case msg := <-s.memoryMsgQueue.Consumer():
				switch msg.(type) {
				case Message:
					_msg = msg.(Message)
				case nil:
				default:
					s.logf(diskqueue.ERROR, "%s %s", s.option.Name, "msg assert error")
					// TODO
					continue
				}
			case msg := <-s.diskMsgQueue.Queue.ReadChan():
				_msg = s.newMsg()
				_msg.Decode(msg)
			}
			/*
			 * send message
			 */
		loop:
			s._lock.RLock()
			fn := s.handler
			s._lock.RUnlock()
			if fn != nil {
				if err := fn(_msg); err != nil {
					timer := time.NewTimer(s.option.FullWaitTime)
					select {
					case <-timer.C:
						goto loop
					case <-s.Wakeup():
						s.Sleep()
						timer.Stop()
						goto loop
					}
				}
			}
		}
	}()
}

// Wakeup ...
func (s *Service) Wakeup() <-chan int {
	return s.wakeup
}

// Sleep ...
func (s *Service) Sleep() {
	atomic.StoreInt32(&s.done, FreeStatus)
}

// Release ...
func (s *Service) Release() {
	if atomic.CompareAndSwapInt32(&s.done, FreeStatus, LockStatus) {
		s.wakeup <- 1
	}
}

// Exit ...
func (s *Service) Exit() {
	s.logf(diskqueue.INFO, "%s %s", s.option.Name, "begin to stop ...")
	atomic.StoreUint32(&s.status, closed) // close put method
	s.close <- 1                          // stop background worker first
	<-s.closeDone
	/* Flush no-locker queue message to disk
	 * Depends on whether use s.RequeueMessage(&v) in RegistHandler func
	 */
	t := time.NewTicker(time.Second)
	var times int
	for range t.C {
		if s.nonBlockQueue.Len() == 0 {
			s.logf(diskqueue.INFO, "%s", s.option.Name, "non block queue success")
			break
		}
		if times >= 10 {
			s.logf(diskqueue.ERROR, "%s %s %d %s", s.option.Name, "non block queue flush error, lost about", s.nonBlockQueue.Len(), "message")
			break
		}
		times++
	}
	// flush chan queue message to disk
	for {
		select {
		case each := <-s.memoryMsgQueue.Msg:
			switch each.(type) {
			case Message:
				msg := each.(Message)
				if err := s.diskMsgQueue.Put(msg.Encode()); err != nil {
					s.logf(diskqueue.ERROR, "%s %s", s.option.Name, err.Error())
					// log.L.Errorln(err)
				}
			case nil:
			}
		default:
			goto flush
		}
	}
flush:
	s.logf(diskqueue.INFO, "%s %s", s.option.Name, "memory queue flush success ...")
	s.logf(diskqueue.INFO, "%s %s", s.option.Name, "start disk queue ...")
	s.diskMsgQueue.Queue.Close() // stop disk queue
	s.logf(diskqueue.INFO, "%s %s", s.option.Name, "disk queue flush success ...")
}

// Debug ...
func (s *Service) Debug() map[string]interface{} {
	m := map[string]interface{}{}
	m["total"] = atomic.LoadUint64(&s.total)
	m["requeue"] = atomic.LoadUint64(&s.requeueCount)
	m["nlock_queue_current"] = s.nonBlockQueue.Len()
	m["nlock_queue_total"] = atomic.LoadUint64(&s.nonBlockQueue.Success) + atomic.LoadUint64(&s.nonBlockQueue.Failed)
	m["nlock_queue_success"] = atomic.LoadUint64(&s.nonBlockQueue.Success)
	m["nlock_queue_failed"] = atomic.LoadUint64(&s.nonBlockQueue.Failed)
	m["mem_total"] = atomic.LoadUint64(&s.memoryMsgQueue.TotalCount)
	m["mem_current"] = len(s.memoryMsgQueue.Msg)
	m["disk_total"] = atomic.LoadUint64(&s.diskMsgQueue.TotalCount)
	m["disk_current"] = s.diskMsgQueue.Queue.Depth()

	return m
}

// StopAll 关闭接入层所有 server & 数据持久化
func StopAll() {
	m.Lock()
	var each sync.WaitGroup
	for k, v := range services {
		_ = k // avoid building error
		each.Add(1)
		go func(v *Service) {
			defer each.Done()
			v.Exit()
		}(v)
	}
	each.Wait()
	m.Unlock()
}
