package barriermq

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

var (
	serviceID uint64
	idPrefix  string
	m         sync.RWMutex
	services  map[string]*Service
	// ErrExists 服务已存在
	ErrExists error = errors.New("Service duplicate, plz check your config")
	hits      uint64
	misses    uint64
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
	nonBlockQueue  *NLockQueue
	// diskMsgQueue
	diskMsgQueue *DiskQueue
	total        uint64
	requeueCount uint64
	close        chan int
	closeDone    chan int
	newMsg       func() Message
	handler      Handler
	wakeup       chan int
	done         int32
	option       Options
	_lock        sync.RWMutex
}

// NewService .
func NewService(option Options) (*Service, error) {
	m.Lock()
	if _, ok := services[option.Name]; ok {
		return nil, ErrExists
	}
	diskqueue := NewDiskQueue(option)
	s := &Service{
		ID:             atomic.AddUint64(&serviceID, 1),
		memoryMsgQueue: NewMemQueue(option.MemQueueSize),
		nonBlockQueue:  NewNlockQueue(option.MemQueueSize),
		diskMsgQueue:   diskqueue,
		option:         option,
		wakeup:         make(chan int),
		close:          make(chan int, 1),
		closeDone:      make(chan int, 1),
		newMsg:         option.NewMsg,
	}
	services[option.Name] = s
	m.Unlock()
	s.background()
	// log.L.Println(option.Name, "service start success")
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
			// log.L.Errorln(err)
		}
	}
}

func (s *Service) background() {
	go func() {
		// log.L.Println(s.Name, "background job start")
		for {
			var _msg Message
			select {
			case <-s.close:
				// log.L.Println(s.Name, "background job exit success ...")
				s.closeDone <- 1
				runtime.Goexit()
			case msg := <-s.memoryMsgQueue.Consumer():
				switch msg.(type) {
				case Message:
					_msg = msg.(Message)
				case nil:
				default:
					// log.L.Println(s.Name, "msg assert error")
					// TODO
					continue
				}
			case msg := <-s.diskMsgQueue.Queue.ReadChan():
				_msg.Decode(msg)
			}
			/*
			 * send message
			 */
		loop:
			s._lock.RLock()
			if s.handler != nil {
				if err := s.handler(_msg); err != nil {
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
				s._lock.Unlock()
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
	// log.L.Println(s.Name, "start stop ...")
	s.close <- 1 // stop background worker first
	<-s.closeDone
	for {
		select {
		case each := <-s.memoryMsgQueue.Msg:
			switch each.(type) {
			case Message:
				msg := each.(Message)
				if err := s.diskMsgQueue.Put(msg.Encode()); err != nil {
					// log.L.Errorln(err)
				}
			case nil:
			}
		default:
			goto flush
		}
	}
flush:
	// log.L.Println(s.Name, "memory queue flush success ...")
	// log.L.Println(s.Name, "start disk queue ...")
	s.diskMsgQueue.Queue.Close() // stop disk queue
	// log.L.Println(s.Name, "disk queue flush success ...")
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
	// m["mem_success"] = atomic.LoadUint64(&s.memoryMsgQueue.SuccessCount)
	m["mem_current"] = len(s.memoryMsgQueue.Msg)
	m["disk_total"] = atomic.LoadUint64(&s.diskMsgQueue.TotalCount)
	m["disk_current"] = s.diskMsgQueue.Queue.Depth()
	m["cache_stats_lv2"] =
		map[string]uint64{
			"hits":   atomic.LoadUint64(&hits),
			"misses": atomic.LoadUint64(&misses),
		}
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
