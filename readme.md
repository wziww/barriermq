## Barrier mq 消息队列
> 内置三种 mq，无锁化队列，chan，disk queue，投递顺序按照下方列表
 - 内存型无锁化队列投递
 - 内存型有锁队列投递（chan）
 - 本地追加落盘缓存

```go
// example
s, err := barriermq.NewService(barriermq.Options{
      MemQueueSize:    config.C.MemQueueSize,
      FullWaitTime:    time.Microsecond * time.Duration(config.C.Kafka.FullWaitMillisecond),
      RequeueTime:     time.Second,
      Name:            name,
      DataPath:        config.C.DataPath,
      MaxBytesPerFile: config.C.MaxBytesPerFile,
      MinMsgSize:      0,
      MaxMsgSize:      config.C.MaxMsgSize,
      SyncEvery:       config.C.SyncEvery,
      SyncTimeout:     time.Millisecond * time.Duration(config.C.SyncTimeout),
      Logf: func(level diskqueue.LogLevel, f string, args ...interface{}) {
        switch level {
        case diskqueue.ERROR:
            log.L.Errorf(f, args...)
        default:
            log.L.Infof(f, args...)
        }
      },
      NewMsg: func() barriermq.Message {
        return &message.Message{}
      },
    })

  s.RegistHandler(func(data interface{}) error {
    // do some thing
    if success {

      return nil

    } else {

      s.RequeueMessage(data)
      
    }
    return err
  })
s.put(nil)

```