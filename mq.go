package msmq

import (
	"errors"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// TODO 基于读写锁的并发Map 与 sync.Map

type ConsumerFunc func(taskId int64, payload []byte)

type Options struct {
	Debug               bool
	QueueLen            int
	DefaultConsumersLen int
}

type Server struct {
	opts             *Options
	log              logrus.FieldLogger
	store            StoreInterface
	done             chan struct{}
	queue            chan Task
	gopool           GoPoolInterface
	consumersRWMutex sync.RWMutex
	consumers        map[string]ConsumerFunc
	wg               sync.WaitGroup
}

func NewServer(opts *Options, log logrus.FieldLogger, store StoreInterface, gopool GoPoolInterface) *Server {
	log = log.WithField("type", "mqq.Server")
	return &Server{
		opts:      opts,
		log:       log,
		store:     store,
		done:      make(chan struct{}),
		queue:     make(chan Task, opts.QueueLen),
		gopool:    gopool,
		consumers: make(map[string]ConsumerFunc, opts.DefaultConsumersLen),
	}
}

func (s *Server) Produce(topic string, payload []byte) error {
	return s.store.Insert(topic, payload)
}

func (s *Server) getConsumer(topic string) ConsumerFunc {
	s.consumersRWMutex.RLock()
	consumer, _ := s.consumers[topic]
	s.consumersRWMutex.RUnlock()
	return consumer
}

func (s *Server) Register(topic string, consumer ConsumerFunc) error {
	s.consumersRWMutex.RLock()
	if _, exists := s.consumers[topic]; exists {
		s.consumersRWMutex.RUnlock()
		return errors.New("topic has existed")
	}
	s.consumersRWMutex.RUnlock()

	s.consumersRWMutex.Lock()
	if _, exists := s.consumers[topic]; exists {
		s.consumersRWMutex.Unlock()
		return errors.New("topic has existed")
	}
	s.consumers[topic] = consumer
	s.consumersRWMutex.Unlock()
	return nil
}

func (s *Server) Run() error {
	// TODO 需要重构一下这个地方
	next, err := s.store.FetchNext(s.done)
	if err != nil {
		return err
	}

	s.wg.Add(1)
	go func() {
		s.consumer()
		s.wg.Done()
	}()

	go s.producer(next)

	return nil
}

// 从 store 中反复读取数据，放入 queue
func (s *Server) producer(next NextFunc) {
FOR:
	for {
		// TODO 需要重构一下这个地方，让这里的done生效
		select {
		case <-s.done:
			break FOR
		default:
		}

		task, err := next()
		if err != nil {
			s.log.Error("next err:", err) // TODO 日志太多了
			continue
		}

		if s.opts.Debug {
			time.Sleep(30 * time.Second)
		}
		s.queue <- task
	}
	close(s.queue)
}

// 从 queue 中读取数据，然后执行
func (s *Server) consumer() {
	for task := range s.queue {
		s.gopool.Go(func() {
			s.exec(task)
		})
	}
	s.gopool.Wait()
}

// Start
// Done
func (s *Server) exec(task Task) {
	log := s.log.WithField("task_id", task.Id).WithField("method", "exec")

	consumer := s.getConsumer(task.Topic)
	if consumer == nil {
		log.Error("no topic consumer")
		return
	}

	if err := s.store.Start(task); err != nil {
		log.Error("store Start err:", err)
		return
	}

	row, err := s.store.Select(task.Id)
	if err != nil {
		log.Error("store Select err:", err) // TODO 不太应该失败
		return
	}

	// Important
	task.Status = row.Status
	task.Version = row.Version

	consumer(row.Id, row.Payload) // TODO 要不要加上 error 来 ACK

	if err := s.store.Done(task); err != nil {
		log.Error("store Done err:", err)
		log.Infof("task.Id %d, start_time: %f, end_time: %f",
			row.Id,
			row.StartTime,
			time.Now().UnixNano(),
		)
	}
}

func (s *Server) Close() {
	close(s.done)
	s.wg.Wait()
}
