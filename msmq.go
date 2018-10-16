package msmq

import (
	"context"
	"time"
)

// disallow concurrency
type Message interface {
	ID() int
	Start() error
	Done() error

	Topic() string
	Payload() (interface{}, error)
}

// the functions must concurrency
type Store interface {
	ScanRows(id int, topic string) (Rows, error)
	Insert(topic string, payload interface{}) error
}

type Rows interface {
	Next() bool
	Close() error
	Scan() (Message, error)
}

type Options struct {
	QueueCacheLength   int           // 默认 64
	StoreCheckInternal time.Duration // 默认 500ms
}

type Logger interface {
	Printf(format string, args ...interface{})
}

type MessageQueue struct {
	opts   *Options
	logger Logger
	store  Store
}

func NewMessageQueue(opts *Options, logger Logger, store Store) *MessageQueue {
	if opts.QueueCacheLength == 0 {
		opts.QueueCacheLength = 64
	}
	if opts.StoreCheckInternal == 0 {
		opts.StoreCheckInternal = 500 * time.Millisecond
	}
	return &MessageQueue{
		opts:   opts,
		logger: logger,
		store:  store,
	}
}

// allow Concurrency
func (mq *MessageQueue) Consume(ctx context.Context, topic string) (<-chan Message, <-chan struct{}) {
	ch := make(chan Message, mq.opts.QueueCacheLength)
	done := make(chan struct{})
	go func() {
		var (
			rows Rows
			err  error
			id   = 0
		)

		defer func() {
			if rows != nil {
				if err := rows.Close(); err != nil {
					mq.logf("rows Close err:", err)
					return
				}
			}

			close(ch)
			done <- struct{}{}
		}()

		for {
			rows, err = mq.store.ScanRows(id, topic)
			if err != nil {
				mq.logf("store ScanRows err:", err)
				return
			}

			for {
				select {
				case <-ctx.Done():
					mq.logf("topic:%s consumer canceled", topic)
					return
				default:
				}

				ok := rows.Next()
				if !ok {
					if err := rows.Close(); err != nil {
						mq.logf("rows Close err:", err)
						return
					}
					break
				}
				m, err := rows.Scan()
				if err != nil {
					mq.logf("rows scan err:", err)
					return
				}
				ch <- m

				id = m.ID()
			}

			time.Sleep(300 * time.Millisecond) // TODO 惰性检查间隔
		}
	}()

	return ch, done
}

// allow Concurrency
func (mq *MessageQueue) Produce(topic string, payload interface{}) error {
	return mq.store.Insert(topic, payload)
}

func (mq *MessageQueue) logf(format string, args ...interface{}) {
	mq.logger.Printf(format, args...)
}
