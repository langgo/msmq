package store

import (
	"testing"

	"log"
	"os"

	"context"

	"time"

	"fmt"

	"sync"

	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/langgo/msmq"
)

func GetMQ() (msmq.MessageQueue, error) {
	mopts := Options{
		Debug:     false,
		User:      "cloud",
		Password:  "212147a1c567bb9e",
		Address:   "mysql.yun3.com:3306",
		DBName:    "msmq",
		TableName: "mq",
	}
	store, err := NewMysqlStore(&mopts, &DefaultPayload{})
	if err != nil {
		return nil, err
	}

	opts := msmq.Options{}
	mq := msmq.NewMessageQueue(&opts, log.New(os.Stderr, "", log.LstdFlags), store)
	return mq, nil
}

func Test1(t *testing.T) {
	mopts := Options{
		Debug:     false,
		User:      "cloud",
		Password:  "212147a1c567bb9e",
		Address:   "mysql.yun3.com:3306",
		DBName:    "msmq",
		TableName: "mq",
	}
	store, err := NewMysqlStore(&mopts, &DefaultPayload{})
	if err != nil {
		t.Error(err)
	}

	opts := msmq.Options{}
	mq := msmq.NewMessageQueue(&opts, log.New(os.Stderr, "", log.LstdFlags), store)

	go func() {
		ch := mq.Consume(context.Background(), "test")

		for msg := range ch {
			if err := msg.Start(); err != nil {
				t.Error(err)
			}

			p, err := msg.Payload()
			if err != nil {
				t.Error(err)
			}

			fmt.Printf("%s: %s\n", msg.Topic(), string(p.([]byte)))

			if err := msg.Done(); err != nil {
				t.Error(err)
			}
		}
	}()

	if err := mq.Produce("test", []byte("test data")); err != nil {
		t.Error(err)
	}
	if err := mq.Produce("test1", []byte("test data 1")); err != nil {
		t.Error(err)
	}
	if err := mq.Produce("test", []byte("test data 1")); err != nil {
		t.Error(err)
	}

	n := time.Now()
	for time.Now().Before(n.Add(2 * time.Second)) {

	}
}

func BenchmarkProduce(b *testing.B) {
	mq, err := GetMQ()
	if err != nil {
		b.Error(err)
	}

	for n := 0; n < b.N; n++ {
		if err := mq.Produce("test", []byte("test data 1")); err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkProduceParallel(b *testing.B) {
	mq, err := GetMQ()
	if err != nil {
		b.Error(err)
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := mq.Produce("test", []byte("test data 1")); err != nil {
				b.Error(err)
			}
		}
	})
}

func TestBenchmarkConsume1(t *testing.T) {
	mq, err := GetMQ()
	if err != nil {
		t.Error(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	ch := mq.Consume(ctx, "test")
	var wg sync.WaitGroup

	wg.Add(1)
	var count = 0
	var delta time.Duration

	go func() {
		st := time.Now()
		defer wg.Done()

		for msg := range ch {
			if err := msg.Start(); err != nil {
				if err == ErrMessageIng {
					continue
				}
				t.Error(err)
			}

			count++

			if err := msg.Done(); err != nil {
				t.Error(err)
			}
		}
		delta = time.Now().Sub(st)
	}()

	n := time.Now().Add(5 * time.Second)
	for time.Now().Before(n) {

	}
	cancel()
	wg.Wait()

	fmt.Println(count, delta)
}
