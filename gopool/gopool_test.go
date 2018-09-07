package gopool

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TODO 比较下面差别

// 1 里面有一个 chan. 2 没有
// 怎么从性能上找出原因

func TestNewPool1(t *testing.T) {
	pool := NewPool(1)

	timeout := time.After(10 * time.Second)

	m := sync.Mutex{}
	a := 0

FOR:
	for {
		select {
		case <-timeout:
			break FOR
		default:
		}

		pool.Go(func() {
			m.Lock()
			a++
			m.Unlock()
		})
	}

	pool.Wait()
	fmt.Println(a)
}

func TestNewPool2(t *testing.T) {
	timeout := time.After(10 * time.Second)

	m := sync.Mutex{}
	a := 0

	//mm := sync.Mutex{}
	//syncMax := 0
	//
	//max := 0

	var wg sync.WaitGroup
FOR:
	for {
		select {
		case <-timeout:
			break FOR
		default:
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			//mm.Lock()
			//syncMax++
			//if syncMax > max {
			//	max = syncMax
			//}
			//mm.Unlock()

			m.Lock()
			a++
			m.Unlock()

			//mm.Lock()
			//syncMax--
			//mm.Unlock()
		}()
	}

	wg.Wait()
	fmt.Println(a)
	//fmt.Println("sync max", max)
}

func TestNewPool3(t *testing.T) {
	timeout := time.After(10 * time.Second)

	a := 0

FOR:
	for {
		select {
		case <-timeout:
			break FOR
		default:
		}

		a++
	}

	fmt.Println(a)
}
