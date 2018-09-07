package gopool

import "sync"

// 每次直接开好指定数量的协程。为了简单
type Pool struct {
	maxGos int

	tasks chan func()
	wg    sync.WaitGroup
}

func NewPool(maxGos int) *Pool {
	pool := &Pool{
		maxGos: maxGos,
		tasks:  make(chan func(), maxGos),
	}

	go pool.run()

	return pool
}

func (p *Pool) run() {
	for i := 0; i < p.maxGos; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()

			for task := range p.tasks {
				task()
			}
		}()
	}
}

func (p *Pool) Go(cb func()) {
	p.tasks <- cb
}

func (p *Pool) Wait() {
	close(p.tasks)

	p.wg.Wait()
}
