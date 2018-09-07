package msmq

type GoPoolInterface interface {
	Go(func())
	Wait()
}
