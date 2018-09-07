package msmq

import "time"

type StoreRowStatus int8

const (
	StoreRowStatusUndo  StoreRowStatus = 0
	StoreRowStatusDone  StoreRowStatus = 1
	StoreRowStatusDoing StoreRowStatus = 2
)

type Task struct {
	Id      int64
	Topic   string
	Status  StoreRowStatus
	Version int64
}

type MQStore struct {
	Id         int64          `json:"id"`
	Status     StoreRowStatus `json:"status"`
	Version    int64          `json:"version"`
	Topic      string         `json:"topic"`
	Payload    []byte         `json:"payload"`
	StartTime  int64          `json:"start_time"`
	EndTime    int64          `json:"end_time"`
	CreateTime time.Time      `json:"create_time"`
}

// TODO 服务启动的时候，处理中间状态

// 循环查找 status == 0 || status == 2 && now - start_time > 60s
type NextFunc func() (row Task, err error)

// id status(0 待执行, 1 执行完成, 2 执行中) topic payload start_time(status = 2 有效) end_time(执行完成的时间，用于统计)
type StoreInterface interface {
	Insert(topic string, payload []byte) error
	FetchNext(done <-chan struct{}) (NextFunc, error)
	Select(id int64) (*MQStore, error) // TODO *Row
	Start(task Task) error             // 锁定状态 和 开始执行时间
	Done(task Task) error              // 如果存在并行时，更新失败（以前一状态和开始时间作为条件）。记录日志。
}
