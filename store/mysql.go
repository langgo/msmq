package store

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jinzhu/gorm"
	"github.com/langgo/msmq"
)

var (
	ErrMessageIng      = errors.New("the message is consuming")
	ErrMessageDone     = errors.New("the message is consumed")
	ErrAssertFail      = errors.New("assert failure")
	ErrMessageNotStart = errors.New("message not start")
)

type Payloader interface {
	Encode(topic string, payload interface{}) ([]byte, error)
	Decode(topic string, payload []byte) (interface{}, error)
}

var DefaultPayload = &defaultPayload{}

type defaultPayload struct{}

func (dp *defaultPayload) Decode(topic string, payload []byte) (interface{}, error) {
	return payload, nil
}

func (dp *defaultPayload) Encode(topic string, payload interface{}) ([]byte, error) {
	p, ok := payload.([]byte)
	if !ok {
		return nil, ErrAssertFail
	}
	return p, nil
}

type mysqlStore struct {
	opts      *Options
	db        *gorm.DB
	payloader Payloader
}

type Options struct {
	Debug bool

	User        string
	Password    string
	Address     string
	DBName      string
	TableName   string
	Timeout     string
	ReadTimeout string

	// 假设任务最长执行时间为 a
	// 假设宕机最长时间为 b
	// 下面时间为 c
	// 应当满足 b < c < a
	ReadRepeatTimeout time.Duration
}

func NewMysqlStore(opts *Options, payloader Payloader) (*mysqlStore, error) {
	if opts.Timeout == "" {
		opts.Timeout = "60s"
	}
	if opts.ReadTimeout == "" {
		opts.ReadTimeout = "60s"
	}
	if opts.ReadRepeatTimeout == 0 {
		opts.ReadRepeatTimeout = 10 * time.Minute
	}

	dsn := fmt.Sprintf(`%s:%s@tcp(%s)/%s?timeout=%s&readTimeout=%s&charset=utf8&parseTime=True&loc=Local`,
		opts.User, opts.Password, opts.Address, opts.DBName, opts.Timeout, opts.ReadTimeout,
	)
	db, err := gorm.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SingularTable(true)

	if opts.Debug {
		db = db.Debug()
	}

	db = db.Table(opts.TableName)
	return &mysqlStore{
		opts:      opts,
		db:        db,
		payloader: payloader,
	}, nil
}

func (ms *mysqlStore) ScanRows(id int, topic string) (msmq.Rows, error) {
	db := ms.db

	sqlrows, err := db.Where("id > ? && topic = ?", id, topic).Where(
		"status = ? or (status = ? and start_time < ?)",
		StatusUndo, StatusDoing, time.Now().Add(-ms.opts.ReadRepeatTimeout),
	).Rows()
	if err != nil {
		return nil, err
	}

	return &rows{
		store:   ms,
		sqlrows: sqlrows,
	}, nil
}

func (ms *mysqlStore) Insert(topic string, payload interface{}) error {
	p, err := ms.payloader.Encode(topic, payload)
	if err != nil {
		return err
	}

	dao := mqDao{
		Status:     StatusUndo,
		Topic:      topic,
		Payload:    p,
		CreateTime: time.Now(),
	}
	return ms.db.Create(&dao).Error
}

func (ms *mysqlStore) Close() error {
	return ms.db.Close()
}

type message struct {
	store *mysqlStore
	dao   *mqDao
}

func (m *message) ID() int {
	return m.dao.ID
}

func (m *message) Start() error {
	if err := m.dao.Start(m.store.db); err != nil {
		return nil
	}
	return nil
}

func (m *message) Done() error {
	return m.dao.Done(m.store.db)
}

func (m *message) Topic() string {
	return m.dao.Topic
}

func (m *message) Payload() (interface{}, error) {
	return m.store.payloader.Decode(m.Topic(), m.dao.Payload)
}

type rows struct {
	store   *mysqlStore
	sqlrows *sql.Rows
}

func (r *rows) Next() bool {
	return r.sqlrows.Next()
}

func (r *rows) Close() error {
	return r.sqlrows.Close()
}

func (r *rows) Scan() (msmq.Message, error) {
	dao := mqDao{}
	if err := r.store.db.ScanRows(r.sqlrows, &dao); err != nil {
		return nil, err
	}
	return &message{store: r.store, dao: &dao}, nil
}

type MQStatus int

const (
	StatusUndo  MQStatus = 0
	StatusDoing MQStatus = 1
	StatusDone  MQStatus = 2
)

type mqDao struct {
	ID         int        `json:"id"`
	Status     MQStatus   `json:"status"`
	Topic      string     `json:"topic"`
	Payload    []byte     `json:"payload"`
	CreateTime time.Time  `json:"create_time"`
	StartTime  *time.Time `json:"start_time"`
	EndTime    *time.Time `json:"end_time"`
}

func (d *mqDao) Start(db *gorm.DB) error {
	db = db.Where("id = ? && status = ?", d.ID, StatusUndo).Updates(map[string]interface{}{
		"status":     StatusDoing,
		"start_time": time.Now(),
	})
	if db.Error != nil {
		return db.Error
	}
	if db.RowsAffected == 0 {
		return ErrMessageIng
	}
	return nil
}

func (d *mqDao) Done(db *gorm.DB) error {
	db = db.Where("id = ? && status = ?", d.ID, StatusDoing).Updates(map[string]interface{}{
		"status":   StatusDone,
		"end_time": time.Now(),
	})
	if db.Error != nil {
		return db.Error
	}
	if db.RowsAffected == 0 {
		return ErrMessageDone
	}
	return nil
}
