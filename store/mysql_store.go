package store

import (
	"database/sql"
	"time"

	"github.com/jinzhu/gorm"
	"github.com/sirupsen/logrus"
	"gitlab.dev.tulong.net/microservice/compose/errors"
	"gitlab.dev.tulong.net/microservice/compose/mqq"
)

// TODO 日志太多了，需要处理一下

type MQHelp struct {
	Id     int64 `json:"id"`
	NextId int64 `json:"next_id"`
}

type MysqlStore struct {
	log         logrus.FieldLogger
	db          *gorm.DB
	taskTimeout time.Duration
	turnTimeout time.Duration
}

func NewMysqlStore(log logrus.FieldLogger, db *gorm.DB, taskTimeout time.Duration, turnTimeout time.Duration) *MysqlStore {
	return &MysqlStore{
		log:         log,
		db:          db.Model(&mqq.MQStore{}),
		taskTimeout: taskTimeout,
		turnTimeout: turnTimeout,
	}
}

func (ms *MysqlStore) Insert(topic string, payload []byte) error {
	err := ms.db.Create(&mqq.MQStore{
		Status:     mqq.StoreRowStatusUndo,
		Topic:      topic,
		Payload:    payload,
		CreateTime: time.Now(),
	}).Error
	if err != nil {
		return errors.Wrap(err).E()
	}
	return nil
}

func (ms *MysqlStore) findLastId() (int64, error) {
	var row mqq.MQStore
	err := ms.db.Select("id").Last(&row).Error
	if err == gorm.ErrRecordNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, errors.Wrap(err).E()
	}
	return row.Id, nil
}

func (ms *MysqlStore) findNextId() (nextId int64, e error) {
	row := ms.db.Model(&MQHelp{}).Where("id = 1").Select("next_id").Row()
	e = row.Scan(&nextId)
	if e == sql.ErrNoRows {
		nextId = 1
		e = nil

		db := ms.db.Create(&MQHelp{
			Id:     1,
			NextId: 1,
		})
		e = db.Error
		e = errors.Wrap(e).E()
	}

	return
}

func (ms *MysqlStore) updateNextId(nextId int64) error {
	db := ms.db.Model(&MQHelp{}).Where("id = 1").Update("next_id", nextId)
	if err := db.Error; err != nil {
		return errors.Wrap(err).E()
	}
	if db.RowsAffected != 1 {
		return errors.New("no affected").E()
	}
	return nil
}

func (ms *MysqlStore) findNewNextId(nextId int64) (int64, error) {
	db := ms.db

	nextRows, err := db.
		Select("id,status").
		Where("id >= ?", nextId).
		Rows()
	if err != nil {
		return 0, errors.Wrap(err).E()
	}
	defer nextRows.Close()

	newNextId := nextId
	for {
		ok := nextRows.Next()
		if !ok {
			break
		}

		var id int64
		var status mqq.StoreRowStatus
		if err := nextRows.Scan(&id, &status); err != nil {
			return 0, errors.Wrap(err).E()
		}

		if status != mqq.StoreRowStatusDone {
			break
		}

		newNextId = id + 1
	}
	return newNextId, nil
}

// pool
func (ms *MysqlStore) FetchNext(done <-chan struct{}) (mqq.NextFunc, error) {
	var (
		err    error
		lastId int64
		nextId int64
		rows   *sql.Rows
	)
	db := ms.db

	lastId, err = ms.findLastId()
	if err != nil {
		return nil, errors.Wrap(err).E()
	}

	updateRows := func() error {
		time.Sleep(ms.turnTimeout)

		{ // 动态获取 nextId
			newNextId, err := ms.findNextId()
			if err != nil {
				return errors.Wrap(err).E()
			}
			nextId = newNextId
		}

		{ // 尝试更新 nextId
			newNextId, err := ms.findNewNextId(nextId)
			if err != nil {
				ms.log.Error("findNewNextId err:", err)
			} else if newNextId > nextId {
				nextId = newNextId

				if err := ms.updateNextId(nextId); err != nil {
					ms.log.Error("updateNextId err:", err)
				}
			}
		}

		for nextId > lastId { // 等待有新数据产生
			select {
			case <-done:
				return errors.New("close").E()
			default:
			}

			time.Sleep(ms.turnTimeout)

			newLastId, err := ms.findLastId()
			if err != nil {
				return errors.Wrap(err).E()
			}
			lastId = newLastId
		}

		newRows, err := db.
			Select("id,status,topic,version").
			Where("id >= ?", nextId).
			Where("status = ? or status = ? and start_time < ?",
				mqq.StoreRowStatusUndo,
				mqq.StoreRowStatusDoing,
				time.Now().Add(-ms.taskTimeout).UnixNano(),
			).
			Rows()
		if err != nil {
			return errors.Wrap(err).E()
		}

		rows = newRows
		return nil
	}

	return mqq.NextFunc(func() (mqq.Task, error) {
		if rows == nil {
			err := updateRows()
			if err != nil {
				return mqq.Task{}, errors.Wrap(err).E()
			}
		}

		ok := rows.Next()
		if !ok {
			err := rows.Err()

			rows.Close()
			rows = nil

			if err != nil {
				return mqq.Task{}, errors.Wrap(err).E()
			}

			return mqq.Task{}, errors.New("the turn is over").E()
		}

		var row mqq.MQStore
		if err := ms.db.ScanRows(rows, &row); err != nil {
			return mqq.Task{}, errors.Wrap(err).E()
		}

		return mqq.Task{
			Id:      row.Id,
			Topic:   row.Topic,
			Status:  row.Status,
			Version: row.Version,
		}, nil
	}), nil
}

func (ms *MysqlStore) Select(id int64) (*mqq.MQStore, error) {
	var row mqq.MQStore
	err := ms.db.Where("id = ?", id).Last(&row).Error
	if err != nil {
		return nil, errors.Wrap(err).E()
	}
	return &row, nil
}

// 锁定状态 和 开始执行时间
func (ms *MysqlStore) Start(task mqq.Task) error {
	db := ms.db.Where("id = ?", task.Id)
	if task.Status == mqq.StoreRowStatusUndo {
		db = db.Where("status = ?", task.Status)
	} else if task.Status == mqq.StoreRowStatusDoing {
		db = db.Where("status = ? && version = ?", task.Status, task.Version)
	} else {
		return errors.New("err args").E()
	}

	db = db.Updates(map[string]interface{}{
		"status":     mqq.StoreRowStatusDoing,
		"start_time": time.Now().UnixNano(),
		"version":    task.Version + 1,
	})

	if err := db.Error; err != nil {
		return errors.Wrap(err).E()
	}
	if db.RowsAffected != 1 {
		return errors.New("no affected").E()
	}

	return nil
}

// 如果存在并行时，更新失败（以前一状态和开始时间作为条件）。记录日志。
func (ms *MysqlStore) Done(task mqq.Task) error {
	db := ms.db
	db = db.Where("id = ?", task.Id)
	if task.Status == mqq.StoreRowStatusDoing {
		db = db.Where("status = ? && version = ?", task.Status, task.Version)
	} else {
		return errors.New("err status").E()
	}

	db = db.Updates(map[string]interface{}{
		"status":   mqq.StoreRowStatusDone,
		"end_time": time.Now().UnixNano(),
		"version":  task.Version + 1,
	})
	if err := db.Error; err != nil {
		return errors.Wrap(err).E()
	}

	if db.RowsAffected != 1 {
		return errors.New("no affected").E()
	}

	return nil
}
