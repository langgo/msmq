package store

import (
	"fmt"
	"testing"

	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/sirupsen/logrus"
)

func connect() *gorm.DB {
	dsn := fmt.Sprintf(`%s:%s@tcp(%s)/%s?timeout=%s&readTimeout=%s&charset=utf8&parseTime=True&loc=Local`,
		"cloud", "212147a1c567bb9e", "mysql.yun3.com:3306", "compose", "60s", "60s",
	)
	db, err := gorm.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	db.SingularTable(true)
	return db
}

func TestMysqlStore_findNextId(t *testing.T) {
	db := connect()
	defer db.Close()

	log := logrus.StandardLogger()
	mysqlStore := NewMysqlStore(log, db, 60*time.Second, 500*time.Millisecond)

	nextId, err := mysqlStore.findNextId()
	if err != nil {
		t.Error(err)
	}
	t.Logf("nextId: %d", nextId)
}

func TestMysqlStore_updateNextId(t *testing.T) {
	db := connect()
	defer db.Close()

	log := logrus.StandardLogger()
	mysqlStore := NewMysqlStore(log, db, 60*time.Second, 500*time.Millisecond)

	err := mysqlStore.updateNextId(10)
	if err != nil {
		t.Error(err)
	}
}

func TestMysqlStore_findLastId(t *testing.T) {
	db := connect()
	defer db.Close()

	log := logrus.StandardLogger()
	mysqlStore := NewMysqlStore(log, db, 60*time.Second, 500*time.Millisecond)

	lastId, err := mysqlStore.findLastId()
	if err != nil {
		t.Error(err)
	}
	t.Logf("lastId: %d", lastId)
}
