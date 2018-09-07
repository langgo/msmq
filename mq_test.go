package msmq_test

import (
	"fmt"
	"testing"

	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/sirupsen/logrus"
	"gitlab.dev.tulong.net/microservice/compose/mqq"
	"gitlab.dev.tulong.net/microservice/compose/mqq/gopool"
	store2 "gitlab.dev.tulong.net/microservice/compose/mqq/store"
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

func TestNewServer(t *testing.T) {
	db := connect()
	defer db.Close()

	log := logrus.StandardLogger()

	store := store2.NewMysqlStore(log, db, 60*time.Second, 500*time.Millisecond)
	pool := gopool.NewPool(100)
	server := mqq.NewServer(&mqq.Options{
		Debug: true,
	}, log, store, pool)

	// TODO Register 可以不用加锁，保证顺序
	//server.Register("topic a", func(payload []byte) {
	//	fmt.Println("topic: topic a", string(payload))
	//})
	//server.Register("a", func(payload []byte) {
	//	fmt.Println("topic: a", string(payload))
	//})

	err := server.Run()
	if err != nil {
		panic(err)
	}

	//err = server.Produce("a", nil)
	//if err != nil {
	//	panic(err)
	//}

	time.Sleep(10 * time.Second)
	server.Close()
}
