# MSMQ

以MySQL做为持久化存储的消息队列。满足简单，性能不苛刻的场景。

## 使用

```go
mopts := Options{
    Debug:     false,
    User:      "",
    Password:  "",
    Address:   "",
    DBName:    "msmq",
    TableName: "mq",
}
store, err := NewMysqlStore(&mopts, DefaultPayload)
if err != nil {
    return nil, err
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

        // 实际消费消息
        fmt.Printf("%s: %s\n", msg.Topic(), string(p.([]byte)))

        if err := msg.Done(); err != nil {
            t.Error(err)
        }
    }
}()

if err := mq.Produce("test", []byte("test data")); err != nil {
    t.Error(err)
}
```

## next

- 有客户端保留消费偏移量，方便重放。
- 支持Pub/Sub
- 考虑批量插入，批量更新，提高并发时的效率
