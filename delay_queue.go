// 延迟队列实现
// 延迟队列就是过一段时间再做某件事
package foo

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const DQ_KEY = "DQ_KEY"

type DelayQueue interface {
	Push(data interface{}, delay int64) error
	Consume(func(a interface{}))
}

// redis zset 实现方式
type DelayQueue1 struct {
	*redis.Client
}

func NewDelayQueue1() DelayQueue {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // 没有密码，默认值
		DB:       0,  // 默认DB 0
	})
	return &DelayQueue1{rdb}
}

func (dq *DelayQueue1) Push(data interface{}, delay int64) error {
	return dq.Client.ZAdd(context.TODO(), DQ_KEY, redis.Z{Member: data, Score: float64(delay)}).Err()
}

func (dq *DelayQueue1) Consume(cb func(data interface{})) {
	go func() {
		for {
			data, err := dq.Client.ZRangeWithScores(context.TODO(), DQ_KEY, 0, 0).Result()
			if len(data) > 0 {
				first := data[0]
				now := time.Now().Unix()
				// 到时间了
				if now >= int64(first.Score) {
					if err := dq.Client.ZRem(context.TODO(), DQ_KEY, first.Member).Err(); err != nil {
						log.Println(err)
						continue
					}
					cb(first.Member)
					continue // 马上获取下一个
				}
			} else {
				time.Sleep(time.Second) // 无数据
			}
			if err != nil {
				log.Println(err)
			}

			time.Sleep(time.Millisecond * 10)
		}
	}()
}

// redis keyevent 实现方式
// 缺陷，不能获取数据，只知道哪个过期了
type DelayQueue2 struct {
	*redis.Client
}

func NewDelayQueue2() DelayQueue {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // 没有密码，默认值
		DB:       0,  // 默认DB 0
	})
	return &DelayQueue2{rdb}
}

func (dq *DelayQueue2) Push(data interface{}, delay int64) error {

	return dq.Client.Set(context.TODO(), DQ_KEY+uuid.New().String(), data, time.Second*time.Duration(delay-time.Now().Unix())).Err()
}

func (dq *DelayQueue2) Consume(cb func(a interface{})) {
	ctx := context.TODO()
	// 订阅键过期事件
	pubsub := dq.Client.Subscribe(ctx, "__keyevent@0__:expired")

	// 检查是否发生错误
	_, err := pubsub.Receive(ctx)
	if err != nil {
		fmt.Println("Error subscribing:", err)
		return
	}

	// 监听过期事件
	ch := pubsub.Channel()

	// 处理过期事件
	go func() {
		for msg := range ch {
			if strings.Contains(msg.Payload, DQ_KEY) {
				cb(msg)
			}
		}
	}()

}
