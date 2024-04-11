// 延迟队列实现
// 延迟队列就是过一段时间再做某件事
package foo

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/streadway/amqp"
)

const DQ_KEY = "DQ_KEY"

type DelayQueue interface {
	Push(data interface{}, delay int64) error
	Consume(func(a interface{}))
}

// redis zset 实现方式======================================================
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

// redis keyevent 实现方式======================================================
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

// rabbitmq死信队列 实现方式======================================================
type DelayQueue3 struct {
	// 实际使用不能这么用，要考虑重连、断开
	*amqp.Connection
}

func NewDelayQueue3() DelayQueue {
	conn, err := amqp.Dial("amqp://localhost:5672")
	failOnError("建立rabbitmq 链接失败", err)

	ch, err := conn.Channel()
	failOnError("建立通道失败", err)
	defer ch.Close()

	// 正常交换机
	err = ch.ExchangeDeclare(
		"DQ_EXCHANGE",
		amqp.ExchangeDirect,
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError("声明延迟队列交换机失败", err)

	// 正常队列
	q, err := ch.QueueDeclare(
		DQ_KEY,
		false, /*是否持久化到硬盘*/
		false, /*没有消费者使用时自动删除*/
		false, /*排他性*/
		false, /*无需等待*/
		amqp.Table{
			"x-dead-letter-exchange":    "DEAD_EXCHANGE", // 指定死信交换机
			"x-dead-letter-routing-key": "DEAD_ROUTING",  // 指定死信routing-key
		},
	)
	failOnError("声明延迟队列失败", err)

	err = ch.QueueBind(
		q.Name,
		"DQ_ROUTING",
		"DQ_EXCHANGE",
		false,
		nil,
	)
	failOnError("延迟队列和交换机绑定失败", err)

	// 创建死信队列
	deadQ, err := ch.QueueDeclare(
		"DEAD_QUEUE",
		false, /*是否持久化到硬盘*/
		false, /*没有消费者使用时自动删除*/
		false, /*排他性*/
		false, /*无需等待*/
		nil,
	)
	failOnError("创建死信队列失败", err)

	// 创建死信交换机
	err = ch.ExchangeDeclare(
		"DEAD_EXCHANGE",
		amqp.ExchangeDirect,
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError("创建死信队列失败", err)

	// 绑定死信队列和死信交换机
	err = ch.QueueBind(
		deadQ.Name,
		"DEAD_ROUTING",
		"DEAD_EXCHANGE",
		false,
		nil,
	)
	failOnError("死信队列与死信交换机绑定失败", err)

	return &DelayQueue3{conn}
}

func (dq *DelayQueue3) Push(data interface{}, expired int64) error {
	// 推送延迟消息到正常队列即可，过期会被放到死信队列
	ch, err := dq.Channel()
	failOnError("[push]创建channel 失败", err)
	if err != nil {
		return err
	}

	jb, err := json.Marshal(data)
	failOnError("[push]json marshal fail", err)

	expiration := strconv.Itoa(int(expired-time.Now().Unix()) * 1000) //过期时间，毫秒
	err = ch.Publish(
		"DQ_EXCHANGE",
		"DQ_ROUTING",
		false, // true且未绑定与路由密钥匹配的队列时，发布可能无法传递
		false, // 同上
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        jb,
			Expiration:  expiration, // 单位需要毫秒
		},
	)
	failOnError("[push]推送消息失败", err)
	if err != nil {
		return err
	}

	return nil
}

func (dq *DelayQueue3) Consume(cb func(a interface{})) {
	// 消费就只需要消费死信队列即可

	ch, err := dq.Channel()
	checkError("[consume]创建channel 失败", err)

	message, err := ch.Consume(
		"DEAD_QUEUE",
		"",    /*消费者*/
		false, /*自动确认*/
		false, /*排他性*/
		false, /*非本地*/
		false, /*无需等待*/
		nil,   /*其他参数*/
	)
	failOnError("[consume]消费失败", err)

	go func() {
		for m := range message {
			cb(string(m.Body))
			m.Ack(false)
		}
	}()

}

func failOnError(name string, err error) {
	if err != nil {
		panic(name + err.Error())
	}
}

func checkError(name string, err error) {
	if err != nil {
		log.Println(name + err.Error())
	}
}
