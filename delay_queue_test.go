// 延迟队列实现
// 延迟队列就是过一段时间再做某件事
package foo

import (
	"log"
	"testing"
	"time"
)

func TestNewDelayQueue1(t *testing.T) {
	dq := NewDelayQueue1()
	dq.Push("过5s钟收到的数据", time.Now().Add(time.Second*5).Unix())
	dq.Push("过10s钟收到的数据", time.Now().Add(time.Second*10).Unix())
	dq.Push("过15s钟收到的数据", time.Now().Add(time.Second*15).Unix())

	log.Println("start")
	dq.Consume(func(a interface{}) {
		log.Println("延迟队列消费====", a)
	})

	t.FailNow()
}

func TestNewDelayQueue2(t *testing.T) {
	dq := NewDelayQueue2()
	dq.Push("过5s钟收到的数据", time.Now().Add(time.Second*5).Unix())
	dq.Push("过10s钟收到的数据", time.Now().Add(time.Second*10).Unix())
	dq.Push("过15s钟收到的数据", time.Now().Add(time.Second*15).Unix())

	log.Println("start")
	dq.Consume(func(a interface{}) {
		log.Println("延迟队列消费====", a)
	})
	time.Sleep(time.Second * 30)

	t.FailNow()
}

func TestNewDelayQueue3(t *testing.T) {
	dq := NewDelayQueue3()
	dq.Push("过5s钟收到的数据", time.Now().Add(time.Second*5).Unix())
	dq.Push("过10s钟收到的数据", time.Now().Add(time.Second*10).Unix())
	dq.Push("过15s钟收到的数据", time.Now().Add(time.Second*15).Unix())

	log.Println("start")
	dq.Consume(func(a interface{}) {
		log.Println("延迟队列消费====", a)
	})
	time.Sleep(time.Second * 30)

	t.FailNow()
}
