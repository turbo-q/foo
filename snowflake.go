package foo

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var worker *snowflakeWorker

const (
	nodeIDBits   = uint64(10) // 10bit 工作机器ID中的节点id
	sequenceBits = uint64(12)

	maxSequence = 1<<sequenceBits - 1 // 4095

	timeLeft = uint8(22) // 时间戳左移位数
	nodeLeft = uint8(12) // 机器号左移位数

)

type snowflakeWorker struct {
	mu        sync.Mutex
	NodeID    int64 // 机器号
	LastStamp int64 // 上一次的时间戳
	Sequence  int64 // 当前毫秒的序列数
}

func (s *snowflakeWorker) NextID() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.nextID()
}

// 获取毫秒时间戳
func (s *snowflakeWorker) getMilliSeconds() int64 {
	return time.Now().UnixNano() / 1e6
}

func (s *snowflakeWorker) nextID() (int64, error) {
	timeStamp := s.getMilliSeconds()

	// 当前毫秒则增加sequence
	if timeStamp == s.LastStamp {
		// 如果时间回拨则等待
		s.Sequence = (s.Sequence + 1) & maxSequence
		// 为0代表当前时间也完毕需等待
		if s.Sequence == 0 {
			for timeStamp <= s.LastStamp {
				timeStamp = s.getMilliSeconds()
			}
		}
	} else if timeStamp > s.LastStamp {
		s.Sequence = 0
	} else {
		// 时间回拨大于1ms
		// TODO考虑直接报错
		return 0, errors.New("雪花发号器时间回拨大于1ms")
	}

	s.LastStamp = timeStamp

	id := (timeStamp << timeLeft) | (s.NodeID << nodeLeft) | (s.Sequence)

	return id, nil
}

func main() {
	worker = &snowflakeWorker{
		NodeID: 0,
	}
	m := make(map[int64]struct{}, 10000)

	for i := 0; i < 10000; i++ {
		id, err := worker.NextID()
		fmt.Println(id)
		if err != nil {
			fmt.Println(err)
			return
		}
		_, ok := m[id]
		if ok {
			fmt.Println("重复")
			return
		}
		m[id] = struct{}{}

	}

}
