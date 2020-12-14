package foo

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// 语句qps测试
// 只能多个语句之前定性的说明，定量没有意义
// param
// 		dbname:数据库名
// 		testname:本次测试的名字
// 		qs:语句
// 		t:测试时间
func QPS(dbname, testname, qs string, t int) {
	db, err := sql.Open("mysql", "root:123456@tcp(localhost:3306)/"+dbname+"?charset=utf8")
	if err != nil {
		log.Printf("db error:%v", err)
	}
	var wg sync.WaitGroup

	// 测试时间
	// t := flag.Int("t", 10, "-t 10")
	// name := flag.String("name", "query", "-name queryName")
	// flag.Parse()

	// 预热时间
	timer := time.NewTimer(2 * time.Second)
	preparech := make(chan struct{})
	go func() {
		defer func() {
			preparech <- struct{}{}
		}()
		for {
			select {
			case <-timer.C:
				return
			default:
				_, err := db.Exec(qs)
				if err != nil {
					log.Printf("happen a error:%v", err)
				}
			}
		}
	}()

	<-preparech
	wg.Add(1)
	var Result struct {
		Mint      float64 // 最小时间
		Maxt      float64 // 最大时间
		Meant     float64 // 平均时间
		Test      int     // 测试次数
		SUCCESS   int     // 成功
		TotalTime float64
		TestTime  float64
	}
	Result.Mint = 1000000

	timer = time.NewTimer(time.Duration(t) * time.Second)
	if err != nil {
		log.Printf("db error:%v", err)
		return
	}
	teststart := time.Now()
	go func() {
		defer wg.Done()
		for {
			select {
			// 时间到
			case <-timer.C:
				Result.TestTime = float64(time.Since(teststart).Milliseconds())
				return
			// 查询
			default:
				start := time.Now()

				_, err := db.Exec(qs)
				if err != nil {
					log.Printf("happen a error:%v", err)
				} else {
					Result.SUCCESS++
				}
				testTime := float64(time.Since(start).Milliseconds())
				Result.TotalTime += testTime
				if testTime > Result.Maxt {
					Result.Maxt = testTime
				}
				if testTime < Result.Mint {
					Result.Mint = testTime
				}

				Result.Test++

			}
		}
	}()

	wg.Wait()
	Result.Meant = Result.TotalTime / float64(Result.Test)
	fmt.Printf("--------%s--------\n", testname)
	fmt.Printf("测试次数：%d\n", Result.Test)
	fmt.Printf("测试时间：%fms\n", Result.TestTime)
	fmt.Printf("成功：%d\n", Result.SUCCESS)
	fmt.Printf("预热时间：%ds\n", 2)
	fmt.Printf("最大查询时间：%fms\n", Result.Maxt)
	fmt.Printf("最小查询时间：%fms\n", Result.Mint)
	fmt.Printf("平均查询时间：%fms\n", Result.Meant)
	fmt.Printf("QPS：%f\n", float64(Result.Test)/float64(t))
}
