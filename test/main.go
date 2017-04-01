package main

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"go.uber.org/ratelimit"

	"github.com/smallnest/fasttimerwheel"
)

//测试数据总数
var dataCount = 1000000

//每秒更新频率
var qps = 20000

//数据
var allData = make([]*time.Time, dataCount)

// 检查的时间间隔
var tick = time.Second

// 槽数，最大的过期时间为 tick * slotCount
var slotCount = 600

// 测试时间
var testTime = 20 * time.Minute

var r1 = rand.New(rand.NewSource(time.Now().UnixNano()))

var failedCount int64

func main() {
	//数据过期检查函数
	f := func(start time.Time, slot fasttimerwheel.Slot) {
		begin := time.Now()
		interval := time.Now().UnixNano()/1e9 - start.UnixNano()/1e9
		if interval > int64(tick) {
			fmt.Printf("处理时间间隔超长: %d s\n", interval)
		}

		// 监测过期数据是否真的过期，或者本来早就该过期
		for k := range slot.Data {
			data := k.(*time.Time)
			// 如果时间轮处理慢了，导致当前的槽中的数据过期时间大于滴答时间，计数
			if (data.UnixNano()-start.UnixNano())/1e9 > tick.Nanoseconds()/1e9 {
				atomic.AddInt64(&failedCount, 1)
				fmt.Printf("超时: %d ms\n", (data.UnixNano()-start.UnixNano()-tick.Nanoseconds())/1e6)
			}
		}

		took := (time.Now().UnixNano() - begin.UnixNano()) / 1e6

		if took > tick.Nanoseconds()/1e6 {
			fmt.Printf("迭代过期数据超时: %d ms. 单个槽中的数据过多: %d\n", took, len(slot.Data))
		}
	}

	tw := fasttimerwheel.New(tick, slotCount, 0, f)
	tw.Start()

	// 初始化数据,随机产生待过期数据
	d := int64(tick) * int64(slotCount)
	for i := 0; i < dataCount; i++ {
		expiredTime := time.Now().Add(time.Duration(r1.Int63n(d)))
		allData = append(allData, &expiredTime)
		tw.ScheduleAt(expiredTime, &expiredTime)
	}

	// 模拟heartbeat
	// heartbeat时，需要调整数据的过期时间，这里通过从时间轮中移除数据，再重新Schedule的方式实现
	for i := 0; i < 1000; i++ {
		go func() {
			//https://github.com/golang/go/issues/3611
			var r1 = rand.New(rand.NewSource(time.Now().UnixNano()))
			var r2 = rand.New(rand.NewSource(time.Now().UnixNano()))
			rl := ratelimit.New(qps / 1000)
			for {
				rl.Take()
				// 找个随机过期数据
				index := r2.Intn(dataCount)
				data := allData[index]

				// 从时间轮中移除
				tw.Remove(data)

				// 更新过期时间，重新加入时间轮
				expiredTime := time.Now().Add(time.Duration(r1.Int63n(d)))
				allData[index] = &expiredTime
				tw.ScheduleAt(expiredTime, &expiredTime)
			}
		}()
	}

	time.Sleep(testTime)
	tw.Stop()

	fmt.Printf("监测到异常的数据量: %d\n", atomic.LoadInt64(&failedCount))
}
