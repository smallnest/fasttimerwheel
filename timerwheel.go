package fasttimerwheel

import (
	"errors"
	"sync"
	"time"
)

var (
	// ErrScheduleInPast schedule的时间已经过期
	ErrScheduleInPast = errors.New("schedule a data in past")
	// ErrSchedulePastHighThreshold 超过了最大的schedule时间
	ErrSchedulePastHighThreshold = errors.New("schedule a data that exceeds high threshold")
	// ErrSlotIsFull 时间槽中scheduled数据已达到最大数量
	ErrSlotIsFull = errors.New("the slot is full for scheduling")
)

// TimerWheel 时间轮.
type TimerWheel struct {
	tickTime         time.Duration
	slotCount        int
	ticker           *time.Ticker
	CurrentStartTime time.Time
	CurrentIndex     int
	slots            []Slot
	maxSlotSize      int
	idSlotMap        *SyncIntMap
	expiredDataFunc  ExpiredDataFunc
}

// Slot 时间槽.
type Slot *SyncBoolMap

// ExpiredDataFunc 处理过期数据的函数.
// start 是过期的时间槽的起始时间.
// slot 是过期的时间槽.
type ExpiredDataFunc func(start time.Time, slot Slot)

// New 创建一个时间轮.
// tickTime 是时间轮检查的时间间隔，也是过期数据超过过期时间还未处理的最大时间间隔.
// slotCount 是时间轮上的槽数.
// maxSlotSize 单个槽中可存放的数据数， 0 代表不做限制，可以任意存放.
// expiredDataFunc 数据过期后要要调用的函数.
func New(tickTime time.Duration, slotCount int, maxSlotSize int, expiredDataFunc ExpiredDataFunc) *TimerWheel {
	tw := &TimerWheel{
		tickTime:        tickTime,
		slotCount:       slotCount,
		slots:           make([]Slot, slotCount),
		maxSlotSize:     maxSlotSize,
		idSlotMap:       &SyncIntMap{Data: make(map[interface{}]int)},
		expiredDataFunc: expiredDataFunc,
	}

	for i := 0; i < slotCount; i++ {
		tw.slots[i] = Slot(&SyncBoolMap{Data: make(map[interface{}]bool)})
	}
	return tw
}

// Start 启动
func (tw *TimerWheel) Start() {
	tw.ticker = time.NewTicker(tw.tickTime)
	tw.CurrentStartTime = time.Now()

	go func() {
		for range tw.ticker.C {
			tw.step()
		}
	}()
}

// Stop 停止时间轮的处理.
func (tw *TimerWheel) Stop() {
	tw.ticker.Stop()
}

func (tw *TimerWheel) step() {
	// 单个ticker中调用，索引的步进不会有并发的问题
	start := tw.CurrentStartTime

	expiredIndex := tw.CurrentIndex
	tw.CurrentIndex = tw.CurrentIndex + 1
	if tw.CurrentIndex >= tw.slotCount {
		tw.CurrentIndex = 0
	}
	tw.CurrentStartTime = time.Now()

	expiredSlot := tw.slots[expiredIndex]
	expiredSlot.Lock()
	tw.slots[expiredIndex] = Slot(&SyncBoolMap{Data: make(map[interface{}]bool)})
	expiredSlot.Unlock()

	tw.idSlotMap.Lock()
	for k := range expiredSlot.Data {
		delete(tw.idSlotMap.Data, k)
	}
	tw.idSlotMap.Unlock()

	// 此时应该没有并发访问expiredSlot的问题
	tw.expiredDataFunc(start, expiredSlot)
}

// ScheduleAt schedule一个数据，将在at时间过期.
// TimerWheel 会将它放入一个合适的时间槽中.
func (tw *TimerWheel) ScheduleAt(at time.Time, data interface{}) error {
	if at.Before(tw.CurrentStartTime) {
		return ErrScheduleInPast
	}

	d := at.UnixNano() - tw.CurrentStartTime.UnixNano()
	steps := int(d / int64(tw.tickTime))
	if steps >= tw.slotCount {
		return ErrSchedulePastHighThreshold
	}

	index := (tw.CurrentIndex + steps) % tw.slotCount
	slot := tw.slots[index]

	slot.Lock()
	if tw.maxSlotSize > 0 && len(slot.Data) > tw.maxSlotSize {
		slot.Unlock()
		return ErrSlotIsFull
	}
	slot.Data[data] = true
	slot.Unlock()

	tw.idSlotMap.Lock()
	tw.idSlotMap.Data[data] = index
	tw.idSlotMap.Unlock()

	return nil
}

// ScheduleIn schedule 一个数据在时间in后过期.
func (tw *TimerWheel) ScheduleIn(in time.Duration, data interface{}) error {
	at := time.Now().Add(in)
	return tw.ScheduleAt(at, data)
}

// Remove 移除一个schedule的数据.
func (tw *TimerWheel) Remove(data interface{}) {
	tw.idSlotMap.Lock()
	index, ok := tw.idSlotMap.Data[data]
	tw.idSlotMap.Unlock()
	if !ok {
		return
	}

	slot := tw.slots[index]
	slot.Lock()
	delete(slot.Data, index)
	slot.Unlock()
}

// SyncBoolMap 线程安全的bool map.
type SyncBoolMap struct {
	Data map[interface{}]bool
	sync.RWMutex
}

// SyncIntMap 线程安全的int map.
type SyncIntMap struct {
	Data map[interface{}]int
	sync.RWMutex
}
