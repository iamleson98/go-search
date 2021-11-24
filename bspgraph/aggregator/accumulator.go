package aggregator

import (
	"math"
	"sync/atomic"
	"unsafe"
)

type Float64Accumulator struct {
	prevSum float64
	curSum  float64
}

func (a *Float64Accumulator) Type() string {
	return "Float64Accumulator"
}

func (a *Float64Accumulator) Get() interface{} {
	return loadFloat64(&a.curSum)
}

func loadFloat64(v *float64) float64 {
	return math.Float64frombits(atomic.LoadUint64((*uint64)(unsafe.Pointer(v))))
}

func (a *Float64Accumulator) Set(v interface{}) {
	for v64 := v.(float64); ; {
		oldCur := loadFloat64(&a.curSum)
		oldPrev := loadFloat64(&a.prevSum)
		swappedCur := atomic.CompareAndSwapUint64(
			(*uint64)(unsafe.Pointer(&a.curSum)),
			math.Float64bits(oldCur),
			math.Float64bits(v64),
		)
		swappedPrev := atomic.CompareAndSwapUint64(
			(*uint64)(unsafe.Pointer(&a.prevSum)),
			math.Float64bits(oldPrev),
			math.Float64bits(v64),
		)
		if swappedCur && swappedPrev {
			return
		}
	}
}

func (a *Float64Accumulator) Aggregate(v interface{}) {
	for v64 := v.(float64); ; {
		oldV := loadFloat64(&a.curSum)
		newV := oldV + v64
		if atomic.CompareAndSwapUint64(
			(*uint64)(unsafe.Pointer(&a.curSum)),
			math.Float64bits(oldV),
			math.Float64bits(newV),
		) {
			return
		}
	}
}

func (a *Float64Accumulator) Delta() interface{} {
	for {
		curSUm := loadFloat64(&a.curSum)
		prevSum := loadFloat64(&a.prevSum)
		if atomic.CompareAndSwapUint64(
			(*uint64)(unsafe.Pointer(&a.prevSum)),
			math.Float64bits(prevSum),
			math.Float64bits(curSUm),
		) {
			return curSUm - prevSum
		}
	}
}

type IntAccumulator struct {
	prevSum int64
	curSum  int64
}

func (a *IntAccumulator) Type() string {
	return "IntAccumulator"
}

func (a *IntAccumulator) Get() interface{} {
	return int(atomic.LoadInt64(&a.curSum))
}

func (a *IntAccumulator) Set(v interface{}) {
	for v64 := int64(v.(int)); ; {
		oldCur := a.curSum
		oldPrev := a.prevSum
		swappedCur := atomic.CompareAndSwapInt64(&a.curSum, oldCur, v64)
		swappedPrev := atomic.CompareAndSwapInt64(&a.prevSum, oldPrev, v64)
		if swappedCur && swappedPrev {
			return
		}
	}
}

func (a *IntAccumulator) Aggregate(v interface{}) {
	_ = atomic.AddInt64(&a.curSum, int64(v.(int)))
}

func (a *IntAccumulator) Delta() interface{} {
	for {
		curSum := atomic.LoadInt64(&a.curSum)
		prevSum := atomic.LoadInt64(&a.prevSum)
		if atomic.CompareAndSwapInt64(&a.prevSum, prevSum, curSum) {
			return int(curSum - prevSum)
		}
	}
}
