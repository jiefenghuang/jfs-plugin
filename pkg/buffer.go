package pkg

import (
	"math/bits"
	"sort"
	"sync"
)

var (
	DefaultSvrCapList = []int{
		headerLen,
		4 + 1<<20, // for batch
		512 + 4<<20,
	}
	DefaultCliCapList = []int{
		headerLen,
		headerLen + 2 + 64,
		headerLen + 2 + 128,
	}
)

type bufferPool struct {
	specPools []*sync.Pool
	specCaps  []int
	minCaps   []int
	maxCaps   []int
	expPools  [23]*sync.Pool // max 4MiB
}

func newBufferPool(capList []int) *bufferPool {
	sort.Ints(capList)
	pool := &bufferPool{specCaps: capList}

	for _, c := range capList {
		capacity := c
		pool.maxCaps = append(pool.maxCaps, capacity*115/100)
		pool.minCaps = append(pool.minCaps, capacity*90/100)
		pool.specPools = append(pool.specPools, &sync.Pool{
			New: func() any {
				b := make([]byte, capacity)
				return &b
			},
		})
	}
	for i := range pool.expPools {
		pool.expPools[i] = &sync.Pool{
			New: func() any {
				b := make([]byte, 1<<i)
				return &b
			},
		}
	}
	return pool
}

func (p *bufferPool) Get(targetCap int) []byte {
	if targetCap&(targetCap-1) != 0 {
		for i, c := range p.specCaps {
			if c >= targetCap {
				if targetCap >= p.minCaps[i] {
					return (*p.specPools[i].Get().(*[]byte))[:targetCap]
				}
				break
			}
		}
	}

	idx := bits.Len32(uint32(targetCap - 1))
	if idx < len(p.expPools) {
		return (*(p.expPools[idx].Get().(*[]byte)))[:targetCap]
	}
	return make([]byte, targetCap)
}

func (p *bufferPool) Put(buff []byte) {
	if buff == nil {
		return
	}
	targetCap := cap(buff)
	if targetCap&(targetCap-1) == 0 {
		idx := bits.Len32(uint32(targetCap - 1))
		if idx < len(p.expPools) {
			p.expPools[idx].Put(&buff)
			return
		}
	} else {
		for i := len(p.specCaps) - 1; i >= 0; i-- {
			if targetCap >= p.specCaps[i] {
				if targetCap <= p.maxCaps[i] {
					p.specPools[i].Put(&buff)
				}
				return
			}
		}
	}
}
