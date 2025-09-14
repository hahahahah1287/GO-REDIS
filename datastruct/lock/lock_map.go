package lock

import (
	"sort"
	"sync"
)

type Locks struct {
	table []*sync.RWMutex
}

const (
	prime32 = uint32(16777619)
)

func Make(size int) *Locks {
	table := make([]*sync.RWMutex, size)
	for i := 0; i < size; i++ {
		table[i] = &sync.RWMutex{}
	}
	return &Locks{table: table}
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

func (locks *Locks) spread(hashCode uint32) uint32 {
	if locks == nil {
		panic("dict is nil")
	}
	tableSize := uint32(len(locks.table))
	return (tableSize - 1) & hashCode
}

func (locks *Locks) Lock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.Lock()
}

func (locks *Locks) UnLock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.Unlock()
}

func (locks *Locks) Locks(keys ...string) {
	indices := locks.toLockIndices(keys, false)
	for _, index := range indices {
		mu := locks.table[index]
		mu.Lock()
	}
}
func (locks *Locks) UnLocks(keys ...string) {
	indices := locks.toLockIndices(keys, true)
	for _, index := range indices {
		mu := locks.table[index]
		mu.Unlock()
	}
}


// 在锁定多个key时需要注意，若协程A持有键a的锁试图获得键b的锁，此时协程B持有键b的锁试图获得键a的锁则会形成死锁。
// 解决方法是所有协程都按照相同顺序加锁，若两个协程都想获得键a和键b的锁，那么必须先获取键a的锁后获取键b的锁，这样就可以避免循环等待。

func (locks *Locks) toLockIndices(keys []string, reverse bool) []uint32 {
	indexMap := make(map[uint32]bool)
	for _, key := range keys {
		index := locks.spread(fnv32(key))
		indexMap[index] = true
	}
	indices := make([]uint32, 0, len(indexMap))
	// 如果 reverse 为 false，则 indices 按升序排列。
	// 如果 reverse 为 true，则 indices 按降序排列。
	// reverse == false：通常用于需要按自然顺序访问锁的场景。
	// reverse == true：用于需要逆序操作的场景，例如释放锁时按照与加锁相反的顺序解锁以避免死锁。
	sort.Slice(indices, func(i, j int) bool {
		if !reverse {
			return indices[i] < indices[j]
		} else {
			return indices[i] > indices[j]
		}
	})
	return indices
}

func (locks *Locks) RWLock(readKeys []string, writeKeys []string) {
	keys := append(readKeys, writeKeys...)
	indices := locks.toLockIndices(keys, false)
	writeIndices := locks.toLockIndices(writeKeys, true)
	// 使用 map 来记录哪些索引对应的是写锁。
	// struct{} 是一种轻量级的占位符，节省内存空间。
	writeIndexSet := make(map[uint32]interface{})
	for _, index := range writeIndices {
		writeIndexSet[index] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := locks.table[index]
		if w {
			mu.Lock()
		} else {
			mu.RLock()
		}
	}
}

func (locks *Locks) RWUnLock(readKeys []string, writeKeys []string) {
	keys := append(readKeys, writeKeys...)
	indices := locks.toLockIndices(keys, true)
	writeIndices := locks.toLockIndices(writeKeys, true)
	writeIndexSet := make(map[uint32]interface{})
	for _,index := range writeIndices {
		writeIndexSet[index] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := locks.table[index]
		if w {
			mu.Unlock()
		} else {
			mu.RUnlock()
		}
	}
}
