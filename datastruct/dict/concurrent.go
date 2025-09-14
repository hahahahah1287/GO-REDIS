package dict

import (
	"log/slog"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhangming/go-redis/lib/wildcard"
)

type Shard struct {
	m     map[string]interface{}
	mutex sync.RWMutex
}

type ConcurrentDict struct {
	table      []*Shard
	count      int32
	shardCount int
}

const prime32 = uint32(16777619)

// 哈希函数常用于哈希表中，将键转换为哈希值，以便快速查找。
func fnv32(key string) uint32 {
	hash := uint32(2166136261) // 初始哈希值（FNV offset basis）
	for i := 0; i < len(key); i++ {
		hash *= prime32        // 每次迭代乘以 FNV prime
		hash ^= uint32(key[i]) // 异或当前字符的 ASCII 值
	}
	return hash
}

func (dict *ConcurrentDict) addCount() {
	atomic.AddInt32(&dict.count, 1)
}

func (dict *ConcurrentDict) decreaseCount() {
	atomic.AddInt32(&dict.count, -1)
}

// 将任意整数转换为不小于它的最小 2 的幂次值
func computeCapacity(param int) int {
	if param <= 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	}
	return n + 1
}

// 创建具有给定分片数的并发字典
func MakeConcurrent(shardCount int) *ConcurrentDict {
	if shardCount == 1 {
		table := []*Shard{
			{
				m: make(map[string]interface{}),
			},
		}
		return &ConcurrentDict{
			count:      0,
			table:      table,
			shardCount: shardCount,
		}
	}
	shardCount = computeCapacity(shardCount)
	table := make([]*Shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &Shard{
			m: make(map[string]interface{}),
		}
	}
	d := &ConcurrentDict{
		count:      0,
		table:      table,
		shardCount: shardCount,
	}
	return d
}

func MakeNewConcurrentDict(capacity int) *ConcurrentDict {
	shardCount := computeCapacity(capacity)
	table := make([]*Shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &Shard{m: make(map[string]interface{})}
	}

	d := &ConcurrentDict{table: table, count: 0}
	return d
}

// 将哈希码均匀地映射到 ConcurrentDict 的各个分片上
func (dict *ConcurrentDict) spread(key string) uint32 {
	if dict == nil {
		panic("dict is nil")
	}
	if len(dict.table) == 1 {
		return 0
	}
	hashCode := fnv32(key)
	tableSize := uint32(len(dict.table))
	return (tableSize - 1) & hashCode
}

func (dict *ConcurrentDict) Len() int {
	if dict == nil {
		panic("dict is nil")
	}
	// 这里是为了保证并发情况下可以原子性读取
	// go的所有操作都是不保证原子性的
	return int(atomic.LoadInt32(&dict.count))
}

func (dict *ConcurrentDict) getShard(index uint32) *Shard {
	if dict == nil {
		panic("dict is nil")
	}
	return dict.table[index]
}

func (dict *ConcurrentDict) Remove(key string) (val interface{}, result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	if val, ok := s.m[key]; ok {
		delete(s.m, key)
		dict.decreaseCount()
		return val, 1
	}
	return val, 0
}

func (dict *ConcurrentDict) RemoveWithLock(key string) (val interface{}, result int) {
	if dict == nil {
		slog.Error("dict is nil")
		return nil, 0
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	defer s.mutex.Unlock()
	s.mutex.Lock()
	if val, ok := s.m[key]; ok {
		delete(s.m, key)
		dict.decreaseCount()
		return val, 1
	}
	return nil, 0
}

func (dict *ConcurrentDict) toLockIndices(keys []string, reverse bool) []uint32 {
	indexMap := make(map[uint32]struct{})
	for _, key := range keys {
		index := dict.spread(key)
		indexMap[index] = struct{}{}
	}
	indices := make([]uint32, 0, len(indexMap))
	for index := range indexMap {
		indices = append(indices, index)
	}
	sort.Slice(indices, func(i, j int) bool {
		if !reverse {
			return indices[i] < indices[j]
		}
		return indices[i] > indices[j]
	})
	return indices
}

// RWLocks locks write keys and read keys together. allow duplicate keys
func (dict *ConcurrentDict) RWLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := dict.toLockIndices(keys, false)
	writeIndexSet := make(map[uint32]struct{})
	for _, wKey := range writeKeys {
		idx := dict.spread(wKey)
		writeIndexSet[idx] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := &dict.table[index].mutex
		if w {
			mu.Lock()
		} else {
			mu.RLock()
		}
	}
}

// RWUnLocks unlocks write keys and read keys together. allow duplicate keys
func (dict *ConcurrentDict) RWUnLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := dict.toLockIndices(keys, true)
	writeIndexSet := make(map[uint32]struct{})
	for _, wKey := range writeKeys {
		idx := dict.spread(wKey)
		writeIndexSet[idx] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := &dict.table[index].mutex
		if w {
			mu.Unlock()
		} else {
			mu.RUnlock()
		}
	}
}

func (dict *ConcurrentDict) ForEach(consumer Consumer) {
	if dict == nil {
		panic("dict is nil")

	}
	for _, s := range dict.table {
		s.mutex.RLock()
		f := func() bool {
			defer s.mutex.RUnlock()
			for key, value := range s.m {
				continues := consumer(key, value)
				if !continues {
					return false
				}
			}
			return true
		}
		if !f() {
			break
		}
	}
}

func (dict *ConcurrentDict) Keys() []string {
	if dict == nil {
		panic("dict is nil")
	}
	i := 0
	keys := make([]string, dict.Len())
	dict.ForEach(func(key string, value interface{}) bool {
		if i < len(keys) {
			keys[i] = key
			i++
		} else {
			// 如果在遍历期间有新的键被插入，那么实际遍历到的 key 数量可能会超过最初的 dict.Len()，即比预分配的 keys 切片长度要长。
			keys = append(keys, key)
		}
		return true
	})

	return keys
}

func (shard *Shard) RandomKey() string {
	if shard == nil {
		panic("shard is nil")
	}
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()

	for key := range shard.m {
		return key
	}
	return ""
}

func (dict *ConcurrentDict) Clear() {
	*dict = *MakeConcurrent(dict.shardCount)
}

func (dict *ConcurrentDict) GetShard(index uint32) *Shard {
	if dict == nil {
		panic("dict is nil")
	}
	return dict.table[index]
}

func (dict *ConcurrentDict) GetWithLock(key string) (val interface{}, exists bool) {
	// hashCode := fnv32(key)
	index := dict.spread(key)
	shard := dict.getShard(index)
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()
	val, exists = shard.m[key]
	return val, exists
}

func (dict *ConcurrentDict) PutWithLock(key string, val interface{}) int {
	// hashCode := fnv32(key)
	index := dict.spread(key)
	shard := dict.getShard(index)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()
	if _, ok := shard.m[key]; ok {
		shard.m[key] = val
		return 0
	} else {
		shard.m[key] = val
		dict.addCount()
		return 1
	}
}
func (dict *ConcurrentDict) Get(key string) (interface{}, bool) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	shard := dict.GetShard(index)
	val, exists := shard.m[key]
	return val, exists
}

func (dict *ConcurrentDict) Put(key string, val interface{}) int {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	shard := dict.GetShard(index)
	if _, ok := shard.m[key]; ok {
		shard.m[key] = val
		return 0
	}
	dict.addCount()
	shard.m[key] = val
	return 1
}
func (dict *ConcurrentDict) PutIfExistsWithLock(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 1
	}
	return 0
}

func (dict *ConcurrentDict) PutIfExists(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)

	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 1
	}
	return 0
}

func (dict *ConcurrentDict) PutIfAbsentWithLock(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		return 0
	}
	s.m[key] = val
	dict.addCount()
	return 1
}

func (dict *ConcurrentDict) PutIfAbsent(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)

	if _, ok := s.m[key]; ok {
		return 0
	}
	s.m[key] = val
	dict.addCount()
	return 1
}

func (dict *ConcurrentDict) DictScan(cursor int, count int, pattern string) ([][]byte, int) {
	size := dict.Len()
	result := make([][]byte, 0)

	if pattern == "*" && count >= size {
		return stringsToBytes(dict.Keys()), 0
	}

	matchKey, err := wildcard.CompilePattern(pattern)
	if err != nil {
		return result, -1
	}

	shardCount := len(dict.table)
	shardIndex := cursor

	for shardIndex < shardCount {
		shard := dict.table[shardIndex]
		shard.mutex.RLock()
		if len(result)+len(shard.m) > count && shardIndex > cursor {
			shard.mutex.RUnlock()
			return result, shardIndex
		}

		for key := range shard.m {
			if pattern == "*" || matchKey.IsMatch(key) {
				result = append(result, []byte(key))
			}
		}
		shard.mutex.RUnlock()
		shardIndex++
	}

	return result, 0
}
func stringsToBytes(strSlice []string) [][]byte {
	byteSlice := make([][]byte, len(strSlice))
	for i, str := range strSlice {
		byteSlice[i] = []byte(str)
	}
	return byteSlice
}

// RandomDistinctKeys randomly returns keys of the given number, won't contain duplicated key
func (dict *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.Keys()
	}

	shardCount := len(dict.table)
	result := make(map[string]struct{})
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(result) < limit {
		shardIndex := uint32(nR.Intn(shardCount))
		s := dict.getShard(shardIndex)
		if s == nil {
			continue
		}
		key := s.RandomKey()
		if key != "" {
			if _, exists := result[key]; !exists {
				result[key] = struct{}{}
			}
		}
	}
	arr := make([]string, limit)
	i := 0
	for k := range result {
		arr[i] = k
		i++
	}
	return arr
}

// RandomKeys randomly returns keys of the given number, may contain duplicated key
func (dict *ConcurrentDict) RandomKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.Keys()
	}
	shardCount := len(dict.table)

	result := make([]string, limit)
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < limit; {
		s := dict.getShard(uint32(nR.Intn(shardCount)))
		if s == nil {
			continue
		}
		key := s.RandomKey()
		if key != "" {
			result[i] = key
			i++
		}
	}
	return result
}
