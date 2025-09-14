package set

import (
	"github.com/zhangming/go-redis/datastruct/dict"
	"github.com/zhangming/go-redis/lib/wildcard"
)

type Set struct {
	dict dict.Dict
}

func (set *Set) Add(val string) int {
	return set.dict.Put(val, nil)
}

func (set *Set) Remove(val string) int {
	_, ret := set.dict.Remove(val)
	return ret
}

func Make(members ...string) *Set {
	set := &Set{
		dict: dict.MakeSimple(),
	}
	for _, member := range members {
		set.Add(member)
	}
	return set
}

// 并发情况下安全创建
func MakeConcurrentSafe(members ...string) *Set {
	set := &Set{
		dict: dict.MakeConcurrent(1),
	}
	for _, member := range members {
		set.Add(member)
	}
	return set
}

func (set *Set) Has(key string) bool {
	if key == "" {
		return false
	}
	_, exist := set.dict.Get(key)
	return exist
}

func (set *Set) Len() int {
	if set == nil {
		return 0
	}
	return set.dict.Len()
}

func (set *Set) ToSlice() []string {
	if set == nil {
		return nil
	}
	i := 0
	slice := make([]string, set.Len())
	set.dict.ForEach(func(key string, val interface{}) bool {
		if i < len(slice) {
			slice[i] = key
		} else {
			// set extended during traversal
			slice = append(slice, key)
		}
		i++
		return true
	})
	return slice
}

func (set *Set) ForEach(consumer func(member string) bool) {
	if set == nil {
		return
	}
	set.dict.ForEach(func(key string, val interface{}) bool {
		return consumer(key)
	})
}

// 交集
func Intersect(sets ...*Set) *Set {
	result := Make()
	if len(sets) == 0 {
		return result
	}

	countMap := make(map[string]int)
	for _, set := range sets {
		set.ForEach(func(member string) bool {
			countMap[member]++
			return true
		})
	}
	for k, v := range countMap {
		if v == len(sets) {
			result.Add(k)
		}
	}
	return result
}

// 并集
func Union(sets ...*Set) *Set {
	result := Make()
	if len(sets) == 0 {
		return result
	}
	for _, set := range sets {
		set.ForEach(func(member string) bool {
			result.Add(member)
			return true
		})
	}
	return result
}

// 差集
func Diff(sets ...*Set) *Set {
	if len(sets) == 0 {
		return Make()
	}
	result := sets[0].ShallowCopy()
	for i := 1; i < len(sets); i++ {
		sets[i].ForEach(func(member string) bool {
			result.Remove(member)
			return true
		})
		if result.Len() == 0 {
			break
		}
	}
	return result
}

func (set *Set) ShallowCopy() *Set {
	result := Make()
	set.ForEach(func(member string) bool {
		result.Add(member)
		return true
	})
	return result
}

// 扫描集合中符合特定模式的成员
func (set *Set) SetScan(cursor int, count int, pattern string) ([][]byte, int) {
	result := make([][]byte, 0)
	matchKey, err := wildcard.CompilePattern(pattern)
	if err != nil {
		return result, -1
	}
	set.ForEach(func(member string) bool {
		if pattern == "*" || matchKey.IsMatch(member) {
			result = append(result, []byte(member))
		}
		return true
	})

	return result, 0
}

func (set *Set) RandomMembers(limit int) []string {
	if set == nil || set.dict == nil {
		return nil
	}
	return set.dict.RandomKeys(limit)
}

func (set *Set) RandomDistinctMembers(limit int) []string {
	return set.dict.RandomDistinctKeys(limit)
}