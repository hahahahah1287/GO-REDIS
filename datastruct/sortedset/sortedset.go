package sortedset

import (
	"strconv"

	"github.com/zhangming/go-redis/lib/wildcard"
)

type SortedSet struct {
	skiplist *skiplist
	dict     map[string]*Element
}

func Make() *SortedSet {
	return &SortedSet{
		dict:     make(map[string]*Element),
		skiplist: makeSkiplist(),
	}
}

func (sortedSet *SortedSet) Len() int64 {
	return int64(len(sortedSet.dict))
}

func (sortedSet *SortedSet) Get(key string) (*Element, bool) {
	val, ok := sortedSet.dict[key]
	if !ok {
		return nil, false
	}
	return val, true
}

func (sortedSet *SortedSet) Remove(member string) bool {
	val, ok := sortedSet.dict[member]
	if ok {
		sortedSet.skiplist.remove(val.Score, member)
		delete(sortedSet.dict, member)
		return true
	}
	return false
}

func (sortedSet *SortedSet) Add(member string, score float64) bool {
	element, ok := sortedSet.dict[member]
	sortedSet.dict[member] = &Element{
		Member: member,
		Score:  score,
	}
	if ok {
		if score != element.Score {
			//每个成员（member）都有一个关联的分数（score），这个分数决定了成员在集合中的排序位置。
			//当需要更新一个成员的分数时，不能直接修改原节点的分数，因为这会破坏跳跃表（skip list）这种数据结构的有序性。
			sortedSet.skiplist.remove(element.Score, member)
			sortedSet.skiplist.insert(score, member)
		}
		return false
	}
	sortedSet.skiplist.insert(score, member)
	return true
}

func (sortedSet *SortedSet) GetRank(member string, desc bool) (rank int64) {
	v, ok := sortedSet.dict[member]
	if !ok {
		return -1
	}
	r := sortedSet.skiplist.getRank(member, v.Score)
	if desc {
		r = sortedSet.skiplist.length - r
	} else {
		r--
	}
	return r
}

func (sortedSet *SortedSet) GetByRank(rank int64, desc bool) (member string, score float64) {
	if rank < 0 || rank >= sortedSet.skiplist.length {
		return "", 0
	}
	if desc {
		rank = sortedSet.skiplist.length - rank
	}
	element := sortedSet.skiplist.getByRank(rank)
	return element.Member, element.Score
}

// 有序集合（SortedSet）按排名（rank）进行遍历
func (sortedSet *SortedSet) ForEachByRank(start int64, stop int64, desc bool, consumer func(element *Element) bool) {
	size := sortedSet.Len()
	if start < 0 || start > size {
		panic("start out of range")
	}
	if stop < 0 || stop > size {
		panic("stop out of range")
	}

	// 寻找开始的节点
	var node *Node
	if desc {
		node = sortedSet.skiplist.tail
		if start > 0 {
			node = sortedSet.skiplist.getByRank(size - start)
		}
	} else {
		node = sortedSet.skiplist.header.level[0].forward
		if start > 0 {
			node = sortedSet.skiplist.getByRank(start + 1)
		}
	}

	// 遍历
	sliceSize := int(stop - start)
	for i := 0; i < sliceSize; i++ {
		if !consumer(&node.Element) {
			break
		}
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
	}
}

func (sortedSet *SortedSet) RangeByRank(start int64, stop int64, desc bool) []*Element {
	sliceSize := int(stop - start)
	slice := make([]*Element, sliceSize)
	i := 0
	sortedSet.ForEachByRank(start, stop, desc, func(element *Element) bool {
		slice[i] = element
		i++
		return true
	})
	return slice
}

func (sortedSet *SortedSet) RangeCount(min Border, max Border) int64 {
	var i int64 = 0
	sortedSet.ForEachByRank(0, sortedSet.Len(), false, func(element *Element) bool {
		gtMin := min.less(element)
		if !gtMin {
			return true
		}
		ltMax := max.greater(element)
		if !ltMax {
			return false
		}
		i++
		return true
	})
	return i
}

// 按分数区间遍历
func (sortedSet *SortedSet) ForEach(min Border, max Border, offset int64, limit int64, desc bool, consumer func(element *Element) bool) {
	var node *Node
	if desc {
		node = sortedSet.skiplist.getLastInRange(min, max)
	} else {
		node = sortedSet.skiplist.getFirstInRange(min, max)
	}

	// 跳过前几个节点
	for node != nil && offset > 0 {
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
		offset--
	}
	for i := 0; (i < int(limit) || limit < 0) && node != nil; i++ {
		if !consumer(&node.Element) {
			break
		}
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
		if node == nil {
			break
		}
		gtMin := min.less(&node.Element) // greater than min
		ltMax := max.greater(&node.Element)
		if !gtMin || !ltMax {
			break // break through score border
		}
	}
}

func (sortedSet *SortedSet) Range(min Border, max Border, offset int64, limit int64, desc bool) []*Element {
	if limit < 0 || offset < 0 {
		return []*Element{}
	}
	slices := make([]*Element, 0)
	sortedSet.ForEach(min, max, offset, limit, desc, func(element *Element) bool {
		slices = append(slices, element)
		return true
	})
	return slices
}

func (sortedSet *SortedSet) RemoveRange(min Border, max Border) int64 {
	removed := sortedSet.skiplist.RemoveRange(min, max, 0)
	for _, element := range removed {
		delete(sortedSet.dict, element.Member)
	}
	return int64(len(removed))
}

func (sortedSet *SortedSet) RemoveByRank(start int64, stop int64) int64 {
	removed := sortedSet.skiplist.RemoveRangeByRank(start+1, stop+1)
	for _, element := range removed {
		delete(sortedSet.dict, element.Member)
	}
	return int64(len(removed))
}

func (sortedSet *SortedSet) PopMin(count int) []*Element {
	// 获取最小元素
	first := sortedSet.skiplist.getFirstInRange(scoreNegativeInfBorder, scorePositiveInfBorder)
	if first == nil {
		return nil
	}
	border := &ScoreBorder{
		Value:   first.Score,
		Exclude: false,
	}
	// 移除
	removed := sortedSet.skiplist.RemoveRange(border, scorePositiveInfBorder, count)
	for _, element := range removed {
		delete(sortedSet.dict, element.Member)
	}
	return removed
}

func (sortedSet *SortedSet) ZSetScan(cursor int, count int, pattern string) ([][]byte, int) {
	result := make([][]byte, 0)
	matchKey, err := wildcard.CompilePattern(pattern)
	if err != nil {
		return result, -1
	}
	for k := range sortedSet.dict {
		if pattern == "*" || matchKey.IsMatch(k) {
			elem, exists := sortedSet.dict[k]
			if !exists {
				continue
			}
			result = append(result, []byte(k))
			result = append(result, []byte(strconv.FormatFloat(elem.Score, 'f', 10, 64)))
		}
	}
	return result, 0
}

