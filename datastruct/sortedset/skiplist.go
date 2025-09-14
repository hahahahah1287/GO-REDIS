package sortedset

import (
	"math/bits"
	"math/rand"
)

const (
	maxLevel = 16
)

// 对外的元素抽象
type Element struct {
	Member string
	Score  float64 //跳表中每个元素的排序权重
}

type Node struct {
	Element // 元素的名称和 score
	// 所有层级的节点在逻辑上都对应于 level 0 上的节点，因此只需维护一个 backward 指针即可满足反向查找需求。
	// 要不然会浪费空间
	backward *Node    // 后向指针
	level    []*Level // 前向指针, level[0] 为最下层
}

// 节点中每一层的抽象
type Level struct {
	forward *Node // 指向同层中的下一个节点
	span    int64 // 到 forward 跳过的节点数
}

// 跳表的定义
type skiplist struct {
	header *Node
	tail   *Node
	length int64
	level  int16
}

func makeNode(level int16, score float64, member string) *Node {
	n := &Node{
		Element: Element{
			Score:  score,
			Member: member,
		},
		level: make([]*Level, level),
	}
	for i := range n.level {
		n.level[i] = new(Level)
	}
	return n
}

func makeSkiplist() *skiplist {
	return &skiplist{
		level:  1,
		header: makeNode(maxLevel, 0, ""),
	}
}

// 寻找排名为 rank 的节点, rank 从1开始
func (sl *skiplist) getByRank(rank int64) *Node {
	var i int64 = 0
	n := sl.header
	for level := sl.level - 1; level >= 0; level-- {
		// 从当前层向前搜索
		// 若当前层的下一个节点已经超过目标 (i+n.level[level].span > rank)，则结束当前层搜索进入下一层
		for n.level[level].forward != nil && (i+n.level[level].span) <= rank {
			i += n.level[level].span
			n = n.level[level].forward
		}
		if i == rank {
			return n
		}
	}
	return nil
}

func (sl *skiplist) GetRank(score float64, member string) int64 {
	var rank int64 = 0
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.Score < score ||
				(x.level[i].forward.Score == score &&
					x.level[i].forward.Member <= member)) {
			rank += x.level[i].span
			x = x.level[i].forward
		}

		if member == x.Member {
			return rank
		}
	}
	return 0
}

func (skiplist *skiplist) hasInRange(min Border, max Border) bool {
	if min.isIntersected(max) { //是有交集的，则返回false
		return false
	}
	// min > tail
	n := skiplist.tail
	if n == nil || !min.less(&n.Element) {
		return false
	}
	// max < head
	n = skiplist.header.level[0].forward
	if n == nil || !max.greater(&n.Element) {
		return false
	}
	return true
}
func (skiplist *skiplist) getFirstInRange(min Border, max Border) *Node {
	if !skiplist.hasInRange(min, max) { // 检查范围内是否有元素
		return nil
	}
	n := skiplist.header
	// 从最高层往下扫描
	for level := skiplist.level - 1; level >= 0; level-- {
		// 如果当前层的下一个节点不在范围内，则向前移动
		for n.level[level].forward != nil && !min.less(&n.level[level].forward.Element) {
			n = n.level[level].forward
		}
	}
	/* 这里应该是在范围内的第一个节点 */
	n = n.level[0].forward
	if !max.greater(&n.Element) { // 确保该节点在最大边界内
		return nil
	}
	return n
}

func (skiplist *skiplist) getLastInRange(min Border, max Border) *Node {
	if !skiplist.hasInRange(min, max) { // 检查范围内是否有元素
		return nil
	}
	n := skiplist.header
	// 从最高层往下扫描
	for level := skiplist.level - 1; level >= 0; level-- {
		for n.level[level].forward != nil && max.greater(&n.level[level].forward.Element) {
			n = n.level[level].forward
		}
	}
	if !min.less(&n.Element) { // 确保该节点在最小边界外
		return nil
	}
	return n
}

/*
 * param node: node to delete
 * param update: backward node (of target)
 */
func (skiplist *skiplist) removeNode(node *Node, update []*Node) {
	for i := 0; i < len(update); i++ {
		if update[i].level[i].forward == node {
			update[i].level[i].span += node.level[i].span - 1
			update[i].level[i].forward = node.level[i].forward

		} else {
			update[i].level[i].span--
		}
	}
	if node.level[0].forward != nil {
		// 当前节点不是链表的最后一个节点，因此需要将下一个节点的 backward 指针指向当前节点的前一个节点（node.backward），以绕过当前节点。
		node.level[0].forward.backward = node.backward
	} else {
		// 当前节点是链表的最后一个节点，此时需要将跳表的尾指针 tail 更新为当前节点的前一个节点（node.backward）。
		skiplist.tail = node.backward
	}
	for skiplist.level > 1 && skiplist.header.level[skiplist.level-1].forward == nil {
		skiplist.level--
	}
	skiplist.length--
}

func (skiplist *skiplist) remove(score float64, member string) bool {
	update := make([]*Node, maxLevel)
	node := skiplist.header
	for i := skiplist.level - 1; i >= 0; i-- {
		// node.level[i].forward != nil：确保当前层有下一个节点。
		// node.level[i].forward.Score < score：下一个节点的分数小于目标分数。
		// 或者：
		// node.level[i].forward.Score == score && node.level[i].forward.Member < member：分数相等时，按成员字符串大小排序。
		for node.level[i].forward != nil &&
			(node.level[i].forward.Score < score ||
				(node.level[i].forward.Score == score &&
					node.level[i].forward.Member < member)) {
			node = node.level[i].forward
		}
		update[i] = node
	}
	node = node.level[0].forward
	if node != nil && score == node.Score && node.Member == member {
		skiplist.removeNode(node, update)
		// free x
		return true
	}
	return false
}

func (skiplist *skiplist) insert(score float64, member string) *Node {
	update := make([]*Node, maxLevel)
	//  记录每一层上从头节点到当前查找位置所经过的节点数量。
	//  rank[i] 表示：在第 i 层，从 header 到当前 node 所跨越的节点数。
	rank := make([]int64, maxLevel)

	node := skiplist.header
	for i := skiplist.level - 1; i >= 0; i-- {
		if i == skiplist.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		if node.level[i] != nil {
			for node.level[i].forward != nil &&
				(node.level[i].forward.Score < score ||
					(node.level[i].forward.Score == score && node.level[i].forward.Member < member)) {
				rank[i] += node.level[i].span
				node = node.level[i].forward
			}
			update[i] = node
		}
	}
	level := randomLevel()
	node = makeNode(level, score, member)
	for i := int16(0); i < level; i++ {
		node.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = node

		node.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	for i := level; i < skiplist.level; i++ {
		update[i].level[i].span++
	}

	if update[0] == skiplist.header {
		node.backward = nil
	} else {
		node.backward = update[0]
	}

	if node.level[0].forward != nil {
		node.level[0].forward.backward = node
	} else {
		skiplist.tail = node
	}

	skiplist.length++
	return node
}

func randomLevel() int16 {
	total := uint64(1)<<uint64(maxLevel) - 1
	k := rand.Uint64() % total
	return maxLevel - int16(bits.Len64(k+1)) + 1
}

func (skiplist *skiplist) getRank(member string, score float64) int64 {
	var rank int64 = 0
	x := skiplist.header
	for i := skiplist.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.Score < score ||
				(x.level[i].forward.Score == score &&
					x.level[i].forward.Member <= member)) {
			rank += x.level[i].span
			x = x.level[i].forward
		}

		/* x might be equal to zsl->header, so test if obj is non-NULL */
		if x.Member == member {
			return rank
		}
	}
	return 0
}

func (skiplist *skiplist) RemoveRange(min Border, max Border, limit int) (removed []*Element) {
	update := make([]*Node, maxLevel)
	removed = make([]*Element, 0)
	// find backward nodes (of target range) or last node of each level
	node := skiplist.header
	for i := skiplist.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil {
			if min.less(&node.level[i].forward.Element) { // already in range
				break
			}
			node = node.level[i].forward
		}
		update[i] = node
	}

	// node is the first one within range
	node = node.level[0].forward

	// remove nodes in range
	for node != nil {
		if !max.greater(&node.Element) { // already out of range
			break
		}
		next := node.level[0].forward
		removedElement := node.Element
		removed = append(removed, &removedElement)
		skiplist.removeNode(node, update)
		if limit > 0 && len(removed) == limit {
			break
		}
		node = next
	}
	return removed
}

func (skiplist *skiplist) RemoveRangeByRank(start int64, stop int64) (removed []*Element) {
	var i int64 = 0 // rank of iterator
	update := make([]*Node, maxLevel)
	removed = make([]*Element, 0)

	// scan from top level
	node := skiplist.header
	for level := skiplist.level - 1; level >= 0; level-- {
		for node.level[level].forward != nil && (i+node.level[level].span) < start {
			i += node.level[level].span
			node = node.level[level].forward
		}
		update[level] = node
	}

	i++
	node = node.level[0].forward // first node in range

	// remove nodes in range
	for node != nil && i < stop {
		next := node.level[0].forward
		removedElement := node.Element
		removed = append(removed, &removedElement)
		skiplist.removeNode(node, update)
		node = next
		i++
	}
	return removed
}
