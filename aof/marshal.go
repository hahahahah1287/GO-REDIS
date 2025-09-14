package aof

import (
	"strconv"
	"time"

	"github.com/zhangming/go-redis/datastruct/dict"
	"github.com/zhangming/go-redis/datastruct/list"
	List "github.com/zhangming/go-redis/datastruct/list"
	"github.com/zhangming/go-redis/datastruct/set"
	"github.com/zhangming/go-redis/datastruct/sortedset"
	"github.com/zhangming/go-redis/interfaces/database"
	"github.com/zhangming/go-redis/redis/protocol"
)

// EntityToCmd serialize data entity to redis command
// 该函数将内存中的数据实体(database.DataEntity)转换为Redis命令格式(protocol.MultiBulkReply)
// 当Redis服务器重启时，可以通过执行这些转换后的命令来恢复数据
//AOF重写（AOF Rewrite）这里是这个，例如我set key 多次，这里的话直接重写的话，直接设置最终态的写法
func EntityToCmd(key string, entity *database.DataEntity) *protocol.MultiBulkReply {
	if entity == nil {
		return nil
	}
	var cmd *protocol.MultiBulkReply
	switch val := entity.Data.(type) {
	case []byte:
		cmd = stringToCmd(key, val)
	case list.List:
		cmd = listToCmd(key, val)
	case *set.Set:
		cmd = setToCmd(key, val)
	case dict.Dict:
		cmd = hashToCmd(key, val)
	case *sortedset.SortedSet:
		cmd = zSetToCmd(key, val)
	}
	return cmd
}

var pExpireAtBytes = []byte("PEXPIREAT")

// MakeExpireCmd 生成命令行以设置给定键的过期时间
func MakeExpireCmd(key string, expireAt time.Time) *protocol.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = pExpireAtBytes
	args[1] = []byte(key)
	args[2] = []byte(strconv.FormatInt(expireAt.UnixNano()/1e6, 10))
	return protocol.MakeMultiBulkReply(args)
}

var setCmd = []byte("SET")

func stringToCmd(key string, bytes []byte) *protocol.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = setCmd
	args[1] = []byte(key)
	args[2] = bytes
	return protocol.MakeMultiBulkReply(args)
}

var rPushAllCmd = []byte("RPUSH")

func listToCmd(key string, list List.List) *protocol.MultiBulkReply {
	args := make([][]byte, 2+list.Len())
	args[0] = rPushAllCmd
	args[1] = []byte(key)
	list.ForEach(func(i int, val interface{}) bool {
		bytes, _ := val.([]byte)
		args[2+i] = bytes
		return true
	})
	return protocol.MakeMultiBulkReply(args)
}

var sAddCmd = []byte("SADD")

func setToCmd(key string, set *set.Set) *protocol.MultiBulkReply {
	args := make([][]byte, 2+set.Len())
	args[0] = sAddCmd
	args[1] = []byte(key)
	i := 0
	set.ForEach(func(val string) bool {
		args[2+i] = []byte(val)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(args)
}

var hMSetCmd = []byte("HMSET")

func hashToCmd(key string, hash dict.Dict) *protocol.MultiBulkReply {
	args := make([][]byte, 2+hash.Len()*2)
	args[0] = hMSetCmd
	args[1] = []byte(key)
	i := 0
	hash.ForEach(func(key string, val interface{}) bool {
		bytes, _ := val.([]byte)
		args[2*i] = []byte(key)
		args[2*i+1] = bytes
		i++
		return true

	})
	return protocol.MakeMultiBulkReply(args)
}

var zAddCmd = []byte("ZADD")

func zSetToCmd(key string, zSet *sortedset.SortedSet) *protocol.MultiBulkReply {
	args := make([][]byte, 2+zSet.Len()*2)
	args[0] = zAddCmd
	args[1] = []byte(key)
	i := 0
	zSet.ForEachByRank(int64(0), int64(zSet.Len()), true, func(element *sortedset.Element) bool {
		value := strconv.FormatFloat(element.Score, 'f', -1, 64)
		args[2+i*2] = []byte(value)
		args[3+i*2] = []byte(element.Member)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(args)
}
