package database

import (
	"log/slog"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/zhangming/go-redis/aof"
	"github.com/zhangming/go-redis/datastruct/dict"
	"github.com/zhangming/go-redis/datastruct/list"
	"github.com/zhangming/go-redis/datastruct/set"
	"github.com/zhangming/go-redis/datastruct/sortedset"
	"github.com/zhangming/go-redis/interfaces/redis"
	"github.com/zhangming/go-redis/lib/utils"
	"github.com/zhangming/go-redis/lib/wildcard"
	"github.com/zhangming/go-redis/redis/protocol"
)

const (
	redisFlagWrite         = "write"
	redisFlagReadonly      = "readonly"
	redisFlagDenyOOM       = "denyoom"
	redisFlagAdmin         = "admin"
	redisFlagPubSub        = "pubsub"
	redisFlagNoScript      = "noscript"
	redisFlagRandom        = "random"
	redisFlagSortForScript = "sortforscript"
	redisFlagLoading       = "loading"
	redisFlagStale         = "stale"
	redisFlagSkipMonitor   = "skip_monitor"
	redisFlagAsking        = "asking"
	redisFlagFast          = "fast"
	redisFlagMovableKeys   = "movablekeys"
)

//通用键操作
//管理键的生命周期、存在性、扫描、过期等

func toTTLCmd(db *DB, key string) *protocol.MultiBulkReply {
	raw, exists := db.ttlMap.Get(key)
	if !exists {
		// has no TTL
		return protocol.MakeMultiBulkReply(utils.ToCmdLine("PERSIST", key))
	}
	expireTime, _ := raw.(time.Time)
	timestamp := strconv.FormatInt(expireTime.UnixNano()/1000/1000, 10)
	return protocol.MakeMultiBulkReply(utils.ToCmdLine("PEXPIREAT", key, timestamp))
}

// 删除
func execDel(db *DB, args [][]byte) redis.Reply {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	deleted := db.Removes(keys...)
	if deleted > 0 {
		db.addAof(utils.ToCmdLine3("del", args...))
	}
	return protocol.MakeIntReply(int64(deleted))
}

// 删除的回滚
func undoDel(db *DB, args [][]byte) []CmdLine {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	return rollbackGivenKeys(db, keys...)
}

// 检查是否存在
func execExists(db *DB, args [][]byte) redis.Reply {
	result := 0
	for _, arg := range args {
		_, exist := db.GetEntity(string(arg))
		if exist {
			result++
		}
	}
	return protocol.MakeIntReply(int64(result))
}

func execFlushDB(db *DB, args [][]byte) redis.Reply {
	db.Flush()
	db.addAof(utils.ToCmdLine3("flushdb", args...))
	return &protocol.OkReply{}
}

func getType(db *DB, key string) string {
	entity, exists := db.GetEntity(key)
	if !exists {
		return "none"
	}
	switch entity.Data.(type) {
	case []byte:
		return "string"
	case list.List:
		return "list"
	case dict.Dict:
		return "hash"
	case *set.Set:
		return "set"
	case *sortedset.SortedSet:
		return "zset"
	}
	return ""
}

// 检查类型
func execType(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	result := getType(db, key)
	if result != "" {
		return protocol.MakeStatusReply(result)
	} else {
		return &protocol.UnknownErrReply{}
	}
}

func prepareRename(args [][]byte) ([]string, []string) {
	src := string(args[0])
	dest := string(args[1])
	return []string{dest}, []string{src}
}

func undoRename(db *DB, args [][]byte) []CmdLine {
	set := string(args[0])
	dset := string(args[1])
	return rollbackGivenKeys(db, set, dset)
}

// 执行重命名
// 如果 dest 存在，直接覆盖。
// 清除 src 和 dest 的 TTL。
// 设置新的 TTL（如果原 src 有 TTL）。
func execRename(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'rename' command")
	}
	src := string(args[0])
	dest := string(args[1])

	entity, ok := db.GetEntity(src)
	if !ok {
		return protocol.MakeErrReply("no such key")
	}
	rawTTL, hasTTL := db.ttlMap.Get(src)
	db.PutEntity(dest, entity)
	db.Remove(src)
	if hasTTL {
		db.Persist(src) // clean src and dest with their ttl
		db.Persist(dest)
		expireTime, _ := rawTTL.(time.Time)
		db.Expire(dest, expireTime)
	}
	db.addAof(utils.ToCmdLine3("rename", args...))
	return &protocol.OkReply{}
}

// 执行重命名
// 如果 dest 存在，直接返回 0。
// 清除 src 和 dest 的 TTL。
// 设置新的 TTL（如果原 src 有 TTL）。
func execRenameNx(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'renamenx' command")
	}
	src := string(args[0])
	dest := string(args[1])

	entity, ok := db.GetEntity(src)
	if !ok {
		return protocol.MakeIntReply(0)
	}
	_, ok = db.GetEntity(dest)
	if ok {
		return protocol.MakeIntReply(0)
	}
	rawTTL, hasTTL := db.ttlMap.Get(src)
	db.PutEntity(dest, entity)
	db.Remove(src)
	if hasTTL {
		db.Persist(src)
		db.Persist(dest)
		expireTime, _ := rawTTL.(time.Time)
		db.Expire(dest, expireTime)
	}
	db.addAof(utils.ToCmdLine3("renamenx", args...))
	return protocol.MakeIntReply(1)
}

// 设置key的时间以秒为单位
func execExpire(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	expireTime := time.Now().Add(time.Duration(ttlArg) * time.Second)
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}
	db.Expire(key, expireTime)
	db.addAof(aof.MakeExpireCmd(key, expireTime).Args)
	return protocol.MakeIntReply(1)
}

// 在Unix时间戳中设置密钥的过期时间
// 绝对时间，在哪个时间点过期（秒级 Unix 时间戳）
func execExpireAt(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	raw, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	expireAt := time.Unix(raw, 0)

	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	db.Expire(key, expireAt)
	db.addAof(aof.MakeExpireCmd(key, expireAt).Args)
	return protocol.MakeIntReply(1)
}

// 查询一个键的 绝对过期时间戳（秒）
func execExpireTime(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(-2)
	}

	raw, exists := db.ttlMap.Get(key)
	if !exists {
		return protocol.MakeIntReply(-1)
	}
	rawExpireTime, _ := raw.(time.Time)
	expireTime := rawExpireTime.Unix()
	return protocol.MakeIntReply(expireTime)
}

// 查询一个键的 剩余生存时间（秒）
func execTTL(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(-2)
	}

	raw, exists := db.ttlMap.Get(key)
	if !exists {
		return protocol.MakeIntReply(-1)

	}
	expireTime, _ := raw.(time.Time)
	ttl := expireTime.Sub(time.Now()).Seconds()
	return protocol.MakeIntReply(int64(math.Round(ttl)))
}

// 删除键
func execPersist(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	db.Persist(key)
	db.addAof(utils.ToCmdLine3("persist", args...))
	return protocol.MakeIntReply(1)
}
func undoExpire(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	return []CmdLine{
		toTTLCmd(db, key).Args,
	}
}

// 返回所有键
func execKeys(db *DB, args [][]byte) redis.Reply {
	pattern, err := wildcard.CompilePattern(string(args[0]))
	if err != nil {
		return protocol.MakeErrReply("ERR pattern is not a valid glob-style pattern")
	}
	result := make([][]byte, 0)
	// 这里的foreach是加锁的了，所以这就是为什么不推荐使用的原因了
	db.data.ForEach(func(key string, value interface{}) bool {
		if !pattern.IsMatch(key) {
			return true
		}
		if !db.IsExpired(key) {
			result = append(result, []byte(key))
		}
		return true
	})
	return protocol.MakeMultiBulkReply(result)
}

func execScan(db *DB, args [][]byte) redis.Reply {
	var count int = 10
	slog.Info("scan args:", len(args))
	// 初始化默认匹配模式
	var pattern string = "*"
	var scanType string = ""
	if len(args) > 1 {
		for i := 0; i < len(args); i++ {
			arg := strings.ToLower(string(args[i]))
			if arg == "count" {
				count0, err := strconv.Atoi(string(args[i+1]))
				if err != nil {
					return &protocol.SyntaxErrReply{}
				}
				count = count0
				i++
			} else if arg == "match" {
				pattern = string(args[i+1])
				i++
			} else if arg == "type" {
				scanType = strings.ToLower(string(args[i+1]))
				i++
			} else {
				return &protocol.SyntaxErrReply{}
			}
		}
	}
	cursor, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return protocol.MakeErrReply("ERR invalid cursor")
	}
	// 针对那一部分的分片上锁
	keysReply, nextCursor := db.data.DictScan(cursor, count, pattern)
	if nextCursor < 0 {
		return protocol.MakeErrReply("Invalid argument")
	}

	if len(scanType) != 0 {
		for i := 0; i < len(keysReply); {
			if getType(db, string(keysReply[i])) != scanType {
				keysReply = append(keysReply[:i], keysReply[i+1:]...)
			} else {
				i++
			}
		}
	}
	result := make([]redis.Reply, 2)
	result[0] = protocol.MakeBulkReply([]byte(strconv.FormatInt(int64(nextCursor), 10)))
	result[1] = protocol.MakeMultiBulkReply(keysReply)

	return protocol.MakeMultiRawReply(result)
}

func init() {
	registerCommand("Del", execDel, writeAllKeys, undoDel, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, -1, 1)
	registerCommand("Expire", execExpire, writeFirstKey, undoExpire, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("ExpireAt", execExpireAt, writeFirstKey, undoExpire, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("ExpireTime", execExpireTime, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("TTL", execTTL, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagRandom, redisFlagFast}, 1, 1, 1)
	registerCommand("Persist", execPersist, writeFirstKey, undoExpire, 2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("Exists", execExists, readAllKeys, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("Type", execType, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("Rename", execRename, prepareRename, undoRename, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("RenameNx", execRenameNx, prepareRename, undoRename, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("Keys", execKeys, noPrepare, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 1, 1, 1)
	registerCommand("Scan", execScan, noPrepare, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 1, 1, 1)
}
