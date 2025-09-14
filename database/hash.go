package database

import (
	"strconv"
	"strings"

	"github.com/zhangming/go-redis/datastruct/dict"
	"github.com/zhangming/go-redis/interfaces/database"
	"github.com/zhangming/go-redis/interfaces/redis"
	"github.com/zhangming/go-redis/lib/utils"
	"github.com/zhangming/go-redis/redis/protocol"
)

// 管理键的内部结构

func (db *DB) getAsDict(key string) (dict.Dict, protocol.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil

	}
	d, ok := entity.Data.(dict.Dict)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}

	return d, nil
}

func (db *DB) getOrInitDict(key string) (dict.Dict, bool, protocol.ErrorReply) {
	d, errReply := db.getAsDict(key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited := false
	if d == nil {
		// 这里不用并发map，是为了降低锁的消耗，在进行db相关操作的时候，db层已经加过锁了
		d = dict.MakeSimple()
		db.PutEntity(key, &database.DataEntity{
			Data: d,
		})
		inited = true
	}
	return d, inited, nil
}

func execHSet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])   // Hash 键
	field := string(args[1]) // Hash 字段名
	value := args[2]         // Hash 字段值

	d, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}
	result := d.Put(field, value)
	db.addAof(utils.ToCmdLine3("hset", args...))
	return protocol.MakeIntReply(int64(result))
}
func undoHSet(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	field := string(args[1])
	return rollbackHashFields(db, key, field)
}

func execHSetNX(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])   // Hash 键
	field := string(args[1]) // Hash 字段名
	value := args[2]         // Hash 字段值

	d, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}
	result := d.PutIfAbsent(field, value)
	db.addAof(utils.ToCmdLine3("hset", args...))
	return protocol.MakeIntReply(int64(result))
}

// 这里应该返回的是值，如果返回KV的话，应该是另外一个GetAll方法
func execHGet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])   // Hash 键
	field := string(args[1]) // Hash 字段名

	d, errReply := db.getAsDict(key)

	if errReply != nil {
		return errReply

	}
	result, ok := d.Get(field)
	if !ok {
		return protocol.MakeNullBulkReply()
	}
	//发送的命令参数是二进制安全的
	value, _ := result.([]byte)
	return protocol.MakeBulkReply(value)
}

func execHExists(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])

	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeIntReply(0)
	}

	_, exists := dict.Get(field)
	if exists {
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

func execHDel(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	fields := make([]string, len(args)-1)
	for i, v := range args[1:] {
		fields[i] = string(v)
	}

	d, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	deleted := 0
	for _, v := range fields {
		_, res := d.Remove(v)
		deleted += res
	}
	if d.Len() == 0 {
		db.Remove(key)
	}
	if deleted > 0 {
		db.addAof(utils.ToCmdLine3("hdel", args...))
	}
	return protocol.MakeIntReply(int64(deleted))
}

func undoHDel(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	fields := make([]string, len(args)-1)
	fieldArgs := args[1:]
	for i, v := range fieldArgs {
		fields[i] = string(v)
	}
	return rollbackHashFields(db, key, fields...)
}

func execHLen(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	d, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if d == nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(int64(d.Len()))
}

// 获取 Hash 键中某个字段对应的值的字节长度（byte 数）
func execHStrlen(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	filed := string(args[1])
	d, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	value, ok := d.Get(filed)
	if !ok {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(int64(len(value.([]byte))))
}

// 一次性设置多个filed-value
func execHMSet(db *DB, args [][]byte) redis.Reply {
	//eg:HMSET user:1 name "Tom" age "25"
	// parse args
	if len(args)%2 != 1 {
		return protocol.MakeSyntaxErrReply()
	}
	key := string(args[0])
	//len(args) - 1 是除 key 外的参数个数
	size := (len(args) - 1) / 2
	fields := make([]string, size)
	values := make([][]byte, size)
	for i := 0; i < size; i++ {
		fields[i] = string(args[2*i+1])
		values[i] = args[2*i+2]
	}

	dict, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	for i, field := range fields {
		value := values[i]
		dict.Put(field, value)
	}
	db.addAof(utils.ToCmdLine3("hmset", args...))
	return &protocol.OkReply{}
}

func undoHMSet(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	size := (len(args) - 1) / 2
	fields := make([]string, size)
	for i := 0; i < size; i++ {
		fields[i] = string(args[2*i+1])
	}
	return rollbackHashFields(db, key, fields...)
}

func execHMGet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	size := len(args) - 1
	fields := make([]string, size)
	for i := 0; i < size; i++ {
		fields[i] = string(args[i+1])
	}

	result := make([][]byte, size)
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return protocol.MakeMultiBulkReply(result)
	}

	for i, field := range fields {
		value, ok := dict.Get(field)
		if !ok {
			result[i] = nil
		} else {
			bytes, _ := value.([]byte)
			result[i] = bytes
		}
	}
	return protocol.MakeMultiBulkReply(result)
}

func execHKeys(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	d, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	fileds := make([][]byte, d.Len()-1)
	i := 0
	d.ForEach(func(key string, value interface{}) bool {
		fileds[i] = []byte(key)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(fileds[:i])
}

func execHVals(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	d, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	values := make([][]byte, d.Len()-1)
	i := 0
	d.ForEach(func(key string, value interface{}) bool {
		values[i] = value.([]byte)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(values[:i])
}

// 在执行 HINCRBY 等命令时，Godis 会先调用 undoHIncr 函数生成一个“回滚命令”，并把这个命令缓存起来。如果事务失败，就会执行这些 undo 命令来恢复状态。

func undoHIncr(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	field := string(args[1])
	return rollbackHashFields(db, key, field)
}

func execHIncrBy(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	field := string(args[1])
	rawDelta := string(args[2])
	delta, err := strconv.ParseInt(rawDelta, 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	d, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	value, exists := d.Get(field)
	if !exists {
		return protocol.MakeErrReply("ERR no such key")
	}
	val, err := strconv.ParseInt(string(value.([]byte)), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	val += delta
	bytes := []byte(strconv.FormatInt(val, 10))
	d.Put(field, bytes)
	db.addAof(utils.ToCmdLine3("hincrby", args...))
	return protocol.MakeBulkReply(bytes)
}

// 从存储在key的哈希值中返回一个随机字段（或字段值）
// 常见使用场景：随机推荐系统
func execHRandField(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	count := 1     // 返回字段数量
	withValue := 0 // 是否返回值
	if len(args) > 3 {
		return protocol.MakeErrReply("ERR syntax error")
	}

	if len(args) == 3 {
		if strings.ToUpper(string(args[2])) == "WITHVALUES" {
			withValue = 1
		} else {
			return protocol.MakeErrReply("ERR syntax error")
		}
	}

	if len(args) >= 2 {
		count64, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		count = int(count64)
	}
	d, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}

	if count > 0 {
		fields := d.RandomDistinctKeys(count)
		size := len(fields)
		if withValue == 0 {
			results := make([][]byte, size)
			for i, field := range fields {
				results[i] = []byte(field)
			}
			return protocol.MakeMultiBulkReply(results)
		} else {
			results := make([][]byte, size*2)
			for i, field := range fields {
				value, ok := d.Get(field)
				if !ok {
					return protocol.MakeErrReply("ERR no such key")
				}
				results[i*2] = []byte(field)
				results[i*2+1] = value.([]byte)
			}
			return protocol.MakeMultiBulkReply(results)
		}
	} else if count < 0 {
		fields := d.RandomKeys(-count)
		size := len(fields)
		if withValue == 0 {
			results := make([][]byte, size)
			for i, field := range fields {
				results[i] = []byte(field)
			}
			return protocol.MakeMultiBulkReply(results)
		} else {
			results := make([][]byte, size*2)
			for i, field := range fields {
				value, ok := d.Get(field)
				if !ok {
					return protocol.MakeErrReply("ERR no such key")
				}
				results[i*2] = []byte(field)
				results[i*2+1] = value.([]byte)
			}
			return protocol.MakeMultiBulkReply(results)
		}

	}
	// count == 0直接走这里，什么都不返回
	return protocol.MakeMultiBulkReply(nil)
}

func execHGetAll(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	d, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if d == nil {
		return protocol.MakeMultiBulkReply(nil)
	}
	size := d.Len()
	results := make([][]byte, size*2)
	i := 0
	d.ForEach(func(key string, value interface{}) bool {
		results[i] = []byte(key)
		i++
		results[i] = value.([]byte)
		i++
		return true
	})
	return protocol.MakeMultiBulkReply(results[:i])
}

func execHScan(db *DB, args [][]byte) redis.Reply {
	//指定每次迭代返回的字段数量
	count := 10
	//匹配模式
	pattern := "*"
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR syntax error")
	}
	if len(args) >= 2 {
		for i := 2; i < len(args); i++ {
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
			} else {
				return &protocol.SyntaxErrReply{}
			}
		}
	}
	key := string(args[0])
	d, errReply := db.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	cursor, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("ERR invalid cursor")
	}
	//用于下一次请求时传入，以继续扫描。
	keysReply, nextCursor := d.DictScan(cursor, count, pattern)
	if nextCursor < 0 {
		return protocol.MakeErrReply("Invalid argument")
	}

	result := make([]redis.Reply, 2)
	result[0] = protocol.MakeBulkReply([]byte(strconv.FormatInt(int64(nextCursor), 10)))
	result[1] = protocol.MakeMultiBulkReply(keysReply)

	return protocol.MakeMultiRawReply(result)
}

func init() {
	registerCommand("HSet", execHSet, writeFirstKey, undoHSet, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("HSetNX", execHSetNX, writeFirstKey, undoHSet, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("HGet", execHGet, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("HExists", execHExists, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("HDel", execHDel, writeFirstKey, undoHDel, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("HLen", execHLen, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("HStrlen", execHStrlen, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("HMSet", execHMSet, writeFirstKey, undoHMSet, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("HMGet", execHMGet, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("HGet", execHGet, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("HKeys", execHKeys, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 1, 1, 1)
	registerCommand("HVals", execHVals, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 1, 1, 1)
	registerCommand("HGetAll", execHGetAll, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagRandom}, 1, 1, 1)
	registerCommand("HIncrBy", execHIncrBy, writeFirstKey, undoHIncr, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("HRandField", execHRandField, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagRandom, redisFlagReadonly}, 1, 1, 1)
	registerCommand("HScan", execHScan, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 1, 1, 1)
}
