package database

import (
	"log/slog"
	"strings"
	"time"

	"github.com/zhangming/go-redis/datastruct/dict"
	"github.com/zhangming/go-redis/interfaces/database"
	"github.com/zhangming/go-redis/interfaces/redis"
	"github.com/zhangming/go-redis/lib/timewheel"
	"github.com/zhangming/go-redis/redis/protocol"
)

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
)

// DB stores data and execute user's commands
type DB struct {
	index int
	// 数据存储的键值对
	data *dict.ConcurrentDict
	// key -> expireTime (time.Time) 记录键的过期时间
	ttlMap *dict.ConcurrentDict
	// key -> version(uint32) 记录键的版本信息
	versionMap *dict.ConcurrentDict
	// addaof is used to add command to aof
	addAof func(CmdLine)
	// callbacks
	// 回调函数
	insertCallback database.KeyEventCallback
	deleteCallback database.KeyEventCallback
}

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

func makeBasicDB() *DB {
	db := &DB{
		data:       dict.MakeConcurrent(dataDictSize),
		ttlMap:     dict.MakeConcurrent(ttlDictSize),
		versionMap: dict.MakeConcurrent(dataDictSize),
		addAof:     func(line CmdLine) {},
	}
	return db
}

/* ---- Transaction Functions ---- */

// 参数验证
func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

// execNormalCommand 是完整的命令执行流程，包含加锁、版本控制等
func (db *DB) execNormalCommand(cmdLine [][]byte) redis.Reply {
	slog.Info("exec normal command")
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + string(cmdLine[0]) + "'")
	}
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrReply(cmdName)
	}

	prepare := cmd.prepare
	write, read := prepare(cmdLine[1:])
	db.addVersion(write...)

	// defer fmt.Println("锁放执行完毕")
	slog.Info("即将执行命令")
	db.RWLocks(write, read)
	defer db.RWUnLocks(write, read)
	// defer func() {
	// 	if err := recover(); err != nil {
	// 		slog.Error("panic in command execution", "err", err)
	// 	}
	// 	db.RWUnLocks(write, read) // 确保锁释放
	// 	fmt.Println("锁释放执行完毕")
	// }()
	executer := cmd.executor
	return executer(db, cmdLine[1:])
}

func execMulti(db *DB, conn redis.Connection) redis.Reply {
	slog.Info("已经进入execMulti方法")
	if !conn.InMultiState() {
		return protocol.MakeErrReply("ERR EXEC without MULTI")
	}
	defer conn.SetMultiState(false)
	if len(conn.GetTxErrors()) > 0 {
		return protocol.MakeErrReply("EXECABORT Transaction discarded because of previous errors.")
	}
	cmdLines := conn.GetQueuedCmdLine()
	slog.Info("即将进入db.ExecMulti方法")
	return db.ExecMulti(conn, conn.GetWatching(), cmdLines)
}

func (db *DB) Exec(c redis.Connection, cmdLine [][]byte) redis.Reply {
	// transaction control commands and other commands which cannot execute within transaction
	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "multi" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return StartMulti(c)
	} else if cmdName == "discard" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return DiscardMulti(c)
	} else if cmdName == "exec" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		slog.Info("即将进入execMulti方法")
		return execMulti(db, c)
	} else if cmdName == "watch" {
		if !validateArity(-2, cmdLine) {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return Watch(db, c, cmdLine[1:])
	}
	if c != nil && c.InMultiState() {
		return EnqueueCmd(c, cmdLine)
	}

	return db.execNormalCommand(cmdLine)
}

/* ---- TTL Functions ---- */
// TTL（Time To Live）表示一个键的剩余生存时间，单位通常是秒。它的意义是：该键将在指定的秒数后自动从数据库中删除。
//  2. 防止内存无限增长
// 限流与计数器
// 如限制用户每分钟最多请求 100 次，可以为每个用户 key 设置 TTL=60。
func genExpireTask(key string) string {
	return "expire:" + key
}

// 设定ttl的键的过期时间
func (db *DB) Expire(key string, expireTime time.Time) {
	db.ttlMap.Put(key, expireTime)
	taskKey := genExpireTask(key)
	timewheel.At(expireTime, taskKey, func() {
		keys := []string{key}
		db.RWLocks(keys, nil)
		defer db.RWUnLocks(keys, nil)
		// check-lock-check, ttl may be updated during waiting lock
		slog.Info("expire " + key)
		rawExpireTime, ok := db.ttlMap.Get(key)
		if !ok {
			return
		}
		expireTime, _ := rawExpireTime.(time.Time)
		expired := time.Now().After(expireTime)
		if expired {
			db.Remove(key)
		}
	})
}

// 持久化取消TTL键
func (db *DB) Persist(key string) {
	db.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
}

// 检查密钥是否过期
func (db *DB) IsExpired(key string) bool {
	rawExpireTime, ok := db.ttlMap.Get(key)
	if !ok {
		return false
	}
	expireTime, _ := rawExpireTime.(time.Time)
	expired := time.Now().After(expireTime)
	if expired {
		db.Remove(key)
	}
	return expired
}

/* ---- Data Access ----- */
// 返回给定键的数据实体绑定
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	// 这里选用不上锁的吧
	raw, ok := db.data.Get(key)
	if !ok {
		return nil, false
	}
	if db.IsExpired(key) {
		//惰性检查，键过期了，就直接删除
		db.Remove(key)
		return nil, false
	}
	entity, _ := raw.(*database.DataEntity)
	return entity, true
}

func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	ret := db.data.Put(key, entity)
	if cb := db.insertCallback; ret > 0 && cb != nil {
		cb(db.index, key, entity)
	}
	return ret
}

// 编辑现有的数据实体
func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.data.PutIfExistsWithLock(key, entity)
}

// 只有当键不存在时才插入数据实体
func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	ret := db.data.PutIfAbsentWithLock(key, entity)
	// db.insertCallback may be set as nil, during `if` and actually callback
	// so introduce a local variable `cb`
	if cb := db.insertCallback; ret > 0 && cb != nil {
		cb(db.index, key, entity)
	}
	return ret
}

// 从数据库中删除给定的键
func (db *DB) Remove(key string) {
	raw, deleted := db.data.Remove(key)
	db.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
	if cb := db.deleteCallback; cb != nil {
		var entity *database.DataEntity
		if deleted > 0 {
			entity = raw.(*database.DataEntity)
		}
		cb(db.index, key, entity)
	}
}

// Removes the given keys from db
func (db *DB) Removes(keys ...string) (deleted int) {
	deleted = 0
	for _, key := range keys {
		_, exists := db.data.GetWithLock(key)
		if exists {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
}

/* ---- Lock Function ----- */
func (db *DB) RWLocks(writeKeys []string, readKeys []string) {
	db.data.RWLocks(writeKeys, readKeys)
}

func (db *DB) RWUnLocks(writeKeys []string, readKeys []string) {
	db.data.RWUnLocks(writeKeys, readKeys)
}

/* --- add version --- */

// 返回给定键的版本代码
func (db *DB) GetVersion(key string) uint32 {
	entity, ok := db.versionMap.Get(key)
	if !ok {
		return 0
	}
	// entity 是一个接口类型（interface{}），可以存储任何类型的值。
	// .(uint32) 表示你断言这个接口中当前存储的值是 uint32 类型。
	// 如果 entity 中存储的确是 uint32 类型的值，那么这个表达式会返回该值。
	// 如果不是，则会引发 panic。
	return entity.(uint32)
}

func (db *DB) addVersion(keys ...string) {
	for _, key := range keys {
		version := db.GetVersion(key)
		db.versionMap.Put(key, version+1)
	}
}

// 遍历数据库的每个键
func (db *DB) ForEach(cb func(key string, entity *database.DataEntity, expiration *time.Time) bool) {
	db.data.ForEach(func(key string, raw interface{}) bool {
		entity := raw.(*database.DataEntity)
		var expiration *time.Time
		rawExpireTime, ok := db.ttlMap.Get(key)
		if ok {
			expireTime, _ := rawExpireTime.(time.Time)
			expiration = &expireTime
		}

		return cb(key, entity, expiration)
	})
}

func (db *DB) Flush() {
	db.data.Clear()
	db.ttlMap.Clear()
}
