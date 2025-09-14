package database

import (
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/zhangming/go-redis/aof"
	"github.com/zhangming/go-redis/config"
	"github.com/zhangming/go-redis/interfaces/database"
	"github.com/zhangming/go-redis/interfaces/redis"
	"github.com/zhangming/go-redis/lib/utils"
	"github.com/zhangming/go-redis/pubhub"
	"github.com/zhangming/go-redis/redis/protocol"
)

var godisVersion = "1.2.8" // do not modify


type Server struct {
	dbSet []*atomic.Value // 数据库序号

	// subscribe publish
	hub *pubhub.Hub
	// handle aof persistence
	persister *aof.Persister

	// 回调函数
	insertCallback database.KeyEventCallback
	deleteCallback database.KeyEventCallback
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

// AfterClientClose does some clean after client close connection
func (server *Server) AfterClientClose(c redis.Connection) {
	pubhub.UnsubscribeAll(server.hub, c)
}

func (server *Server) Close() {
	if server.persister != nil {
		server.persister.Close()
	}
}

// 创捷sercer
func NewStandaloneServer() *Server {
	server := &Server{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	server.dbSet = make([]*atomic.Value, config.Properties.Databases)
	// 创建临时文件，防止写入aof和rdb的时候，导致失败时毁坏源文件
	err := os.MkdirAll(config.GetTmpDir(), os.ModePerm)
	if err != nil {
		slog.Error("mkdir failed", "path", config.GetTmpDir(), "error", err)
	}
	for i := range server.dbSet {
		singleDB := makeBasicDB()
		singleDB.index = i
		holder := &atomic.Value{}
		holder.Store(singleDB)
		server.dbSet[i] = holder
	}
	validAof := false
	if config.Properties.AppendOnly {
		validAof = fileExists(config.Properties.AppendFilename)
		aofHandler, err := NewPersister(server,
			config.Properties.AppendFilename, true, config.Properties.AppendFsync)
		if err != nil {
			panic(err)
		}
		server.bindPersister(aofHandler)
	}
	if config.Properties.RDBFilename != "" && !validAof {
		// load rdb
		err := server.loadRdbFile()
		if err != nil {
			slog.Error("err",err)
		}
	}

	return server
}

func (server *Server) selectDB(dbIndex int) (*DB, *protocol.StandardErrReply) {
	if dbIndex > len(server.dbSet) || dbIndex < 0 {
		return nil, protocol.MakeErrReply("ERR invalid DB index")
	}
	return server.dbSet[dbIndex].Load().(*DB), nil
}

func (server *Server) mustSelectDB(dbIndex int) *DB {
	db, errReply := server.selectDB(dbIndex)
	if errReply != nil {
		panic(errReply.Error())
	}
	return db
}

func (server *Server) GetEntity(dbIndex int, key string) (*database.DataEntity, bool) {
	return server.mustSelectDB(dbIndex).GetEntity(key)
}
func (server *Server) GetExpiration(dbIndex int, key string) *time.Time {
	raw, ok := server.mustSelectDB(dbIndex).ttlMap.Get(key)
	if !ok {
		return nil
	}
	expireTime, _ := raw.(time.Time)
	return &expireTime
}

func (server *Server) GetUndoLogs(dbIndex int, cmdLine [][]byte) []CmdLine {
	return server.mustSelectDB(dbIndex).GetUndoLogs(cmdLine)
}

// RWLocks lock keys for writing and reading
func (server *Server) RWLocks(dbIndex int, writeKeys []string, readKeys []string) {
	server.mustSelectDB(dbIndex).RWLocks(writeKeys, readKeys)
}

// RWUnLocks unlock keys for writing and reading
func (server *Server) RWUnLocks(dbIndex int, writeKeys []string, readKeys []string) {
	server.mustSelectDB(dbIndex).RWUnLocks(writeKeys, readKeys)
}

func (server *Server) GetDBSize(dbIndex int) (int, int) {
	db := server.mustSelectDB(dbIndex)
	return db.data.Len(), db.ttlMap.Len()
}

func (server *Server) ForEach(dbIndex int, cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	server.mustSelectDB(dbIndex).ForEach(cb)
}

// 重载当前的数据库，新的直接覆盖旧的
func (server *Server) loadDB(dbIndex int, newDB *DB) redis.Reply {
	if dbIndex < 0 || dbIndex >= len(server.dbSet) {
		return protocol.MakeErrReply("ERR invalid DB index")
	}
	newDB.index = dbIndex
	oldDB := server.mustSelectDB(dbIndex)
	newDB.addAof = oldDB.addAof
	server.dbSet[dbIndex].Store(newDB)
	return protocol.MakeOkReply()
}

// 清空当前选中的数据库中所有的键值对
func (server *Server) FlushDB(dbIndex int) redis.Reply {
	if dbIndex < 0 || dbIndex >= len(server.dbSet) {
		return protocol.MakeErrReply("ERR invalid DB index")
	}
	newDB := makeBasicDB()
	server.loadDB(dbIndex, newDB)
	return protocol.MakeOkReply()
}

// flushAll flushes all databases.
func (server *Server) flushAll() redis.Reply {
	for i := range server.dbSet {
		server.FlushDB(i)
	}
	if server.persister != nil {
		server.persister.SaveCmdLine(0, utils.ToCmdLine("FlushAll"))
	}
	return &protocol.OkReply{}
}

// AOF 持久化机制通常是通过追加写入操作日志到文件中，而不是像 RDB 那样生成全量快照。
// 所以只用写saveRDB 而不用写saveAOF

// 这个函数并不是用于 AOF 文件本身，而是用于生成 RDB 快照文件，只是这个模块的实现方式是基于 AOF 的“Rewrite”机制和“RDB Preamble”技术。
func (server *Server) SaveRDB() redis.Reply {
	if server.persister == nil {
		return protocol.MakeErrReply("ERR no AOF persistence")
	}
	rdbName := config.Properties.RDBFilename
	if rdbName == "" {
		rdbName = "dump.rdb"
	}
	err := server.persister.GenerateRDB(rdbName)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	return protocol.MakeOkReply()
}

// 这里的saveRDB是异步写入
func (server *Server) BGSaveRDB() redis.Reply {
	if server.persister == nil {
		return protocol.MakeErrReply("ERR no AOF persistence")
	}
	go func() {
		defer func() {
			if err := recover(); err != nil {
				slog.Error("Err", err)
			}
		}()
		rdbFilename := config.Properties.RDBFilename
		if rdbFilename == "" {
			rdbFilename = "dump.rdb"
		}
		err := server.persister.GenerateRDB(rdbFilename)
		if err != nil {
			slog.Error("err", err)
		}
	}()
	return protocol.MakeStatusReply("Background saving started")
}

// BGRewriteAOF asynchronously rewrites Append-Only-File
func BGRewriteAOF(db *Server, args [][]byte) redis.Reply {
	go db.persister.Rewrite()
	return protocol.MakeStatusReply("Background append only file rewriting started")
}

// （用更少命令重建 AOF）
func RewriteAOF(db *Server, args [][]byte) redis.Reply {
	err := db.persister.Rewrite()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	return protocol.MakeOkReply()
}


func (server *Server) SetKeyInsertedCallback(cb database.KeyEventCallback) {
	server.insertCallback = cb
	for i := range server.dbSet {
		db := server.mustSelectDB(i)
		db.insertCallback = cb
	}

}

func (server *Server) SetKeyDeletedCallback(cb database.KeyEventCallback) {
	server.deleteCallback = cb
	for i := range server.dbSet {
		db := server.mustSelectDB(i)
		db.deleteCallback = cb
	}
}



// ExecMulti executes multi commands transaction Atomically and Isolated
func (server *Server) ExecMulti(conn redis.Connection, watching map[string]uint32, cmdLines []CmdLine) redis.Reply {
	selectedDB, errReply := server.selectDB(conn.GetDBIndex())
	if errReply != nil {
		return errReply
	}
	return selectedDB.ExecMulti(conn, watching, cmdLines)
}

// ExecWithLock executes normal commands, invoker should provide locks
func (server *Server) ExecWithLock(conn redis.Connection, cmdLine [][]byte) redis.Reply {
	db, errReply := server.selectDB(conn.GetDBIndex())
	if errReply != nil {
		return errReply
	}
	return db.execWithLock(cmdLine)
}

// 在执行 FlushDB（清空当前数据库）操作时，同时记录该操作到持久化日志（AOF）中
func (server *Server) execFlushDB(dbIndex int) redis.Reply {
	if server.persister != nil {
		server.persister.SaveCmdLine(dbIndex, utils.ToCmdLine("FlushDB"))
	}
	return server.FlushDB(dbIndex)
}

// 在执行select的时候，把操作记录到持久化日志中
func execSelect(c redis.Connection, mdb *Server, args [][]byte) redis.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return protocol.MakeErrReply("ERR invalid DB index")
	}
	if dbIndex >= len(mdb.dbSet) || dbIndex < 0 {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}
	// 设置该客户端当前使用的数据库索引（保存在连接状态中）
	// 不用直接选择实体，选择实体的话，那就是读写数据了
	c.SelectDB(dbIndex)
	return protocol.MakeOkReply()
}

func (server *Server) Exec(c redis.Connection, cmdLine [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			slog.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &protocol.UnknownErrReply{}
		}
	}()

	cmdName := strings.ToLower(string(cmdLine[0]))
	// ping
	if cmdName == "ping" {
		return Ping(c, cmdLine[1:])
	}
	// authenticate
	if cmdName == "auth" {
		return Auth(c, cmdLine[1:])
	}

	// info
	if cmdName == "info" {
		return Info(server, cmdLine[1:])
	}
	if cmdName == "dbsize" {
		return DbSize(c, server)
	}

	// special commands which cannot execute within transaction
	if cmdName == "subscribe" {
		if len(cmdLine) < 2 {
			return protocol.MakeArgNumErrReply("subscribe")
		}
		return pubhub.Subscribe(server.hub, c, cmdLine[1:])
	} else if cmdName == "publish" {
		return pubhub.Publish(server.hub, cmdLine[1:])
	} else if cmdName == "unsubscribe" {
		return pubhub.UnSubscribe(server.hub, c, cmdLine[1:])
	} else if cmdName == "bgrewriteaof" {
		if !config.Properties.AppendOnly {
			return protocol.MakeErrReply("AppendOnly is false, you can't rewrite aof file")
		}
		// aof.go imports router.go, router.go cannot import BGRewriteAOF from aof.go
		return BGRewriteAOF(server, cmdLine[1:])
	} else if cmdName == "rewriteaof" {
		if !config.Properties.AppendOnly {
			return protocol.MakeErrReply("AppendOnly is false, you can't rewrite aof file")
		}
		return RewriteAOF(server, cmdLine[1:])
	} else if cmdName == "flushall" {
		return server.flushAll()
	} else if cmdName == "flushdb" {
		if !validateArity(1, cmdLine) {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		if c.InMultiState() {
			return protocol.MakeErrReply("ERR command 'FlushDB' cannot be used in MULTI")
		}
		return server.execFlushDB(c.GetDBIndex())
	} else if cmdName == "save" {
		return server.SaveRDB()
	} else if cmdName == "bgsave" {
		return server.BGSaveRDB()
	} else if cmdName == "select" {
		if c != nil && c.InMultiState() {
			return protocol.MakeErrReply("cannot select database within multi")
		}
		if len(cmdLine) != 2 {
			return protocol.MakeArgNumErrReply("select")
		}
		return execSelect(c, server, cmdLine[1:])
	}

	// normal commands
	dbIndex := c.GetDBIndex()
	selectedDB, errReply := server.selectDB(dbIndex)
	if errReply != nil {
		return errReply
	}
	return selectedDB.Exec(c, cmdLine)
}
