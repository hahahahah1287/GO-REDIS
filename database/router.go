package database

import (
	"strings"

	"github.com/zhangming/go-redis/interfaces/redis"
	"github.com/zhangming/go-redis/redis/protocol"
)

// 这些都是大致命令的方法定义，到时候在每个具体类中，会有其具体实现逻辑的

// 为某个命令生成“反向操作”命令列表，用于实现 AOF 回滚或事务失败时的撤销。
type UndoFunc func(db *DB, args [][]byte) []CmdLine

// 这是 Redis 命令的实际执行函数。
type ExecFunc func(db *DB, args [][]byte) redis.Reply

// 在将命令加入事务队列前，提前分析该命令涉及的读写 key。
// ExecFunc 前执行，负责分析命令行读写了哪些 key 便于进行加锁
type PreFunc func(args [][]byte) ([]string, []string)

type command struct {
	name     string
	executor ExecFunc
	prepare  PreFunc
	undo     UndoFunc
	arity    int           //参数个数要求：<br>正数表示固定参数个数
	flags    int           //命令标志位，如只读、写操作等
	extra    *commandExtra //扩展信息，用于集群或 Lua 脚本中提取 keys
}

type commandExtra struct {
	signs    []string // 签名
	firstKey int      //第一个key的索引位置
	lastKey  int      //最后一个key的索引位置
	keyStep  int      //key的步长
}

const flagWrite = 0 //默认为读操作

const (
	flagReadOnly = 1 << iota
	flagSpecial  // 特殊命令，只能在事务中使用
)

// redis的命令表，全局注册
var cmdTable = make(map[string]*command)

func registerCommand(name string, executor ExecFunc, prepare PreFunc, rollback UndoFunc, arity int, flags int) *command {
	name = strings.ToLower(name)
	cmd := &command{
		name:     name,
		executor: executor,
		prepare:  prepare,
		undo:     rollback,
		arity:    arity,
		flags:    flags,
	}
	cmdTable[name] = cmd
	return cmd
}

// 特殊命令的注册
func registerSpecialCommand(name string, arity int, flags int) *command {
	name = strings.ToLower(name)
	flags |= flagSpecial
	cmd := &command{
		name:  name,
		arity: arity,
		flags: flags,
	}
	cmdTable[name] = cmd
	return cmd
}

func (cmd *command) attachCommandExtra(signs []string, firstKey int, lastKey int, keyStep int) {
	cmd.extra = &commandExtra{
		signs:    signs,
		firstKey: firstKey,
		lastKey:  lastKey,
		keyStep:  keyStep,
	}
}

// 将一个命令（command 结构体）转换为 Redis 客户端可识别的响应格式（redis.Reply 类型），用于描述该命令的相关信息。
func (cmd *command) toDescReply() redis.Reply {
	args := make([]redis.Reply, 0, 6)
	args = append(args,
		protocol.MakeBulkReply([]byte(cmd.name)),
		protocol.MakeIntReply(int64(cmd.arity)))
	if cmd.extra != nil {
		signs := make([][]byte, len(cmd.extra.signs))
		for i, v := range cmd.extra.signs {
			signs[i] = []byte(v)
		}
		args = append(args,
			protocol.MakeMultiBulkReply(signs),
			protocol.MakeIntReply(int64(cmd.extra.firstKey)),
			protocol.MakeIntReply(int64(cmd.extra.lastKey)),
			protocol.MakeIntReply(int64(cmd.extra.keyStep)),
		)
	}
	return protocol.MakeMultiRawReply(args)
}
