package aof

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	rdb "github.com/hdt3213/rdb/core"

	"github.com/zhangming/go-redis/config"
	"github.com/zhangming/go-redis/interfaces/database"
	"github.com/zhangming/go-redis/interfaces/redis/parser"
	"github.com/zhangming/go-redis/lib/utils"
	"github.com/zhangming/go-redis/redis/connection"
	"github.com/zhangming/go-redis/redis/protocol"
)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

const (
	aofQueueSize = 1 << 20
)

const (
	// FsyncAlways do fsync for every command
	FsyncAlways = "always"
	// FsyncEverySec do fsync every second
	FsyncEverySec = "everysec"
	// FsyncNo lets operating system decides when to do fsync
	FsyncNo = "no"
)

type payload struct {
	cmdLine CmdLine
	// 记录数据库序号
	dbIndex int
	wg      *sync.WaitGroup
}

// Listener will be called-back after receiving a aof payload
// with a listener we can forward the updates to slave nodes etc.
type Listener interface {
	// Callback will be called-back after receiving a aof payload
	Callback([]CmdLine)
}

// Persister receive msgs from channel and write to AOF file
type Persister struct {
	// 控制后台 AOF 写入协程的生命周期
	ctx    context.Context
	cancel context.CancelFunc
	db     database.DBEngine
	// 创建临时数据库实例的函数。
	tmpDBMaker func() database.DBEngine
	// aofChan is the channel to receive aof payload(listenCmd will send payload to this channel)
	//接收来自客户端的 AOF 写入任务
	aofChan chan *payload
	// aofFile is the file handler of aof file
	aofFile *os.File
	// aofFilename is the path of aof file
	aofFilename string
	// aofFsync is the strategy of fsync
	//配置 AOF 的同步策略（fsync），决定何时将内存中的 AOF 缓冲刷到磁盘。
	aofFsync string
	// aof goroutine will send msg to main goroutine through this channel when aof tasks finished and ready to shut down
	//用于通知主线程 AOF 模块已经完成清理并退出。
	aofFinished chan struct{}
	// pause aof for start/finish aof rewrite progress
	//互斥锁，用于在 AOF 重写（rewrite）期间暂停正常的 AOF 写入。
	pausingAof sync.Mutex
	currentDB  int
	//注册监听器（回调函数），当 AOF 有事件发生时通知这些监听者。
	listeners map[Listener]struct{}
	// reuse cmdLine buffer
	buffer []CmdLine
}

func NewPersister(db database.DBEngine, filename string, load bool, fsync string, tmpDBMaker func() database.DBEngine) (*Persister, error) {
	persister := &Persister{}
	persister.db = db
	persister.tmpDBMaker = tmpDBMaker
	persister.aofFilename = filename
	persister.aofFsync = strings.ToLower(fsync)
	persister.currentDB = 0
	if load {
		// 这一行调用 LoadAof(0) 的作用是 从 AOF 文件中加载持久化的命令数据到内存数据库中，通常在 Redis 启动时执行
		// 这是为了恢复上次关闭服务前保存的数据状态，确保重启后数据不会丢失（前提是开启了 AOF 持久化）
		persister.LoadAof(0)
	}
	// os.O_APPEND	写入时始终追加到文件末尾
	// os.O_CREATE	如果文件不存在，则创建它
	// os.O_RDWR	以读写模式打开文件
	// 0600	文件权限：所有者可读写，其他用户无权限
	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	persister.aofFile = aofFile
	persister.aofChan = make(chan *payload, aofQueueSize)
	persister.aofFinished = make(chan struct{})
	persister.listeners = make(map[Listener]struct{})
	go func() {
		persister.listenCmd()
	}()
	ctx, cancel := context.WithCancel(context.Background())
	persister.cancel = cancel
	persister.ctx = ctx
	if persister.aofFsync == FsyncEverySec {
		persister.fsyncEverySecond()
	}
	return persister, nil

}

func (persister *Persister) RemoveListener(Listener Listener) {
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
	delete(persister.listeners, Listener)
}

// listenCmd listen aof channel and write into file
func (persister *Persister) listenCmd() {
	for p := range persister.aofChan {
		// 这里写入了
		persister.writeAof(p)
	}
	persister.aofFinished <- struct{}{}
}

func (persister *Persister) SaveCmdLine(db int, cmdLine CmdLine) {
	if persister.aofChan == nil {
		return
	}
	// FsyncAlways 需要立即写入磁盘，不能等待后台异步处理。
	if persister.aofFsync == FsyncAlways {
		p := &payload{
			dbIndex: db,
			cmdLine: cmdLine,
		}
		persister.writeAof(p)
		return
	}
	persister.aofChan <- &payload{
		cmdLine: cmdLine,
		dbIndex: db,
	}
}

func (persister *Persister) writeAof(p *payload) {
	// 它的作用是清空一个切片（slice）的内容，但保留底层的数组内存空间，以便后续复用。这样做的目的是：

	// 减少频繁创建和释放内存带来的性能损耗。
	persister.buffer = persister.buffer[:0] // reuse underlying array
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
	if persister.currentDB != p.dbIndex {
		//查找数据库
		selectCmd := utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))
		persister.buffer = append(persister.buffer, selectCmd)
		data := protocol.MakeMultiBulkReply(selectCmd).ToBytes()
		_, err := persister.aofFile.Write(data)
		if err != nil {
			slog.Error("err ", err)
			return // skip this command
		}
		persister.currentDB = p.dbIndex
	}
	//执行写入
	data := protocol.MakeMultiBulkReply(p.cmdLine).ToBytes()
	persister.buffer = append(persister.buffer, p.cmdLine)
	_, err := persister.aofFile.Write(data)
	if err != nil {
		slog.Error("err", err)
	}
	for listener := range persister.listeners {
		listener.Callback(persister.buffer)
	}
	if persister.aofFsync == FsyncAlways {
		// /调用该方法会将文件缓冲区中的数据 强制刷新到磁盘，确保数据不会因为程序崩溃而丢失。
		_ = persister.aofFile.Sync()
	}
}

func (persister *Persister) LoadAof(maxBytes int) {
	aofChan := persister.aofChan
	persister.aofChan = nil
	defer func(aofChan chan *payload) {
		persister.aofChan = aofChan
	}(aofChan)

	file, err := os.Open(persister.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		slog.Error("load aof error", err)
		return
	}
	defer file.Close()
	// load rdb preamble if needed
	decoder := rdb.NewDecoder(file)
	err = persister.db.LoadRDB(decoder)
	if err != nil {
		// no rdb preamble
		file.Seek(0, io.SeekStart)
	} else {
		// has rdb preamble
		_, _ = file.Seek(int64(decoder.GetReadCount())+1, io.SeekStart)
		maxBytes = maxBytes - decoder.GetReadCount()
	}
	var reader io.Reader
	if maxBytes > 0 {
		reader = io.LimitReader(file, int64(maxBytes))
	} else {
		reader = file
	}
	ch := parser.ParseStream(reader)
	fakeConn := connection.NewFakeConn() // only used for save dbIndex
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			slog.Error("parse error: " + p.Err.Error())
			continue
		}
		if p.Data == nil {
			slog.Error("empty payload")
			continue
		}
		r, ok := p.Data.(*protocol.MultiBulkReply)
		if !ok {
			slog.Error("require multi bulk protocol")
			continue
		}
		ret := persister.db.Exec(fakeConn, r.Args)
		if protocol.IsErrorReply(ret) {
			slog.Error("exec err", string(ret.ToBytes()))
		}
		if strings.ToLower(string(r.Args[0])) == "select" {
			// execSelect success, here must be no error
			dbIndex, err := strconv.Atoi(string(r.Args[1]))
			if err == nil {
				persister.currentDB = dbIndex
			}
		}
	}
}

// 手动刷盘
func (persister *Persister) Fsync() {
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
	if err := persister.aofFile.Sync(); err != nil {
		slog.Error("aof sync error", "error", err)
	}
}

func (persister *Persister) Close() {
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
	// aofFile 是指向 AOF 日志文件的指针，如果它为 nil，说明 AOF 持久化功能未被启用或尚未初始化。
	// 因此，通过判断 aofFile != nil 可以避免对未初始化的对象执行操作（如关闭通道、关闭文件等），防止空指针 panic。
	if persister.aofFile != nil {
		close(persister.aofChan)
		<-persister.aofFinished
		err := persister.aofFile.Close()
		if err != nil {
			slog.Error("aof close error", "error", err)
		}
	}
	persister.cancel()
}

// fsyncEverySecond fsync aof file every second
func (persister *Persister) fsyncEverySecond() {
	ticker := time.NewTicker(time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				persister.Fsync()
			case <-persister.ctx.Done():
				return
			}
		}
	}()
}

func (persister *Persister) generateAof(ctx *RewriteCtx) error {
	tmpFile := ctx.tmpFile
	tmpAof := persister.newRewriteHandler()
	tmpAof.LoadAof(int(ctx.fileSize))
	for i := 0; i < config.Properties.Databases; i++ {
		// 选择数据库
		data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(i))).ToBytes()
		_, err := tmpFile.Write(data)
		if err != nil {
			return err
		}
		// 循环写入每个键值对是为了完整重建数据库状态
		// 重写 AOF 时需要将当前数据库中的每一个 key-value 对转换为等价的 Redis 命令（如 SET, HSET, SADD 等），并逐条写入到临时 AOF 文件中。
		persister.db.ForEach(i, func(key string, entity *database.DataEntity, expiration *time.Time) bool {
			cmd := EntityToCmd(key, entity)
			if cmd != nil {
				_, _ = tmpFile.Write(cmd.ToBytes())
			}
			if expiration != nil {
				cmd := MakeExpireCmd(key, *expiration)
				if cmd != nil {
					_, _ = tmpFile.Write(cmd.ToBytes())
				}
			}
			return true
		})
	}
	return nil
}
