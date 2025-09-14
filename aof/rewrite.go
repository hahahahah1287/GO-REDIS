package aof

import (
	"io"
	"log/slog"
	"os"
	"strconv"

	"github.com/zhangming/go-redis/config"
	"github.com/zhangming/go-redis/lib/utils"
	"github.com/zhangming/go-redis/redis/protocol"
)

// RewriteCtx holds context of an AOF rewriting procedure
type RewriteCtx struct {
	tmpFile  *os.File // tmpFile is the file handler of aof tmpFile.
	// 在 AOF 重写过程中，可以基于 fileSize 判断是否超过限制（如 auto-aof-rewrite-size）。
	fileSize int64
	dbIdx    int // selected db index when startRewrite
}

func (persister *Persister) newRewriteHandler() *Persister {
	h := &Persister{}
	h.aofFilename = persister.aofFilename
	h.db = persister.tmpDBMaker()
	return h
}

// Rewrite carries out AOF rewrite
func (persister *Persister) Rewrite() error {
	ctx, err := persister.StartRewrite()
	if err != nil {
		return err
	}
	err = persister.DoRewrite(ctx)
	if err != nil {
		return err
	}

	persister.FinishRewrite(ctx)
	return nil
}

func (persister *Persister) DoRewrite(ctx *RewriteCtx) (err error) {
	// start rewrite
	if !config.Properties.AofUseRdbPreamble {
		slog.Info("generate aof preamble")
		err = persister.generateAof(ctx)
	} else {
		slog.Info("generate rdb preamble")
		err = persister.generateRDB(ctx)
	}
	return err
}

func (persister *Persister) StartRewrite() (*RewriteCtx, error) {
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
    err := persister.aofFile.Sync()
	if err != nil {
		slog.Error("sync aof file error", "error", err)
		return nil, err
	}
	fileInfo, _ := os.Stat(persister.aofFilename)
	filesize := fileInfo.Size()

    file, err := os.CreateTemp(config.GetTmpDir(), "*.aof")
	if err != nil {
		slog.Error("create temp file error", "error", err)
		return nil, err
	}
	return &RewriteCtx{
		dbIdx: persister.currentDB,
		fileSize: filesize,
		tmpFile: file,
	}, nil
}

func (persister *Persister) FinishRewrite (ctx *RewriteCtx) { 
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
	tmpFile := ctx.tmpFile

	// 定位最后写到的位置
	src,err := os.Open(persister.aofFilename)
	if err != nil {
		slog.Error("open aof file error", "error", err)
		return
	}
	defer func ()  {
		src.Close()
	}()
    
	_, err = src.Seek(ctx.fileSize, 0)
	if err != nil {
		slog.Error("seek aof file error", "error", err)
		return
	}

	// 写入一条 Select 命令，使 tmpAof 选中重写开始时刻线上 aof 文件选中的数据库
    data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(ctx.dbIdx))).ToBytes()
	// AOF 文件记录的是所有数据库操作命令，包括当前选中的数据库（通过 SELECT <dbindex> 指定）。
	// tmpFile 是最终 AOF 的一部分
	_, err = tmpFile.Write(data)
	if err != nil {
		slog.Error("tmp file rewrite failed: " + err.Error())
		return
	}
	// 对齐数据库后就可以把重写过程中产生的数据复制到 tmpAof 文件了
	_,err = io.Copy(tmpFile, src)
	if err != nil {
		slog.Error("tmp file copy failed: " + err.Error())
		return
	}
	_ = persister.aofFile.Close()
	// 直接改名
	if err := os.Rename(tmpFile.Name(), persister.aofFilename); err != nil {
		slog.Error("",err)
	}
	// reopen aof file for further write
	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	persister.aofFile = aofFile

	// write select command again to resume aof file selected db
	// it should have the same db index with  persister.currentDB
	data = protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(persister.currentDB))).ToBytes()
	_, err = persister.aofFile.Write(data)
	if err != nil {
		panic(err)
	}

}


