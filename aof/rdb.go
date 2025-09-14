package aof

import (
	"os"
	"strconv"
	"time"

	rdb "github.com/hdt3213/rdb/encoder"
	"github.com/hdt3213/rdb/model"
	"github.com/zhangming/go-redis/config"
	"github.com/zhangming/go-redis/datastruct/dict"
	"github.com/zhangming/go-redis/datastruct/list"
	"github.com/zhangming/go-redis/datastruct/set"
	"github.com/zhangming/go-redis/datastruct/sortedset"
	"github.com/zhangming/go-redis/interfaces/database"
)

// 它既可以用于 AOF Rewrite 时写入 RDB 前缀，也可以用于生成完整的 RDB 快照文件。

func (persister *Persister) generateRDB(ctx *RewriteCtx) error {
	// 命令写入aof
	tmpHandler := persister.newRewriteHandler()
	tmpHandler.LoadAof(int(ctx.fileSize))

	encoder := rdb.NewEncoder(ctx.tmpFile).EnableCompress()
	err := encoder.WriteHeader()
	if err != nil {
		return err
	}

	// 5. 准备辅助字段（AUX fields）
	//    这些是存储在 RDB 文件中的元数据。
	auxMap := map[string]string{
		"redis-ver":    "6.0.0",                                  // Redis 版本
		"redis-bits":   "64",                                     // 操作系统位数
		"aof-preamble": "0",                                      // AOF 序言标志，默认为 0
		"ctime":        strconv.FormatInt(time.Now().Unix(), 10), // 创建时间戳
	}

	// 6. 根据配置决定是否开启 AOF 序言
	//    如果配置了 AofUseRdbPreamble，这个 RDB 文件可以作为混合持久化 AOF 文件的头部。
	if config.Properties.AofUseRdbPreamble {
		auxMap["aof-preamble"] = "1"
	}

	// 写入rdb配置信息
	for key, value := range auxMap {
		err := encoder.WriteAux(key, value)
		if err != nil {
			return err
		}
	}

	for i := 0; i < config.Properties.Databases; i++ {
		keyCount, ttlCount := tmpHandler.db.GetDBSize(i)
		if keyCount == 0 {
			continue
		}
		err = encoder.WriteDBHeader(uint(i), uint64(keyCount), uint64(ttlCount))
		if err != nil {
			return err
		}
		// dump db
		var err2 error
		tmpHandler.db.ForEach(i, func(key string, entity *database.DataEntity, expiration *time.Time) bool {
			var opts []interface{}
			if expiration != nil {
				opts = append(opts, rdb.WithTTL(uint64(expiration.UnixNano()/1e6)))
			}
			switch obj := entity.Data.(type) {
			case []byte:
				err = encoder.WriteStringObject(key, obj, opts...)
			case list.List:
				vals := make([][]byte, 0, obj.Len())
				obj.ForEach(func(i int, v interface{}) bool {
					bytes, _ := v.([]byte)
					vals = append(vals, bytes)
					return true
				})
				err = encoder.WriteListObject(key, vals, opts...)
			case *set.Set:
				vals := make([][]byte, 0, obj.Len())
				obj.ForEach(func(m string) bool {
					vals = append(vals, []byte(m))
					return true
				})
				err = encoder.WriteSetObject(key, vals, opts...)
			case dict.Dict:
				hash := make(map[string][]byte)
				obj.ForEach(func(key string, val interface{}) bool {
					bytes, _ := val.([]byte)
					hash[key] = bytes
					return true
				})
				err = encoder.WriteHashMapObject(key, hash, opts...)
			case *sortedset.SortedSet:
				var entries []*model.ZSetEntry
				obj.ForEachByRank(int64(0), obj.Len(), true, func(element *sortedset.Element) bool {
					entries = append(entries, &model.ZSetEntry{
						Member: element.Member,
						Score:  element.Score,
					})
					return true
				})
				err = encoder.WriteZSetObject(key, entries, opts...)
			}
			if err != nil {
				err2 = err
				return false
			}
			return true
		})
		if err2 != nil {
			return err2
		}
	}
	err = encoder.WriteEnd()
	if err != nil {
		return err
	}
	return nil
}

func (persister *Persister) startGenerateRDB(newListener Listener, hook func()) (*RewriteCtx, error) {
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()

	err := persister.aofFile.Sync()
	if err != nil {
		return nil, err
	}
	fileInfo, _ := os.Stat(persister.aofFilename)
	filesize := fileInfo.Size()
	//在 GenerateRDB 这个函数执行期间，往临时文件里写入的内容格式是 RDB。
	// 这里相当于直接按照混合形式来写的
	file, err := os.CreateTemp(config.GetTmpDir(), "*.aof")
	if err != nil {
		return nil, err
	}
	if newListener != nil {

	}
	if hook != nil {
		hook()
	}
	return &RewriteCtx{
		fileSize: filesize,
		tmpFile:  file,
	}, nil
}

func (persister *Persister) GenerateRDB(rdbFilename string) error {
	// 1. 调用 startGenerateRDB 进行准备工作
	//    传入 nil, nil 表示不需要为复制设置监听器（listener）或钩子函数（hook）。
	ctx, err := persister.startGenerateRDB(nil, nil)
	if err != nil {
		// 如果准备阶段出错，直接返回错误。
		return err
	}

	// 2. 调用 generateRDB 执行核心的 RDB 文件生成逻辑
	//    它会把数据写入 ctx.tmpFile（一个临时文件）中。
	err = persister.generateRDB(ctx)
	if err != nil {
		// 如果生成过程中出错，直接返回错误。
		return err
	}

	// 3. 关闭临时文件
	//    确保所有写入的数据都已刷到磁盘。
	err = ctx.tmpFile.Close()
	if err != nil {
		return err
	}

	// 4. 将临时文件重命名为最终的目标文件名
	//    os.Rename 是一个原子操作（在大多数文件系统上）。
	//    这样做的好处是：如果在生成过程中失败，旧的 RDB 文件（如果存在）不会被破坏。
	//    只有当新的 RDB 文件完全成功生成后，才会瞬间替换掉旧文件。
	err = os.Rename(ctx.tmpFile.Name(), rdbFilename)
	if err != nil {
		return err
	}

	// 5. RDB 文件生成成功，返回 nil 表示没有错误。
	return nil
}
