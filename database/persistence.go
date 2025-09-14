package database

import (
	"os"
	"sync/atomic"

	"github.com/hdt3213/rdb/core"
	rdb "github.com/hdt3213/rdb/parser"
	"github.com/zhangming/go-redis/aof"
	"github.com/zhangming/go-redis/config"
	"github.com/zhangming/go-redis/datastruct/dict"
	"github.com/zhangming/go-redis/datastruct/list"
	"github.com/zhangming/go-redis/datastruct/set"
	"github.com/zhangming/go-redis/datastruct/sortedset"
	"github.com/zhangming/go-redis/interfaces/database"
)

func MakeAuxiliaryServer() *Server {
	mdb := &Server{}
	mdb.dbSet = make([]*atomic.Value, config.Properties.Databases)
	for i := range mdb.dbSet {
		holder := &atomic.Value{}
		holder.Store(makeBasicDB())
		mdb.dbSet[i] = holder
	}
	return mdb
}

func (server *Server) bindPersister(persister *aof.Persister) {
	server.persister = persister
	// 持续写入命令
	for _, db := range server.dbSet {
		singleDB := db.Load().(*DB)
		singleDB.addAof = func(line CmdLine) {
			if config.Properties.AppendOnly { // config may be changed during runtime
				server.persister.SaveCmdLine(singleDB.index, line)
			}
		}
	}
}

func NewPersister(db database.DBEngine, filename string, load bool, fsync string) (*aof.Persister, error) {
	return aof.NewPersister(db, filename, load, fsync, func() database.DBEngine {
		return MakeAuxiliaryServer()
	})
}

func (server *Server) AddAof(dbIndex int, cmdLine CmdLine) {
	if server.persister != nil {
		server.persister.SaveCmdLine(dbIndex, cmdLine)
	}
}

func (server *Server) LoadRDB(dec *core.Decoder) error {
	return dec.Parse(func(o rdb.RedisObject) bool {
		db := server.mustSelectDB(o.GetDBIndex())
		var entity *database.DataEntity
		switch o.GetType() {
		case rdb.StringType:
			str := o.(*rdb.StringObject)
			entity = &database.DataEntity{
				Data: str.Value,
			}
		case rdb.ListType:
			listObj := o.(*rdb.ListObject)
			list := list.NewQuickList()
			for _, v := range listObj.Values {
				list.Add(v)
			}
			entity = &database.DataEntity{
				Data: list,
			}
		case rdb.HashType:
			hashObj := o.(*rdb.HashObject)
			hash := dict.MakeSimple()
			for k, v := range hashObj.Hash {
				hash.Put(k, v)
			}
			entity = &database.DataEntity{
				Data: hash,
			}
		case rdb.SetType:
			setObj := o.(*rdb.SetObject)
			set := set.Make()
			for _, mem := range setObj.Members {
				set.Add(string(mem))
			}
			entity = &database.DataEntity{
				Data: set,
			}
		case rdb.ZSetType:
			zsetObj := o.(*rdb.ZSetObject)
			zSet := sortedset.Make()
			for _, e := range zsetObj.Entries {
				zSet.Add(e.Member, e.Score)
			}
			entity = &database.DataEntity{
				Data: zSet,
			}
		}
		if entity != nil {
			db.PutEntity(o.GetKey(), entity)
			if o.GetExpiration() != nil {
				db.Expire(o.GetKey(), *o.GetExpiration())
			}
			// add to aof
			//将当前内存状态转换为命令序列
			//key: "user:1"
			// value: {name: "Alice", age: "25"}
			// 转化成
			// ["HSET", "user:1", "name", "Alice", "age", "25"]
			db.addAof(aof.EntityToCmd(o.GetKey(), entity).Args)
		}
		return true
	})
}

func (server *Server) loadRdbFile() error {
	rdbFile, err := os.Open(config.Properties.RDBFilename)
	if err != nil {
		return err

	}
	defer rdbFile.Close()
	decoder := rdb.NewDecoder(rdbFile)
	err = server.LoadRDB(decoder)
	if err != nil {
		return err
	}
	return nil
}
