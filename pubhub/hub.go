package pubhub

import (
	"github.com/zhangming/go-redis/datastruct/dict"
	"github.com/zhangming/go-redis/datastruct/lock"
)


type Hub struct {
    subs dict.Dict 
	subsLocker *lock.Locks
}

func MakeHub() *Hub {
	return &Hub{
		subs:       dict.MakeConcurrent(4),
		subsLocker: lock.Make(16),
	}
}
