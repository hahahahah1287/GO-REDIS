package pubhub

import (

	"strconv"

	"github.com/zhangming/go-redis/datastruct/list"
	"github.com/zhangming/go-redis/interfaces/redis"
	"github.com/zhangming/go-redis/lib/utils"
	"github.com/zhangming/go-redis/redis/protocol"
)

var (
	_subscribe         = "subscribe"
	_unsubscribe       = "unsubscribe"
	messageBytes       = []byte("message")
	unSubscribeNothing = []byte("*3\r\n$11\r\nunsubscribe\r\n$-1\n:0\r\n")
)

func makeMsg(t string, channel string, code int64) []byte {
	return []byte("*3\r\n$" + strconv.FormatInt(int64(len(t)), 10) + protocol.CRLF + t + protocol.CRLF +
		"$" + strconv.FormatInt(int64(len(channel)), 10) + protocol.CRLF + channel + protocol.CRLF +
		":" + strconv.FormatInt(code, 10) + protocol.CRLF)
}

// 发布订阅信息给客户端
func Publish(hub *Hub, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("publish")
	}
	//发布消息的目标，也是订阅者监听的对象
	channel := string(args[0])
	message := args[1]

	hub.subsLocker.Lock(channel)
	defer hub.subsLocker.UnLock(channel)

	raw, ok := hub.subs.Get(channel)
	if !ok {
		return protocol.MakeIntReply(0)
	}
	subscribers, _ := raw.(*list.LinkedList)
	subscribers.ForEach(func(i int, c interface{}) bool {
		client, _ := c.(redis.Connection)
		replyArgs := make([][]byte, 3)
		replyArgs[0] = messageBytes
		replyArgs[1] = []byte(channel)
		replyArgs[2] = message
		_, _ = client.Write(protocol.MakeMultiBulkReply(replyArgs).ToBytes())
		return true
	})
	return protocol.MakeIntReply(int64(subscribers.Len()))
}

// 这里的topic是指代主题
func subscribe0(client redis.Connection, topic string, hub *Hub) bool {
	client.Subscribe(topic)
	raw, ok := hub.subs.Get(topic)
	var subscribers *list.LinkedList
	if ok {
		subscribers, _ = raw.(*list.LinkedList)
	} else {
		subscribers = list.Make()
		hub.subs.Put(topic, subscribers)
	}
	if subscribers.Contains(func(a interface{}) bool {
		return a == client
	}) {
		return false
	}
	subscribers.Add(client)
	return true
}

func Subscribe(hub *Hub, c redis.Connection, args [][]byte) redis.Reply {
	topics := make([]string, len(args))
	for i, b := range args {
		topics[i] = string(b)
	}

	hub.subsLocker.Locks(topics...)
	defer hub.subsLocker.UnLocks(topics...)

	for _, topic := range topics {
		if subscribe0(c, topic, hub) {
			_, _ = c.Write(makeMsg(_subscribe, topic, int64(c.SubsCount())))
		}
	}
	return &protocol.NoReply{}
}

func unSubScribe0(client redis.Connection, topic string, hub *Hub) bool {
	client.UnSubscribe(topic)
	raw, ok := hub.subs.Get(topic)
	if ok {
		subscribers, _ := raw.(*list.LinkedList)
		subscribers.RemoveAllByVal(func(a interface{}) bool {
			return utils.Equals(a, client)
		})

		if subscribers.Len() == 0 {
			// clean
			hub.subs.Remove(topic)
		}
		return true
	}
	return false
}

func UnSubscribe(hub *Hub, c redis.Connection, args [][]byte) redis.Reply {
	// 与 Redis 原生行为更贴近的是：如果未指定参数，默认取消所有订阅的 channel。
	var topics []string
	if len(args) < 1 {
		topics = c.GetChannels()
	} else {
		topics = make([]string, len(args))
		for i, b := range args {
			topics[i] = string(b)
		}
	}

	if len(topics) == 0 {
		_, _ = c.Write(unSubscribeNothing)
		return &protocol.NoReply{}
	}

	for _, topic := range topics {
		if unSubScribe0(c, topic, hub) {
			_, _ = c.Write(makeMsg(_unsubscribe, topic, int64(c.SubsCount())))
		}
	}
	return &protocol.NoReply{}
}

func UnsubscribeAll(hub *Hub, c redis.Connection) {
	// if hub == nil {
	// 	slog.Error("hub is nil")
	// 	return
	// }
	channels := c.GetChannels()

	hub.subsLocker.Locks(channels...)
	defer hub.subsLocker.UnLocks(channels...)

	for _, channel := range channels {
		unSubScribe0(c, channel, hub)
	}

}
