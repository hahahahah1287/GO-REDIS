package database

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/zhangming/go-redis/config"
	"github.com/zhangming/go-redis/interfaces/redis"
	"github.com/zhangming/go-redis/redis/protocol"
)

func Ping(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) == 0 {
		return &protocol.PongReply{}
	} else if len(args) == 1 {
		return protocol.MakeStatusReply(string(args[0]))
	} else {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ping' command")
	}
}

func Info(db *Server, args [][]byte) redis.Reply {
	if len(args) == 0 {
		infoCommandList := [...]string{"server", "client", "cluster", "keyspace"}
		var allSection []byte
		for _, s := range infoCommandList {
			allSection = append(allSection, GenGodisInfoString(s, db)...)
		}
		return protocol.MakeBulkReply(allSection)
	} else if len(args) == 1 {
		section := strings.ToLower(string(args[0]))
		switch section {
		case "server":
			reply := GenGodisInfoString("server", db)
			return protocol.MakeBulkReply(reply)
		case "client":
			return protocol.MakeBulkReply(GenGodisInfoString("client", db))
		case "cluster":
			return protocol.MakeBulkReply(GenGodisInfoString("cluster", db))
		case "keyspace":
			return protocol.MakeBulkReply(GenGodisInfoString("keyspace", db))
		default:
			return protocol.MakeErrReply("Invalid section for 'info' command")
		}
	}
	return protocol.MakeArgNumErrReply("info")
}

func Auth(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'auth' command")
	}
	if config.Properties.RequirePass == "" {
		return protocol.MakeErrReply("ERR Client sent AUTH, but no password is set")
	}
	password := string(args[0])
	c.SetPassword(password)
	if config.Properties.RequirePass != password {
        return protocol.MakeErrReply("ERR invalid password")
	}
	return protocol.MakeOkReply()
}
func isAuthenticated(c redis.Connection) bool {
	if config.Properties.RequirePass == "" {
		return true
	}
	return c.GetPassword() == config.Properties.RequirePass
}
func DbSize(c redis.Connection, db *Server) redis.Reply {
	keys, _ := db.GetDBSize(c.GetDBIndex())
	return protocol.MakeIntReply(int64(keys))
}

func GenGodisInfoString(section string, db *Server) []byte {
	startUpTimeFromNow := getGodisRuninngTime()
	switch section {
	case "server":
		s := fmt.Sprintf("# Server\r\n"+
			"godis_version:%s\r\n"+
			"godis_mode:%s\r\n"+
			"os:%s %s\r\n"+
			"arch_bits:%d\r\n"+
			"go_version:%s\r\n"+
			"process_id:%d\r\n"+
			"run_id:%s\r\n"+
			"tcp_port:%d\r\n"+
			"uptime_in_seconds:%d\r\n"+
			"uptime_in_days:%d\r\n"+
			"config_file:%s\r\n",
			godisVersion,
			getGodisRunningMode(),
			runtime.GOOS, runtime.GOARCH,
			32<<(^uint(0)>>63),
			//TODO,
			runtime.Version(),
			os.Getpid(),
			config.Properties.RunID,
			config.Properties.Port,
			startUpTimeFromNow,
			startUpTimeFromNow/time.Duration(3600*24),
			config.GetConfigFilePath())
		return []byte(s)
	case "client":
		s := fmt.Sprintf("# Clients\r\n" +
			"connected_clients:%d\r\n",
		//"client_recent_max_input_buffer:%d\r\n"+
		//"client_recent_max_output_buffer:%d\r\n"+
		//"blocked_clients:%d\n",
		)
		return []byte(s)

	}
	return []byte("")
}

func getGodisRunningMode() string {
	if config.Properties.ClusterEnable {
		return config.ClusterMode
	} else {
		return config.StandaloneMode
	}
}

func getGodisRuninngTime() time.Duration {
	return time.Since(config.EachTimeServerInfo.StartUpTime) / time.Second
}

