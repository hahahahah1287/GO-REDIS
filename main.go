package main

import (
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/zhangming/go-redis/config"
	"github.com/zhangming/go-redis/lib/utils"
	"github.com/zhangming/go-redis/redis/server/std"
)

var banner = `
   ______          ___
  / ____/___  ____/ (_)____
 / / __/ __ \/ __  / / ___/
/ /_/ / /_/ / /_/ / (__  )
\____/\____/\__,_/_/____/
`

var defaultProperties = &config.ServerProperties{
	Bind:           "0.0.0.0",
	Port:           6399,
	AppendOnly:     false,
	AppendFilename: "",
	MaxClients:     1000,
	RunID:          utils.RandString(40),
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

func main() {
	print(banner)
	slog.Info("starting redis server...")
	configFilename := os.Getenv("CONFIG")
	if configFilename == "" {
		if fileExists("redis.conf") {
			config.SetupConfig("redis.conf")
		} else {
			config.Properties = defaultProperties
		}
	} else {
		config.SetupConfig(configFilename)
	}
	listenAddr := fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port)
	go func() {
		slog.Info("Starting pprof server on localhost:6060")
		err := http.ListenAndServe("localhost:6060", nil)
		if err != nil {
			slog.Error("pprof server failed to start:", err)
		}
	}()
	var err error
	// 直接用stdserver启动
	handler := std.MakeHandler()
	err = std.Serve(listenAddr, handler)
	if err != nil {
		slog.Error("start server failed: %v", err)
	}

}
