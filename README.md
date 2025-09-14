# Go-Redis

Go-Redis 是一个用 Go 语言实现的 Redis 单体服务器。本项目旨在学习redis的内部结构，了解redis的相关用法与高并发的意义

关键功能:
- 支持 string, list, hash, set, sorted set, bitmap 数据结构
- 自动过期功能(TTL)
- 发布订阅
- 地理位置
- AOF 持久化、RDB 持久化、aof-use-rdb-preamble 混合持久化
- Multi 命令开启的事务具有**原子性**和隔离性. 若在执行过程中遇到错误, godis 会回滚已执行的命令

# 运行 

直接先整理依赖，完成初始化

```
go mod tidy
```

在主目录下，使用命令行启动服务器

```bash
go run main.go
```

redis 默认监听 0.0.0.0:6399，可以使用 redis-cli 或者其它 redis 客户端连接 Go-Redis 服务器。

redis 首先会从CONFIG环境变量中读取配置文件路径。若环境变量中未设置配置文件路径，则会尝试读取工作目录中的 redis.conf 文件。 

所有配置项均在 [example.conf](./example.conf) 中作了说明。

ps : 不要利用浏览器打开，浏览器通信协议和redis通信协议不一样