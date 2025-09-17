# Go-Redis

![Go](https://img.shields.io/badge/Go-1.24+-00ADD8?style=for-the-badge&logo=go)  ![Redis](https://img.shields.io/badge/Redis-DC382D?style=for-the-badge&logo=redis&logoColor=white) ![License](https://img.shields.io/badge/License-MIT-blue?style=for-the-badge) ![Support](https://img.shields.io/badge/Support-Transactions-9FE2BF?style=flat-square) ![Protocol](https://img.shields.io/badge/Protocol-RESP3-FF6B6B?style=flat-square)

**Go-Redis** 是一个使用 Go 语言实现的 Redis 兼容服务器。本项目旨在深入学习 Redis 内部架构、网络协议和高并发编程，完全兼容 Redis 协议，支持标准 Redis 客户端连接。

## ✨ 特性功能

- **丰富的数据结构**: 支持 string、list、hash、set、sorted set等数据结构
- **自动过期机制**: 完整的 TTL (Time-To-Live) 支持
- **发布订阅模式**: 实现 Pub/Sub 消息分发机制
- **持久化支持**:
  - AOF (Append Only File) 持久化
  - RDB (Redis Database) 快照持久化  
  - AOF-use-RDB-preamble 混合持久化模式
- **事务支持**: Multi 命令开启的事务具有**原子性**和隔离性，执行失败时自动回滚
- **高性能**: 基于 Go 的高并发特性，提供优秀的性能表现

## 🚀 快速开始

###  prerequisites

- Go 1.24+
- Redis CLI (用于测试连接)

### 安装运行

1. **下载依赖**
   
   ```bash
   go mod tidy
   ```
   
2. **启动服务器**
   ```bash
   go run main.go
   ```

3. **连接测试**
   服务器默认监听 `0.0.0.0:6399`，可以使用以下命令连接：
   ```bash
   redis-cli -p 6399
   ```
   或者使用任何兼容 Redis 协议的客户端工具。

### 配置说明

Go-Redis 会按以下顺序加载配置：
1. 从 `CONFIG` 环境变量指定的路径读取
2. 如果环境变量未设置，尝试读取工作目录下的 `redis.conf` 文件

所有配置项均在 [redis.conf](./redis.conf) 文件中详细说明。

**注意**: 请不要使用浏览器访问，Redis 使用自定义二进制协议而非 HTTP 协议。

## 命令支持

所有支持的 Redis 命令及其用法请参阅 [commands.md](./commands.md) 文档。

##  参与贡献

欢迎提交 Issue 和 Pull Request！对于想要学习 Redis 内部原理的开发者来说，这是一个很好的实践项目。

**如果这个项目对你有帮助，请给它一个 ⭐️ ！**