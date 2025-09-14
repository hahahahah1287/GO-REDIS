package redis

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"strings"

	"github.com/zhangming/go-redis/interfaces/redis"
	"github.com/zhangming/go-redis/redis/protocol"
)

type Payload struct {
	Data redis.Reply
	Err  error
}

func protocolError(ch chan<- *Payload, msg string) {
	err := errors.New("protocol error: " + msg)
	ch <- &Payload{Err: err}
}

// ParseStream 通过 io.Reader 读取数据并将结果通过 channel 将结果返回给调用者
// 流式处理的接口适合供客户端/服务端使用
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

// ParseOne 解析 []byte 并返回 redis.Reply
func ParseOne(data []byte) (redis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse0(reader, ch)
	payload := <-ch
	if payload == nil {
		return nil, errors.New("parse failed")
	}
	return payload.Data, payload.Err
}

func parse0(rawReader io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil {
			slog.Error("error", err)
		}
	}()
	reader := bufio.NewReader(rawReader)
	for {
		message, err := reader.ReadBytes('\n')
		if err != nil {
			ch <- &Payload{Err: err}
			close(ch)
		}
		length := len(message)
		if length <= 2 && message[length-2] != '\r' {
			continue
		}

		message = bytes.TrimSuffix(message, []byte{'\r', '\n'})
		switch message[0] {
		case '+':
			content := string(message[1:])
			ch <- &Payload{Data: protocol.MakeStatusReply(content)}
			// 当 Redis 主节点与从节点进行 全量同步（Full Sync） 时，主节点会先发送一个 "FULLRESYNC" 状态响应，然后紧接着发送：

			// RDB 文件：作为快照数据。
			// AOF 日志（增量命令）：在 RDB 生成期间新增的操作记录。
			// 由于这两个部分是连续传输的，并且 中间没有 \r\n 分隔符，所以需要特殊处理。
			if strings.HasPrefix(content, "FULLRESYNC") {
				err = parseRDBBulkString(reader, ch)
				if err != nil {
					ch <- &Payload{Err: err}
					close(ch)
					return
				}
			}
		case '-':
			ch <- &Payload{Data: protocol.MakeErrReply(string(message[1:]))}
		case ':':
			val, err := strconv.ParseInt(string(message[1:]), 10, 64)
			if err != nil {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
			ch <- &Payload{Data: protocol.MakeIntReply(val)}
		case '$':
			err = parseBulkString(message, reader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
		case '*':
			err = parseArray(message, reader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
		default:
			args := bytes.Split(message, []byte{' '})
			ch <- &Payload{
				Data: protocol.MakeMultiBulkReply(args),
			}
		}
	}

}

func parseBulkString(line []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	strLen, err := strconv.ParseInt(string(line[1:]), 10, 64)
	if err != nil || strLen < -1 {
		protocolError(ch, "invalid bulk string length")
		return nil
	} else if strLen == -1 {
		ch <- &Payload{
			Data: protocol.MakeNullBulkReply(),
		}
		return nil
	}
	body := make([]byte, strLen+2)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		protocolError(ch, "invalid bulk string")
		return nil
	}
	ch <- &Payload{
		Data: protocol.MakeBulkReply(body[:len(body)-2]),
	}
	return nil
}

func parseArray(line []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	nStrs, err := strconv.ParseInt(string(line[1:]), 10, 64)
	if err != nil || nStrs < 0 {
		protocolError(ch, "invalid array length")
		return nil
	} else if nStrs == 0 {
		ch <- &Payload{
			Data: protocol.MakeMultiBulkReply(nil),
		}
		return nil
	}
	lines := make([][]byte, 0, nStrs)
	for i := 0; i < len(lines); i++ {
		line := lines[i]
		length := len(line)
		if length <= 4 || line[length-2] != '\r' || line[0] != '$' {
			protocolError(ch, "invalid bulk string length")
			break
		}
		strLen, err := strconv.ParseInt(string(line[1:length-2]), 10, 64)
		if err != nil || strLen < -1 {
			protocolError(ch, "invalid bulk string length")
			return nil
		} else if strLen == -1 {
			lines = append(lines, []byte{})
		} else {
			body := make([]byte, strLen+2)
			_, err := io.ReadFull(reader, body)
			if err != nil {
				return err
			}
			lines = append(lines, body[:len(body)-2])
		}
	}
	ch <- &Payload{
		Data: protocol.MakeMultiBulkReply(lines),
	}
	return nil
}

func parseRDBBulkString(reader *bufio.Reader, ch chan<- *Payload) error {
	header, err := reader.ReadBytes('\n')
	if err != nil {
		return err
	}
	header = bytes.TrimSuffix(header, []byte{'\r', '\n'})
	if len(header)==0 {
		return errors.New("invalid RDB bulk string length")
	}
	strLen,err:= strconv.ParseInt(string(header), 10, 64)
	if err != nil || strLen < -1 {
		return errors.New("invalid RDB bulk string length")
	}
	body := make([]byte, strLen)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return err
	}
	ch <- &Payload{
		Data: protocol.MakeBulkReply(body[:len(body)]),
	}
	return nil
}
