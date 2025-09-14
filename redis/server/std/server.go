package std

import (
	"context"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"

	"github.com/zhangming/go-redis/database"
	idatabase "github.com/zhangming/go-redis/interfaces/database"
	"github.com/zhangming/go-redis/interfaces/redis/parser"
	"github.com/zhangming/go-redis/redis/connection"
	"github.com/zhangming/go-redis/redis/protocol"
	"github.com/zhangming/go-redis/tcp"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

type Handler struct {
	activeConn sync.Map // *client -> placeholder
	db         idatabase.DB
	closing    bool
}

func MakeHandler() *Handler {
	db := database.NewStandaloneServer()
	return &Handler{
		db: db,
	}
}
func Serve(addr string, handler *Handler) error {
	return tcp.ListenAndServeWithSignal(&tcp.Config{
		Address: addr,
	}, handler)
}

func (h *Handler) closeClient(client *connection.Connection) {
	_ = client.Close()
	h.db.AfterClientClose(client)
	h.activeConn.Delete(client)
}

func (h *Handler) Close() error {
	slog.Info("handler shutting down...")
	h.closing = true
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	h.db.Close()
	return nil
}

func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	slog.Info("connection accepted: " + conn.RemoteAddr().String())
	slog.Info("ctx 内容 " + conn.RemoteAddr().String())
	if h.closing {
		// closing handler refuse new connection
		_ = conn.Close()
		return
	}

	client := connection.NewConn(conn)
	h.activeConn.Store(client, struct{}{})
	slog.Info("clent 内容 " + client.RemoteAddr())

	ch := parser.ParseStream(conn)
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// connection closed
				slog.Error("进入EOF处理了")
				h.closeClient(client)
				slog.Info("connection closed: " + client.RemoteAddr())
				return
			}
			// protocol err
			slog.Error("进入其他错误")
			errReply := protocol.MakeErrReply(payload.Err.Error())
			_, err := client.Write(errReply.ToBytes())
			if err != nil {
				h.closeClient(client)
				slog.Info("connection closed: " + client.RemoteAddr())
				return
			}
			continue
		}
		if payload.Data == nil {
			slog.Error("empty payload")
			continue
		}
		r, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			slog.Error("require multi bulk protocol")
			continue
		}
		slog.Info("命令内容 " + string(r.ToBytes()))
		result := h.db.Exec(client, r.Args)
		slog.Info("result: ", result)
		if result != nil {
			_, _ = client.Write(result.ToBytes())
		} else {
			_, _ = client.Write(unknownErrReplyBytes)
		}
	}
}
