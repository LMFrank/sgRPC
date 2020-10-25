package sgRPC

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sgRPC/codec"
	"sync"
)

type Call struct {
	Seq           uint64
	ServiceMethod string      // <service>.<method>
	Args          interface{} // 函数的参数
	Reply         interface{} // 函数的返回值
	Error         error
	Done          chan *Call // 当函数调用完成后会用call.Done通知对方，从而支持异步调用
}

func (call *Call) done() {
	call.Done <- call
}

type Client struct {
	cc       codec.Codec // 消息的解编码器
	opt      *Option
	sending  sync.Mutex
	header   codec.Header
	mu       sync.Mutex
	seq      uint64           // 请求编号
	pending  map[uint64]*Call // 存储未处理完的请求，key为编号，value为Call实例
	closing  bool             // closing或shutdown任一置为true时表示Client处于不可用状态
	shutdown bool             // closing表示用户主动关闭，shutdown一般是有错误发生
}

var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("connection is shut down")

func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()

	if client.closing {
		return ErrShutdown
	}
	client.closing = true

	return client.cc.Close()
}

func (client *Client) IsAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()

	return !client.shutdown && !client.closing
}

// 将参数call添加到client.pending中，并更新client.seq
func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()

	if client.closing || client.shutdown {
		return 0, ErrShutdown
	}

	call.Seq = client.seq
	client.pending[call.Seq] = call
	client.seq++

	return call.Seq, nil
}

// 根据seq，从client.pending中移除对应的call
func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()

	call := client.pending[seq]
	delete(client.pending, seq)

	return call
}

// 调用错误时，将shutdown置为true，且将错误信息通知给所有pending状态的call
func (client *Client) terminateCalls(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()

	client.mu.Unlock()
	defer client.mu.Unlock()

	client.shutdown = true
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
}

func (client *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		if err = client.cc.ReadHeader(&h); err != nil {
			break
		}
		call := client.removeCall(h.Seq)
		switch {
		case call == nil:
			// call不存在，可能是请求没有发送完整，或者因为其他原因被取消，但是服务端仍旧处理了
			err = client.cc.ReadBody(nil)
		case h.Error != "":
			// call存在，但服务端处理出错
			call.Error = fmt.Errorf(h.Error)
			err = client.cc.ReadBody(nil)
			call.done()
		default:
			// call存在，服务端处理正常，从body中读取Reply的值
			err = client.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body" + err.Error())
			}
			call.done()
		}
		client.terminateCalls(err)
	}
}

func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}

	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: options error:", err)
		_ = conn.Close()
		return nil, err
	}

	return NewClientCodec(f(conn), opt), nil
}

func newClientCodec(cc codec.Codec, opt *Option) *Client {
	client := &Client{
		seq:     1, // 从1开始，0则为无效的Call
		cc:      cc,
		opt:     opt,
		pending: make(map[uint64]*Call),
	}

	go client.receive()

	return client
}

func parseOptions(opts ...*Option) (*Option, error) {
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}

	opt := opts[0]
	opt.MagicNumber = DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}

	return opt, nil
}

func Dial(network, address string, opts ...*Option) (client *Client, err error) {
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}

	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	defer func() {
		if client == nil {
			_ = conn.Close()
		}
	}()

	return NewClient(conn, opt)
}
