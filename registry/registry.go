package registry

import (
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// SgRegistry is a simple register center, provide following functions.
// add a server and receive heartbeat to keep it alive.
// returns all alive servers and delete dead servers sync simultaneously.
type SgRegistry struct {
	timeout time.Duration
	mu      sync.Mutex
	servers map[string]*ServerItem
}

type ServerItem struct {
	Addr  string
	start time.Time
}

const (
	defaultPath    = "/_sgrpc_/registry"
	defaultTimeout = time.Minute * 5
)

func New(timeout time.Duration) *SgRegistry {
	return &SgRegistry{
		servers: make(map[string]*ServerItem),
		timeout: timeout,
	}
}

var DefaultSgRegistry = New(defaultTimeout)

// 添加服务实例，如果服务已经存在，则更新start
func (r *SgRegistry) putServer(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.servers[addr]
	if s == nil {
		r.servers[addr] = &ServerItem{Addr: addr, start: time.Now()}
	} else {
		s.start = time.Now()
	}
}

// 返回可用的服务列表，如果存在超时服务，则删除
func (r *SgRegistry) aliveServers() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var alive []string
	for addr, s := range r.servers {
		if r.timeout == 0 || s.start.Add(r.timeout).After(time.Now()) {
			alive = append(alive, addr)
		} else {
			delete(r.servers, addr)
		}
	}
	sort.Strings(alive)
	return alive
}

// sgRegistry 采用 HTTP 协议提供服务，且所有的有用信息都承载在 HTTP Header 中
// Get：返回所有可用的服务列表，通过自定义字段 X-SgRPC-Servers 承载。
// Post：添加服务实例或发送心跳，通过自定义字段 X-SgRPC-Server 承载
func (r *SgRegistry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		w.Header().Set("X-SgRPC-Servers", strings.Join(r.aliveServers(), ","))
	case "POST":
		addr := req.Header.Get("X-SgRPC-Server")
		if addr == "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		r.putServer(addr)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

//HandleHTTP registers an HTTP handler for SgRegistry messages on registryPath
func (r *SgRegistry) HandleHTTP(registryPath string) {
	http.Handle(registryPath, r)
	log.Println("prc server path:", registryPath)
}

func HandleHTTP() {
	DefaultSgRegistry.HandleHTTP(defaultPath)
}

// 服务启动时定时向注册中心发送心跳
// 默认周期比注册中心设置的过期时间少 1 min
func Heartbeat(registry, addr string, duration time.Duration) {
	if duration == 0 {
		duration = defaultTimeout - time.Duration(1)*time.Minute
	}
	var err error
	err = sendHeartbeat(registry, addr)
	go func() {
		t := time.NewTicker(duration)
		for err == nil {
			<-t.C
			err = sendHeartbeat(registry, addr)
		}
	}()
}

func sendHeartbeat(registry, addr string) error {
	log.Println(addr, "send heart beat to registry", registry)
	httpClient := &http.Client{}
	req, _ := http.NewRequest("POST", registry, nil)
	req.Header.Set("X-SgRPC-Server", addr)
	if _, err := httpClient.Do(req); err != nil {
		log.Println("rpc server: heart beat err:", err)
		return err
	}
	return nil
}
