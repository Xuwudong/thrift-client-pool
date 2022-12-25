package thrift_client_pool

import (
	"container/list"
	"errors"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"sync"
	"time"
)

const (
	CHECKINTERVAL = 60 //清除超时连接间隔
)

type ThriftDial func(server string, connTimeout time.Duration) (*IdleClient, error)
type ThriftClientClose func(c *IdleClient) error

type ThriftPool struct {
	Dial  ThriftDial
	Close ThriftClientClose

	lock        *sync.Mutex
	idle        list.List
	idleTimeout time.Duration
	connTimeout time.Duration
	maxConn     uint32
	count       uint32
	server      string
	closed      bool
}

type IdleClient struct {
	Transport thrift.TTransport
	Client    interface{}
}

type idleConn struct {
	c *IdleClient
	t time.Time
}

var nowFunc = time.Now

//error
var (
	ErrOverMax          = errors.New("连接超过设置的最大连接数")
	ErrInvalidConn      = errors.New("client回收时变成nil")
	ErrPoolClosed       = errors.New("连接池已经被关闭")
	ErrSocketDisconnect = errors.New("客户端socket连接已断开")
)

func NewThriftPool(server string,
	maxConn, connTimeout, idleTimeout uint32,
	dial ThriftDial, closeFunc ThriftClientClose) *ThriftPool {

	thriftPool := &ThriftPool{
		Dial:        dial,
		Close:       closeFunc,
		server:      server,
		lock:        new(sync.Mutex),
		maxConn:     maxConn,
		idleTimeout: time.Duration(idleTimeout) * time.Second,
		connTimeout: time.Duration(connTimeout) * time.Second,
		closed:      false,
		count:       0,
	}

	go thriftPool.ClearConn()

	return thriftPool
}

func (p *ThriftPool) Get() (*IdleClient, error) {
	p.lock.Lock()
	if p.closed {
		p.lock.Unlock()
		return nil, ErrPoolClosed
	}

	if p.idle.Len() == 0 && p.count >= p.maxConn {
		p.lock.Unlock()
		return nil, ErrOverMax
	}

	if p.idle.Len() == 0 {
		dial := p.Dial
		p.count += 1
		p.lock.Unlock()
		client, err := dial(p.server, p.connTimeout)
		if err != nil {
			p.lock.Lock()
			if p.count > 0 {
				p.count -= 1
			}
			p.lock.Unlock()
			return nil, err
		}
		if !client.Check() {
			p.lock.Lock()
			if p.count > 0 {
				p.count -= 1
			}
			p.lock.Unlock()
			return nil, ErrSocketDisconnect
		}
		return client, nil
	} else {
		ele := p.idle.Front()
		idlec := ele.Value.(*idleConn)
		p.idle.Remove(ele)
		p.lock.Unlock()

		if !idlec.c.Check() {
			p.lock.Lock()
			if p.count > 0 {
				p.count -= 1
			}
			p.lock.Unlock()
			return nil, ErrSocketDisconnect
		}
		return idlec.c, nil
	}
}

func (p *ThriftPool) Put(client *IdleClient) error {
	if client == nil {
		return ErrInvalidConn
	}

	p.lock.Lock()
	if p.closed {
		p.lock.Unlock()

		err := p.Close(client)
		client = nil
		return err
	}

	if p.count > p.maxConn {
		if p.count > 0 {
			p.count -= 1
		}
		p.lock.Unlock()

		err := p.Close(client)
		client = nil
		return err
	}

	if !client.Check() {
		if p.count > 0 {
			p.count -= 1
		}
		p.lock.Unlock()

		err := p.Close(client)
		client = nil
		return err
	}

	p.idle.PushBack(&idleConn{
		c: client,
		t: nowFunc(),
	})
	p.lock.Unlock()

	return nil
}

func (p *ThriftPool) CloseErrConn(client *IdleClient) error {
	if client == nil {
		return nil
	}

	p.lock.Lock()
	if p.count > 0 {
		p.count -= 1
	}
	p.lock.Unlock()

	err := p.Close(client)
	client = nil
	return err
}

func (p *ThriftPool) CheckTimeout() {
	p.lock.Lock()
	for p.idle.Len() != 0 {
		ele := p.idle.Back()
		if ele == nil {
			break
		}
		v := ele.Value.(*idleConn)
		if v.t.Add(p.idleTimeout).After(nowFunc()) {
			break
		}

		//timeout && clear
		p.idle.Remove(ele)
		p.lock.Unlock()
		p.Close(v.c) //close client connection
		p.lock.Lock()
		if p.count > 0 {
			p.count -= 1
		}
	}
	p.lock.Unlock()

	return
}

func (c *IdleClient) Check() bool {
	if c.Transport == nil || c.Client == nil {
		return false
	}
	return c.Transport.IsOpen()
}

func (p *ThriftPool) GetIdleCount() uint32 {
	return uint32(p.idle.Len())
}

func (p *ThriftPool) GetConnCount() uint32 {
	return p.count
}

func (p *ThriftPool) ClearConn() {
	for {
		p.CheckTimeout()
		time.Sleep(CHECKINTERVAL * time.Second)
	}
}

func (p *ThriftPool) Release() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	idle := p.idle
	p.idle.Init()
	p.closed = true
	p.count = 0

	for iter := idle.Front(); iter != nil; iter = iter.Next() {
		err := p.Close(iter.Value.(*idleConn).c)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *ThriftPool) Recover() {
	p.lock.Lock()
	if p.closed == true {
		p.closed = false
	}
	p.lock.Unlock()
}

type MapPool struct {
	Dial  ThriftDial
	Close ThriftClientClose

	lock *sync.Mutex

	idleTimeout uint32
	connTimeout uint32
	maxConn     uint32

	pools map[string]*ThriftPool
}

func NewMapPool(maxConn, connTimeout, idleTimeout uint32,
	dial ThriftDial, closeFunc ThriftClientClose) *MapPool {

	return &MapPool{
		Dial:        dial,
		Close:       closeFunc,
		maxConn:     maxConn,
		idleTimeout: idleTimeout,
		connTimeout: connTimeout,
		pools:       make(map[string]*ThriftPool),
		lock:        new(sync.Mutex),
	}
}

func (mp *MapPool) getServerPool(server string) (*ThriftPool, error) {
	mp.lock.Lock()
	defer mp.lock.Unlock()
	serverPool, ok := mp.pools[server]
	if !ok {
		err := errors.New(fmt.Sprintf("Addr:%s thrift pool not exist", server))
		return nil, err
	}
	return serverPool, nil
}

func (mp *MapPool) Get(server string) *ThriftPool {
	serverPool, err := mp.getServerPool(server)
	if err != nil {
		serverPool = NewThriftPool(server,
			mp.maxConn,
			mp.connTimeout,
			mp.idleTimeout,
			mp.Dial,
			mp.Close,
		)
		mp.lock.Lock()
		mp.pools[server] = serverPool
		mp.lock.Unlock()
	}
	return serverPool
}

func (mp *MapPool) Release(server string) error {
	serverPool, err := mp.getServerPool(server)
	if err != nil {
		return err
	}

	mp.lock.Lock()
	delete(mp.pools, server)
	mp.lock.Unlock()

	return serverPool.Release()
}

func (mp *MapPool) ReleaseAll() error {
	mp.lock.Lock()
	defer mp.lock.Unlock()

	for _, serverPool := range mp.pools {
		err := serverPool.Release()
		if err != nil {
			return err
		}
	}
	return nil
}
