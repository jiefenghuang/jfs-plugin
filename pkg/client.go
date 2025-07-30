package pkg

import (
	"net"
	"sync"
	"time"

	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/juicedata/juicefs/pkg/version"
	"github.com/pkg/errors"

	"github.com/sirupsen/logrus"
)

type CliOptions struct {
	Version  string
	Proto    string
	Addr     string
	MaxConn  uint
	BuffList []int
	Logger   logrus.FieldLogger
}

func (opt *CliOptions) Check() error {
	if opt.Logger == nil {
		opt.Logger = utils.GetLogger("plugin-client")
	}
	if err := checkProto(opt.Proto); err != nil {
		return err
	}
	if opt.Addr == "" {
		return errors.New("address must not be empty")
	}
	if opt.MaxConn == 0 {
		opt.MaxConn = 100
		logger.Warnf("max connection is set to 100")
	}
	if opt.BuffList == nil {
		opt.BuffList = DefaultCliCapList
	}
	if version.Parse(opt.Version) == nil {
		return errors.Errorf("invalid version: %s, format should be like '1.3.0'", opt.Version)
	}
	return nil
}

type Client struct {
	sync.Mutex
	*CliOptions
	closed  bool
	connCh  chan net.Conn
	wg      sync.WaitGroup
	pool    *bufferPool
	authErr error
}

func NewClient(opt *CliOptions) (*Client, error) {
	if err := opt.Check(); err != nil {
		return nil, err
	}
	return &Client{
		CliOptions: opt,
		connCh:     make(chan net.Conn, opt.MaxConn),
		pool:       newBufferPool(opt.BuffList),
	}, nil
}

func (c *Client) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	c.wg.Wait()
	close(c.connCh)
	for conn := range c.connCh {
		_ = conn.Close()
	}
	return nil
}

func (c *Client) getConn() (net.Conn, error) {
	if c.closed {
		return nil, errors.New("client is closed")
	}
	var conn net.Conn
	select {
	case conn = <-c.connCh:
	default:
		c.Lock()
		defer c.Unlock()
		dialer := &net.Dialer{Timeout: time.Second, KeepAlive: time.Minute}
		nConn, err := dialer.Dial(c.Proto, c.Addr)
		if err != nil {
			return nil, err
		}
		conn = nConn
		if err = c.auth(conn); err != nil {
			c.authErr = err
			_ = conn.Close()
			return nil, errors.New("plugin authentication failed")
		}
	}
	return conn, nil
}

func (c *Client) auth(conn net.Conn) (err error) {
	bodyLen := 2 + len(c.Version)
	buff := c.pool.Get(headerLen + bodyLen)
	defer c.pool.Put(buff)
	m := newEncMsg(buff, bodyLen, cmdAuth)
	m.putString(c.Version)
	if _, err = conn.Write(m.Bytes()); err != nil {
		return errors.Wrap(err, "failed to write verify request")
	}
	_, err = c.readResp(conn, buff[:headerLen], cmdAuth)
	return err
}

var ne = new(net.OpError)

func (c *Client) call(f func(conn net.Conn) error) error {
	if c.authErr != nil {
		return c.authErr
	}

	c.wg.Add(1)
	defer c.wg.Done()
	conn, err := c.getConn()
	if err != nil {
		return err
	}
	err = f(conn)
	if c.closed || (errors.As(err, &ne) && !ne.Timeout()) {
		_ = conn.Close()
	} else {
		select {
		case c.connCh <- conn:
		default:
			_ = conn.Close()
		}
	}
	return err
}
