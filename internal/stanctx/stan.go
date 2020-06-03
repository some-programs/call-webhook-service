package stanctx

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/stan.go"
)

func Connect(stanClusterID, clientID string, options ...stan.Option) (*Conn, error) {
	ctx, cancel := context.WithCancel(context.Background())
	conn := &Conn{
		ctx:    ctx,
		cancel: cancel,
	}
	options = append(options, stan.SetConnectionLostHandler(conn.connectionLostHandler))
	c, err := stan.Connect(stanClusterID, clientID, options...)
	if err != nil {
		cancel()
		return nil, err
	}
	conn.Conn = c
	return conn, nil
}

// Conn .
type Conn struct {
	stan.Conn

	mu sync.Mutex

	ctx    context.Context // context which is canceled if the connection is lost
	cancel context.CancelFunc

	err error
}

// last error, if there is one
func (c *Conn) Err() error {
	c.mu.Lock()
	err := c.err
	c.mu.Unlock()
	return err
}

func (c *Conn) connectionLostHandler(conn stan.Conn, err error) {
	log.Println("connection lost")
	c.mu.Lock()
	if c.err == nil {
		c.err = err
	}
	c.mu.Unlock()
	c.cancel()
}

// Subscribe(subject string, cb MsgHandler, opts ...SubscriptionOption) (Subscription, error)
func (conn *Conn) Subscribe(subject string, opts ...stan.SubscriptionOption) (*Subscription, error) {
	if subject == "" {
		return nil, fmt.Errorf("empty subject name")
	}
	ctx, cancel := context.WithCancel(conn.ctx)
	sub := &Subscription{
		ctx:     ctx,
		cancel:  cancel,
		subject: subject,
		opts:    opts,
		conn:    conn,
	}
	var o stan.SubscriptionOptions
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	return sub, nil
}

// Subscription .
type Subscription struct {
	mu   sync.Mutex
	conn *Conn
	done bool

	sub stan.Subscription

	subject string
	opts    []stan.SubscriptionOption

	ctx    context.Context // context which is canceled if the connection is lost
	cancel context.CancelFunc
}

func (s *Subscription) Started() bool {
	s.mu.Lock()
	started := s.conn != nil && !s.done
	s.mu.Unlock()
	return started
}

func (s *Subscription) subscribe(cb stan.MsgHandler) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sub != nil {
		return errors.New("already started")
	}
	if s.done {
		return errors.New("already stopped")
	}
	sub, err := s.conn.Conn.Subscribe(s.subject, cb, s.opts...)
	if err != nil {
		return err
	}
	s.sub = sub
	return nil
}

func (s *Subscription) monitor(ctx context.Context, unsubscribeOnCancel bool) error {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			if !s.sub.IsValid() {
				return SubscriptionError{StanErr: ErrSubscriptionEnded}
			}
		case <-s.ctx.Done():
			return SubscriptionError{
				CtxErr:  ctx.Err(),
				StanErr: s.conn.Err(),
			}
		case <-ctx.Done():
			e := SubscriptionError{
				CtxErr: ctx.Err(),
			}
			if s.sub.IsValid() {
				if unsubscribeOnCancel {
					e.StanErr = s.sub.Unsubscribe()
				} else {
					e.StanErr = s.sub.Close()
				}
			} else {
				e.StanErr = ErrSubscriptionEnded
			}
			if e.Msg != "" || e.StanErr != nil || e.CtxErr != nil {
				return e
			}
			return nil
		}
	}
}

func (s *Subscription) StartFunc(ctx context.Context, cb stan.MsgHandler) error {
	if err := s.subscribe(cb); err != nil {
		return err
	}

	return s.monitor(ctx, false)
}

func (s *Subscription) StartChan(ctx context.Context, msgCh chan<- *stan.Msg) error {
	err := s.subscribe(func(msg *stan.Msg) {
		msgCh <- msg
	})
	if err != nil {
		return err
	}

	if err := s.monitor(ctx, false); err != nil {
		return err
	}
	return nil
}
