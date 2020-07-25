package webhook

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/some-programs/call-webhook-service/internal/stanctx"
)

func ListenNats(ctx context.Context, flags Flags, ch chan<- CallWebHookMessage) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var sc *stanctx.Conn
	{
		var attempts int
	connloop:
		for {
			var err error
			sc, err = stanctx.ConnectContext(ctx, "test-cluster", "client-123", stan.NatsURL(flags.Nats.NatsURL))
			if err != nil {
				if err == nats.ErrNoServers {
					log.Printf("error connecting to nats attempt #%v", attempts+1)
					time.Sleep(time.Second)
					attempts++
					if attempts <= 2 {
						continue connloop
					}
				}
				log.Fatal(err)
			}
			break connloop
		}
	}

	sub, err := sc.Subscribe("call_webhook",
		stan.DurableName(flags.Nats.DurableName),
		stan.AckWait(flags.Nats.AckWait),
		stan.MaxInflight(flags.Nats.MaxInFlight),
		stan.SetManualAckMode())
	if err != nil {
		log.Fatal(err)
	}

	sch := make(chan *stan.Msg, flags.Nats.MaxInFlight)

	go func() {
		defer cancel()
		err := sub.StartChan(ctx, sch)
		if err != nil {
			log.Printf("error from subscribe: %v", err)
		}
	}()

	var (
		seqMaxRecv   uint64
		seqMaxRecvMu sync.Mutex
	)

messages:
	for {
		select {
		case m := <-sch:
			m.Ack()
			promMsgsRreceived.Inc()

			seqMaxRecvMu.Lock()
			if m.Sequence > seqMaxRecv {
				seqMaxRecv = m.Sequence
				promHighestReceivedSequence.Set(float64(seqMaxRecv))
			}
			seqMaxRecvMu.Unlock()

			cm, err := parseStanMessage(m)
			if err != nil {
				promMsgsInvalidFormat.Inc()
				log.Printf("[seq:%v] error parsing message: %v", m.Sequence, err)
				continue messages
			}
			ch <- cm
		case <-ctx.Done():
			return nil
		}
	}

	return nil
}
