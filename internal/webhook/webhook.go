package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/some-programs/call-webhook-service/internal/backoff"
	"github.com/some-programs/call-webhook-service/internal/stanctx"
	"golang.org/x/time/rate"

	_ "net/http/pprof" // register pprof
)

// CallWebhookWorker .
type CallWebhookWorker struct {
	httpRateLimiter *rate.Limiter
}

// CallWebHookMessage .
type CallWebHookMessage struct {
	URL     string          `json:"url"`
	Payload json.RawMessage `json:"payload"`
}

func (c CallWebHookMessage) String() string {
	return fmt.Sprintf("callwebhookmessage, %s: %s", c.URL, c.Payload)
}

// RetryMessage .
type RetryMessage struct {
	Created time.Time
	Next    time.Time

	Msg          CallWebHookMessage
	NextUnixNano int64
	StanMsg      *stan.Msg

	Retries int
}

const StopRetry = math.MaxInt64

// returns false if there is no next attempt
func (r *RetryMessage) SetNext(eb backoff.ExpBackoff) bool {
	if r.NextUnixNano == StopRetry {
		log.Fatal("SetNext run on a RetryMessage which already has ended")
	}

	dur := eb.Backoff(r.Retries + 1)
	next := r.Created.Add(dur)

	if next.After(r.Created.Add(eb.MaxElapsed)) {
		r.NextUnixNano = StopRetry
		r.Next = time.Now().Add(98 * 365 * 24 * time.Hour)
		return false
	}
	r.Retries += 1
	r.Next = next
	r.NextUnixNano = next.UnixNano()

	return true
}

func (r RetryMessage) LogLine(action string) string {
	return fmt.Sprintf("[seq:%v] retry #%v  %v :: %s", r.StanMsg.Sequence, r.Retries, r.Next.Format("0102 15:04:05.000"), action)
}

func (c *CallWebhookWorker) Start(ctx context.Context, flags Flags) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c.httpRateLimiter = rate.NewLimiter(rate.Limit(flags.RateLimit), flags.RateBurst)

	// log backoff settings example
	if flags.PrintConfig {
		t := time.Now()
		rm := RetryMessage{
			Created: t,
		}
		var durs []string
	ll:
		for {
			if !rm.SetNext(flags.Backoff) {
				break ll
			}
			durs = append(durs, rm.Next.Sub(t).Round(time.Second).String())
		}
		log.Printf("example backoff series: %s", strings.Join(durs, ", "))
	}

	var sc *stanctx.Conn
	{
		var attempts int
	connloop:
		for {
			var err error
			sc, err = stanctx.Connect("test-cluster", "client-123", stan.NatsURL(flags.Nats.NatsURL))
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

	addRetryCh := make(chan RetryMessage, 5000)
	doRetryCh := make(chan RetryMessage, (flags.Workers+flags.RetryWorkers)*2)
	ch := make(chan *stan.Msg, flags.Nats.MaxInFlight)

	var (
		seqMaxRecv   uint64
		seqMaxRecvMu sync.Mutex

		seqMaxLastPostSuccess   uint64
		seqMaxLastPostSuccessMu sync.Mutex
	)

	// handle new messages
	for i := 0; i < flags.Workers; i++ {
		go func() {
		messages:
			for {
				select {
				case m := <-ch:
					promMsgsRreceived.Inc()

					seqMaxRecvMu.Lock()
					if m.Sequence > seqMaxRecv {
						seqMaxRecv = m.Sequence
						promHighestReceivedSequence.Set(float64(seqMaxRecv))
					}
					seqMaxRecvMu.Unlock()

					cm, err := c.parseMessage(m)
					if err != nil {
						promMsgsInvalidFormat.Inc()
						log.Printf("[seq:%v] error parsing message: %v", m.Sequence, err)
						m.Ack()
						continue messages
					}

					log.Printf("[seq:%v] recv url:%v", m.Sequence, cm.URL)
					if err := c.httpRateLimiter.Wait(ctx); err != nil {
						log.Fatal("err", err)
					}

					ctx, cancel := context.WithTimeout(context.Background(), flags.PostTimeout)
					status, err := c.postJSON(ctx, cm.URL, cm.Payload)
					cancel()
					if err != nil {
						log.Printf("[seq:%v] error posting message:%v", m.Sequence, err)
					}
					if err != nil || !(status >= 200 && status < 300) {
						promPostFailed.Inc()
						if flags.Backoff.MaxInterval > 0 {
							rm := RetryMessage{
								Created: time.Now(),
								Msg:     cm,
								StanMsg: m,
							}
							if rm.SetNext(flags.Backoff) {
								log.Println(rm.LogLine("send to addRetryCh:"))
								addRetryCh <- rm
							}
						}
					} else {
						promPostSuccess.Inc()

						seqMaxLastPostSuccessMu.Lock()
						if m.Sequence > seqMaxLastPostSuccess {
							seqMaxLastPostSuccess = m.Sequence
							promHighestReceivedSequence.Set(float64(seqMaxLastPostSuccess))
						}
						seqMaxLastPostSuccessMu.Unlock()

					}
					if err := m.Ack(); err != nil {
						log.Printf("[seq:%v] error Ack'ing message:%v", m.Sequence, err)
					}
				}
			}
		}()
	}

	// manage retry list
	go func() {
		housekeeping := time.NewTicker(5 * time.Second)
		defer housekeeping.Stop()

		dispatchRetry := time.NewTicker(500 * time.Millisecond)
		defer dispatchRetry.Stop()
		var items []RetryMessage
		for {
			select {
			case m := <-addRetryCh:
				items = append(items, m)
				promRetryBufferSize.Inc()

			case <-housekeeping.C:
				sort.Slice(items, func(i, j int) bool { return items[i].NextUnixNano > items[j].NextUnixNano })
				if len(items) > int(float64(flags.RetryBuffer)*1.1) {
					items = items[len(items)-flags.RetryBuffer:]
					promRetryBufferSize.Set(float64(len(items)))
				}
			case <-dispatchRetry.C:
				now := time.Now().UnixNano()
				timer := time.NewTimer(100 * time.Millisecond)
				remove := make(map[int]bool)
			check:
				for idx, v := range items {
					if now > v.NextUnixNano {
						select {
						case doRetryCh <- v:
							log.Println(v.LogLine("send to http post worker"))
							remove[idx] = true
							continue check
						case <-timer.C:
							log.Printf("break check, sent %v items", len(remove))
							break check
						}
					}
				}
				timer.Stop()
				if len(remove) > 0 {
					var newItems []RetryMessage
					for i, v := range items {
						if !remove[i] {
							newItems = append(newItems, v)
						}
					}
					items = newItems
					promRetryBufferSize.Sub(float64(len(remove)))
				}
			}
		}
	}()

	// do http post retries
	for i := 0; i < flags.RetryWorkers; i++ {
		go func() {
			for {
				select {
				case m := <-doRetryCh:
					log.Println(m.LogLine("http post worker received"))

					if err := c.httpRateLimiter.Wait(ctx); err != nil {
						log.Fatal("err", err)
					}

					ctx, cancel := context.WithTimeout(context.Background(), flags.PostTimeout)
					status, err := c.postJSON(ctx, m.Msg.URL, m.Msg.Payload)
					cancel()
					promPostRetries.Inc()
					if err != nil {
						log.Printf("[seq:%v] error posting message:%v", m.StanMsg.Sequence, err)
					}
					if err != nil || !(status >= 200 && status < 300) {
						promPostFailed.Inc()
						if m.SetNext(flags.Backoff) {
							addRetryCh <- m
						} else {
							log.Printf("[seq:%v] max retry age reached", m.StanMsg.Sequence)
						}
					} else {
						seqMaxLastPostSuccessMu.Lock()
						if m.StanMsg.Sequence > seqMaxLastPostSuccess {
							seqMaxLastPostSuccess = m.StanMsg.Sequence
							promHighestReceivedSequence.Set(float64(seqMaxLastPostSuccess))
						}
						seqMaxLastPostSuccessMu.Unlock()
						promPostSuccess.Inc()
					}
				}
			}
		}()
	}

	err = sub.StartChan(ctx, ch)
	if err != nil {
		log.Printf("error from subscribe: %v", err)
	}
	return err
}

func (c *CallWebhookWorker) parseMessage(m *stan.Msg) (CallWebHookMessage, error) {
	var cm CallWebHookMessage
	err := json.Unmarshal(m.Data, &cm)
	return cm, err
}

func (c *CallWebhookWorker) postJSON(ctx context.Context, URL string, payload []byte) (int, error) {
	req, err := http.NewRequest("POST", URL, bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(ctx)
	promActiveHTTPRequests.Inc()
	defer promActiveHTTPRequests.Dec()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return -1, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}
