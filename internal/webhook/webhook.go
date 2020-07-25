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
	"time"

	"github.com/nats-io/stan.go"
	"github.com/some-programs/call-webhook-service/internal/backoff"
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
	return fmt.Sprintf("retry #%v  %v :: %s", r.Retries, r.Next.Format("0102 15:04:05.000"), action)
}

func (c *CallWebhookWorker) Start(ctx context.Context, flags Flags, ch <-chan CallWebHookMessage) error {
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

	addRetryCh := make(chan RetryMessage, 5000)
	doRetryCh := make(chan RetryMessage, (flags.Workers+flags.RetryWorkers)*2)

	// handle new messages
	for i := 0; i < flags.Workers; i++ {
		go func() {
			for {
				select {
				case cm := <-ch:
					promMsgsRreceived.Inc()

					log.Printf("recv url:%v", cm.URL)
					if err := c.httpRateLimiter.Wait(ctx); err != nil {
						log.Fatal("err", err)
					}

					ctx, cancel := context.WithTimeout(context.Background(), flags.PostTimeout)
					status, err := postJSON(ctx, cm.URL, cm.Payload)
					cancel()
					if err != nil {
						log.Printf("error posting message:%v", err)
					}
					if err != nil || !(status >= 200 && status < 300) {
						promPostFailed.Inc()
						if flags.Backoff.MaxInterval > 0 {
							rm := RetryMessage{
								Created: time.Now(),
								Msg:     cm,
							}
							if rm.SetNext(flags.Backoff) {
								log.Println(rm.LogLine("send to addRetryCh:"))
								addRetryCh <- rm
							}
						}
					} else {
						promPostSuccess.Inc()
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
					status, err := postJSON(ctx, m.Msg.URL, m.Msg.Payload)
					cancel()
					promPostRetries.Inc()
					if err != nil {
						log.Printf("error posting message:%v", err)
					}
					if err != nil || !(status >= 200 && status < 300) {
						promPostFailed.Inc()
						if m.SetNext(flags.Backoff) {
							addRetryCh <- m
						} else {
							log.Printf("max retry age reached")
						}
					} else {
						promPostSuccess.Inc()
					}
				}
			}
		}()
	}



	return nil
}

func parseStanMessage(m *stan.Msg) (CallWebHookMessage, error) {
	var cm CallWebHookMessage
	err := json.Unmarshal(m.Data, &cm)
	return cm, err
}

func postJSON(ctx context.Context, URL string, payload []byte) (int, error) {
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
