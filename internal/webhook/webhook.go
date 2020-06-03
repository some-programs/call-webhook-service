package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-pa/fenv"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/some-programs/call-webhook-service/internal/backoff"
	"github.com/some-programs/call-webhook-service/internal/stanctx"
	"golang.org/x/time/rate"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	_ "net/http/pprof" // register pprof
)

// CallWebhookWorker .
type CallWebhookWorker struct {
	httpRateLimiter *rate.Limiter
}

// Flags .
type Flags struct {
	// worker options
	Workers      int
	RetryWorkers int
	MetricsAddr  string

	// nats options
	Nats stanctx.Flags

	// post options
	PostTimeout time.Duration

	RetryBuffer int

	// rate limiting
	RateLimit float64
	RateBurst int

	Backoff backoff.ExpBackoff

	PrintConfig bool
}

func (f *Flags) Register() {
	flag.IntVar(&f.Workers, "workers", 70,
		"number of concurrent message loop workers")
	flag.IntVar(&f.RetryWorkers, "retry_workers", 50,
		"number of concurrent message loop workers")
	flag.StringVar(&f.MetricsAddr, "addr", "localhost:6060",
		"host:port to bind metrics and debug")
	flag.DurationVar(&f.PostTimeout, "post_timeout", 35*time.Second,
		"http client timeout for webhook post")
	flag.IntVar(&f.RetryBuffer, "retry_buffer", 10e5,
		"maximum size of buffered retries")
	flag.Float64Var(&f.RateLimit, "rate_limit", 25,
		"maximum number of new httt prequests per second")
	flag.IntVar(&f.RateBurst, "rate_burst", 5,
		"maximum number burst http requests/s")
	flag.BoolVar(&f.PrintConfig, "print_config", false,
		"print config at start up")
	stanctx.RegisterFlags(&f.Nats)
	backoff.RegisterFlags(&f.Backoff)
}

func (f *Flags) Process() error {
	if f.Workers < 1 {
		return fmt.Errorf("-workers must be a >0")
	}
	if f.Nats.MaxInFlight == 0 {
		f.Nats.MaxInFlight = f.Workers
	}
	f.Nats.Process()
	f.Backoff.Process()

	return nil
}

func init() {
	rand.Seed(time.Now().UnixNano())
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
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

func (c *CallWebhookWorker) Start() error {
	var flags Flags
	flags.Register()
	fenv.CommandLinePrefix("CALL_URL_")
	fenv.MustParse()
	flag.Parse()
	if err := flags.Process(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if flags.PrintConfig {
		data, err := json.MarshalIndent(flags, "", " ")
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("configuration: %s", string(data))
	}

	if flags.MetricsAddr != "" {
		go func() {
			http.Handle("/metrics", promhttp.Handler())
			http.HandleFunc("/webhook/ok", func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(time.Duration(rand.Intn(3)) * time.Second)
				w.WriteHeader(200)
				data, err := ioutil.ReadAll(r.Body)
				r.Body.Close()
				if err != nil {
					log.Printf("webhook: %v", err)
					return
				}
				log.Printf("webhook: data: %s", string(data))
			})
			http.HandleFunc("/webhook/500", func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(time.Duration(rand.Intn(3)) * time.Second)
				w.WriteHeader(500)
				data, err := ioutil.ReadAll(r.Body)
				r.Body.Close()
				if err != nil {
					log.Printf("webhook: %v", err)
					return
				}
				log.Printf("webhook: data: %s", string(data))
			})
			http.HandleFunc("/webhook/rand", func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
				if rand.Float64() > 0.95 {
					return
				}
				if rand.Float64() > 0.95 {
					time.Sleep(flags.PostTimeout + time.Second)
					return
				}
				time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
				if rand.Float64() > 0.25 {
					w.WriteHeader(200 + rand.Intn(4))
				} else {
					w.WriteHeader(400 + rand.Intn(150))
				}
				time.Sleep(time.Duration(rand.Intn(2000)) * time.Millisecond)

				data, err := ioutil.ReadAll(r.Body)
				r.Body.Close()
				if err != nil {
					log.Printf("webhook: %v", err)
					return
				}
				log.Printf("webhook: data: %s", string(data))
			})
			http.ListenAndServe(flags.MetricsAddr, nil)
		}()
	}

	ctx := context.Background()

	c.httpRateLimiter = rate.NewLimiter(rate.Limit(flags.RateLimit), flags.RateBurst)

	var sc *stanctx.Conn
	{
		var attempts int
	connloop:
		for {
			var err error
			sc, err = stanctx.Connect("test-cluster", "client-123", stan.NatsURL(flags.Nats.NatsURL))
			if err != nil {
				if err == nats.ErrNoServers {
					log.Printf("# %v . could not connect to nats, will do 20 attempts before abandoning: %v", attempts, err)
					time.Sleep(time.Second)
					attempts++
					if attempts <= 20 {
						continue connloop
					}
				}

				log.Fatal(err)
			}
			break connloop
		}
	}

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
