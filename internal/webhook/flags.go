package webhook

import (
	"flag"
	"fmt"
	"time"

	"github.com/some-programs/call-webhook-service/internal/backoff"
	"github.com/some-programs/call-webhook-service/internal/stanctx"
)

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
	flag.StringVar(&f.MetricsAddr, "metrics.addr", "localhost:6060",
		"host:port to bind metrics and debug")
	flag.DurationVar(&f.PostTimeout, "post_timeout", 35*time.Second,
		"http client timeout for webhook post")
	flag.IntVar(&f.RetryBuffer, "retry_buffer", 10e5,
		"maximum size of buffered retries")
	flag.Float64Var(&f.RateLimit, "rate.limit", 25,
		"maximum number of new httt prequests per second")
	flag.IntVar(&f.RateBurst, "rate.burst", 5,
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
