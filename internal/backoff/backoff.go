package backoff

import (
	"flag"
	"math/rand"
	"time"
)

type ExpBackoff struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Jitter          float64
	Factor          float64
	MaxElapsed      time.Duration
}

func RegisterFlags(b *ExpBackoff) {
	flag.DurationVar(&b.InitialInterval, "backoff_initial_interval", 30*time.Second, "backoff initial delay until first rety")
	flag.DurationVar(&b.MaxInterval, "backoff_max_interval", 4*time.Hour, "backoff maximum elapsed time")
	flag.DurationVar(&b.MaxElapsed, "backoff_max_elapsed", 3*24*time.Hour, "backoff maximum elapsed time")
	flag.Float64Var(&b.Jitter, "backoff_jitter", 0.1, "backoff randomization factor")
	flag.Float64Var(&b.Factor, "backoff_factor", 1.5, "backoff factor")
}

func (f *ExpBackoff) Process() error {
	return nil
}

func (bc ExpBackoff) Backoff(retries int) time.Duration {
	if retries == 0 {
		return bc.InitialInterval
	}
	backoff, maxElapsed, maxInterval := float64(bc.InitialInterval), float64(bc.MaxElapsed), float64(bc.MaxInterval)
	for backoff < maxElapsed && retries > 0 {
		nb := backoff * bc.Factor
		if (nb - backoff) > maxInterval {
			backoff += maxInterval
		} else {
			backoff = nb
		}
		retries--
	}
	if backoff > maxElapsed {
		backoff = maxElapsed
	}

	backoff *= 1 + bc.Jitter*(rand.Float64()*2-1)
	if backoff < 0 {
		return 0
	}
	return time.Duration(backoff)
}
