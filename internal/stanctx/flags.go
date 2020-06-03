package stanctx

import (
	"flag"
	"fmt"
	"time"
)

type Flags struct {
	DurableName string
	MaxInFlight int
	ClusterName string
	NatsURL     string
	AckWait     time.Duration
}

func (f *Flags) Process() error {
	if f.NatsURL == "" {
		return fmt.Errorf("-nats_url can not be empty")
	}
	if f.ClusterName == "" {
		return fmt.Errorf("-nats_cluster_name can not be empty")
	}
	if f.DurableName == "" {
		return fmt.Errorf("-nats_durable_name can not be empty")
	}

	return nil
}

func RegisterFlags(f *Flags) {
	flag.DurationVar(&f.AckWait, "nats_ack_wait", 5*time.Minute, "time before nats resends message")
	flag.StringVar(&f.NatsURL, "nats_url", "nats://localhost:4222", "nats connection url, fmt: nats://derek:pass@localhost:4222")
	flag.StringVar(&f.ClusterName, "nats_cluster_name", "test-cluster", "name of nats cluster")
	flag.StringVar(&f.DurableName, "nats_durable_name", "call_webhook_worker_dev", "name of durable subscription, must be unique for all clients consuming messages")
	flag.IntVar(&f.MaxInFlight, "nats_max_in_flight", 0, "number of unacked nats/stan `messages in flight, 0=same as number of workers")
}
