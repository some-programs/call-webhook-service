package webhook

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	promMsgsInvalidFormat = promauto.NewCounter(prometheus.CounterOpts{
		Name: "callwebhook_messages_decode_error_total",
		Help: "",
	})
	promMsgsRreceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "callwebhook_received_messages_total",
		Help: "",
	})
	promPostSuccess = promauto.NewCounter(prometheus.CounterOpts{
		Name: "callwebhook_post_success_total",
		Help: "",
	})
	promPostRetries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "callwebhook_post_retries_total",
		Help: "",
	})
	promPostFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "callwebhook_post_failed_total",
		Help: "",
	})
	promRetryBufferSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "callwebhook_retry_buffer_size",
		Help: "",
	})
	promHighestSuccessfulSequence = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "callwebhook_seq_success_max",
		Help: "",
	})
	promHighestReceivedSequence = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "callwebhook_seq_recv_max",
		Help: "",
	})
	promActiveHTTPRequests = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "callwebhook_active_http_requests",
		Help: "",
	})
)
