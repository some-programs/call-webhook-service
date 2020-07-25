package webhook

import (
	"context"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)



func AddMetricsHandlers(ctx context.Context, mux *http.ServeMux) {
	if mux == nil {
		mux = http.DefaultServeMux
	}
	mux.Handle("/metrics", promhttp.Handler())

}

func AddDebugHandlers(ctx context.Context, mux *http.ServeMux, postTimeout time.Duration) {

	if mux == nil {
		mux = http.DefaultServeMux
	}

	mux.HandleFunc("/webhook/ok", func(w http.ResponseWriter, r *http.Request) {
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
	mux.HandleFunc("/webhook/500", func(w http.ResponseWriter, r *http.Request) {
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

	mux.HandleFunc("/webhook/rand", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
		if rand.Float64() > 0.95 {
			return
		}
		if rand.Float64() > 0.95 {
			time.Sleep(postTimeout + time.Second)
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

}
