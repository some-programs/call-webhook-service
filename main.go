package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/go-pa/fenv"
	"github.com/some-programs/call-webhook-service/internal/webhook"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	var flags webhook.Flags
	flags.Register()
	fenv.CommandLinePrefix("CALL_WEBHOOK_")
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

	ctx := context.Background()

	if flags.MetricsAddr != "" {
		webhook.AddMetricsHandlers(ctx, nil)
		webhook.AddDebugHandlers(ctx, nil, flags.PostTimeout)

		srv := http.Server{Addr: flags.MetricsAddr}

		go func() {
			for {
				select {
				case <-ctx.Done():
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					if err := srv.Shutdown(ctx); err != nil {
						log.Printf("HTTP server Shutdown: %v", err)
					}
					return
				}

			}
		}()

		go func() {
			if err := srv.ListenAndServe(); err != http.ErrServerClosed {
				log.Fatalf("HTTP server ListenAndServe: %v", err)
			}
		}()

	}
	messagesCh := make(chan webhook.CallWebHookMessage, flags.Nats.MaxInFlight)

	c := webhook.CallWebhookWorker{}
	if err := c.Start(ctx, flags, messagesCh); err != nil {
		log.Fatal(err)
	}
}
