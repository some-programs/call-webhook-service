package main

import (
	"log"
	"math/rand"
	"time"

	"github.com/some-programs/call-webhook-service/internal/webhook"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
	c := webhook.CallWebhookWorker{}
	if err := c.Start(); err != nil {
		log.Fatal(err)
	}
}
