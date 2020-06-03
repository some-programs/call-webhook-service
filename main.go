package main

import (
	"log"

	"github.com/some-programs/call-webhook-service/internal/webhook"
)

func main() {
	c := webhook.CallWebhookWorker{}
	if err := c.Start(); err != nil {
		log.Fatal(err)
	}
}
