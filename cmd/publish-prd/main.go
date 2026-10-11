// Command publish-prd publishes the PRD document to Kafka.
//
// Usage: ./publish-prd <kafka-broker> <topic> <path-to-prd>
//
// The PRD is published as a Kafka message so the pipeline can demonstrate
// end-to-end flow: file → Kafka → consumer → handler.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"flowguard/internal/kafka"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <topic> <prd-path>\n", os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]
	topic := os.Args[2]
	prdPath := os.Args[3]

	prdData, err := os.ReadFile(prdPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read PRD: %v\n", err)
		os.Exit(1)
	}

	producer, err := kafka.New(kafka.Config{
		Brokers: []string{broker},
		Timeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create producer: %v\n", err)
		os.Exit(1)
	}
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := producer.Publish(ctx, topic, "prd", string(prdData)); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to publish PRD: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("PRD published to %s (topic=%s, size=%d bytes)\n", broker, topic, len(prdData))
}