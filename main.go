package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"cloud.google.com/go/pubsub"
)

var logger = log.New(os.Stdout, "", log.Lshortfile)

func main() {
	ctx := context.Background()
	projectName := os.Getenv("PUBSUB_PROJECT_NAME")
	topicName := os.Getenv("PUBSUB_TOPIC_NAME")
	subName := os.Getenv("PUBSUB_SUBSCRIPTION_NAME")

	// create client
	cli, err := pubsub.NewClient(ctx, projectName)
	if err != nil {
		logger.Fatal(err)
	}

	// set topic
	topic := cli.Topic(topicName)
	defer topic.Stop()

	// publish
	for i := 0; i < 100; i++ {
		message := &pubsub.Message{
			Data: []byte(strconv.Itoa(i)),
		}
		result := topic.Publish(ctx, message)
		id, err := result.Get(ctx)
		if err != nil {
			logger.Fatal(err)
		}
		fmt.Printf("Published a message with a message ID: %s\n", id)
	}

	ctx, _ = context.WithDeadline(ctx, time.Now().Add(5*time.Second))
	eg := errgroup.Group{}
	checkMap := map[string]struct{}{}
	mu := new(sync.Mutex)

	// subscribe in goroutine
	for i := 0; i < 5; i++ {
		sub := cli.Subscription(subName)
		sub.ReceiveSettings.MaxOutstandingMessages = 5
		goNum := i
		eg.Go(func() error {
			return sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
				m.Ack()
				mu.Lock()
				data := string(m.Data)
				if _, ok := checkMap[data]; ok {
					mu.Unlock()
					fmt.Printf("ERROR: %d Receive duplicate message %s", goNum, data)
					return
				}
				checkMap[data] = struct{}{}
				mu.Unlock()
				fmt.Printf("%d Subscribed a message with a message ID: %s, Data: %s\n", goNum, m.ID, data)
			})
		})
	}
	if err := eg.Wait(); err != nil {
		logger.Fatal(err)
	}

	// check message
	for i := 0; i < 1000; i++ {
		if _, ok := checkMap[strconv.Itoa(i)]; !ok {
			fmt.Printf("ERROR: Not Receive message %d", i)
		}
	}
}
