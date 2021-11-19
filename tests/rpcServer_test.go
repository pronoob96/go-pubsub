package tests

import (
	"log"
	"math/rand"
	"net/rpc"
	"pub/client"
	"pub/dtos"
	"testing"
	"time"
)

func subscriber(rpcClient *rpc.Client, subscriptionID string) {
	callback := func(subscriptionID string, message dtos.MessageDto) {
		log.Println("Started Working on message", message.MessageData, "on subscription", subscriptionID)
		time.Sleep(time.Duration(rand.Int31n(7)) * time.Second)
		client.Ack(subscriptionID, message)
	}
	ch := make(chan bool)
	go client.Subscribe(rpcClient, subscriptionID, callback, ch)
	time.Sleep(14 * time.Second)
	client.Unsubscribe(ch)
}

func TestRPCServer(t *testing.T) {
	rpcClient, err := rpc.DialHTTP("tcp", "localhost:4040")
	if err != nil {
		log.Println(err)
		return
	}
	err = client.CreateTopic(rpcClient, "topic1")
	if err != nil {
		return
	}
	defer client.DeleteTopic(rpcClient, "topic1")
	go client.AddSubscription(rpcClient, "topic1", "sub1")
	go client.AddSubscription(rpcClient, "topic1", "sub2")
	err = client.CreateTopic(rpcClient, "topic2")
	if err != nil {
		return
	}
	defer client.DeleteTopic(rpcClient, "topic2")
	go client.AddSubscription(rpcClient, "topic2", "sub3")

	go subscriber(rpcClient, "sub1")
	go subscriber(rpcClient, "sub2")
	go subscriber(rpcClient, "sub3")

	err = client.Publish(rpcClient, "topic1", "publish1")
	if err != nil {
		return
	}
	err = client.Publish(rpcClient, "topic2", "publish2")
	if err != nil {
		return
	}
	err = client.Publish(rpcClient, "topic2", "publish3")
	if err != nil {
		return
	}
	err = client.Publish(rpcClient, "topic1", "publish4")
	if err != nil {
		return
	}
	err = client.Publish(rpcClient, "topic2", "publish5")
	if err != nil {
		return
	}
	err = client.Publish(rpcClient, "topic2", "publish6")
	if err != nil {
		return
	}
	err = client.Publish(rpcClient, "topic1", "publish7")
	if err != nil {
		return
	}
	err = client.Publish(rpcClient, "topic2", "publish8")
	if err != nil {
		return
	}
	err = client.Publish(rpcClient, "topic2", "publish9")
	if err != nil {
		return
	}

	time.Sleep(20 * time.Second)
}
