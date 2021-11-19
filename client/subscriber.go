package client

import (
	"log"
	"net/rpc"
	"pub/constants"
	"pub/dtos"
	"time"
)

func Subscribe(client *rpc.Client, subscriptionID string, subscriberFunc func(string, dtos.MessageDto), ch chan bool) {
	for {
		select {
		case <-ch:
			log.Println("Exited from subscription", subscriptionID)
			return
		default:
			var replyMessage dtos.MessageDto
			err := client.Call("RPC.GetNonProcessedMessage", subscriptionID, &replyMessage)
			if err != nil {
				log.Println(err)
			} else {
				subscriberFunc(subscriptionID, replyMessage)
			}
			time.Sleep(constants.SUBSCRIBER_POLL_RATE * time.Millisecond)
		}
	}
}

func Unsubscribe(ch chan bool) {
	ch <- true
}

func Ack(subscriptionID string, message dtos.MessageDto) {
	ackDto := dtos.AckDto{SubscriptionID: subscriptionID, MessageID: message.MessageID}
	var reply dtos.AckDto
	client, _ := rpc.DialHTTP("tcp", "localhost:4040")
	_ = client.Call("RPC.Ack", ackDto, &reply)
	log.Println("Acknowledgment sent for message", message.MessageData, "on subscriptionID", subscriptionID)
}
