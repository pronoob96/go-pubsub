package client

import (
	"log"
	"net/rpc"
	"pub/dtos"
)

func CreateTopic(client *rpc.Client, topicID string) error {
	var topic dtos.TopicDto
	err := client.Call("RPC.CreateTopic", topicID, &topic)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func AddSubscription(client *rpc.Client, topicID, subscriptionID string) error {
	var replySub1 dtos.AddSubDto
	subscription := dtos.AddSubDto{TopicID: topicID, SubscriptionID: subscriptionID}
	err := client.Call("RPC.AddSubscription", subscription, &replySub1)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func DeleteSubscription(client *rpc.Client, subscriptionID string) error {
	var replyTopic *dtos.AddSubDto
	err := client.Call("RPC.DeleteSubscription", subscriptionID, &replyTopic)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func DeleteTopic(client *rpc.Client, topicID string) error {
	var topic dtos.TopicDto
	err := client.Call("RPC.DeleteTopic", topicID, &topic)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}
