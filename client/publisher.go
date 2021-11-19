package client

import (
	"log"
	"net/rpc"
	"pub/dtos"
)

func Publish(client *rpc.Client, topicID, message string) error {
	var replyDto dtos.PublishDto
	publishDto1 := dtos.PublishDto{TopicID: topicID, Message: dtos.MessageDto{MessageData: message}}
	err := client.Call("RPC.Publish", publishDto1, &replyDto)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}
