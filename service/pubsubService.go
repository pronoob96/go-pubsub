package service

import (
	"errors"
	"github.com/google/uuid"
	"github.com/jinzhu/copier"
	"log"
	"pub/data"
	"pub/dtos"
)

type RPC int

func (a *RPC) CreateTopic(topicID string, replyTopic *dtos.TopicDto) error {
	if topicID == "" {
		log.Println("topicID empty")
		return nil
	}
	topic := data.Topic{TopicID: topicID, Subscriptions: make(map[string]*data.Subscription)}
	data.TopicList[topicID] = topic
	log.Println("Added topic:", topicID)
	err := copier.Copy(replyTopic, &topic)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func (a *RPC) DeleteTopic(topicID string, replyTopic *dtos.TopicDto) error {
	if topicID == "" {
		log.Println("topicID empty")
		return nil
	}
	for _, topic := range data.TopicList {
		if topic.TopicID == topicID {
			delete(data.TopicList, topicID)
			log.Println("Deleted topic:", topicID)
			err := copier.Copy(replyTopic, &topic)
			if err != nil {
				log.Println(err)
				return err
			}
			return nil
		}
	}
	log.Println("Topic not found")
	return nil
}

func (a *RPC) AddSubscription(addsubDto dtos.AddSubDto, replyDto *dtos.AddSubDto) error {
	if addsubDto.SubscriptionID == "" {
		log.Println("SubscriptionID empty")
		return nil
	}
	topic := data.TopicList[addsubDto.TopicID]
	if topic.TopicID == "" {
		log.Println("Topic not found")
		return nil
	}
	topic.Subscriptions[addsubDto.SubscriptionID] = &data.Subscription{SubscriptionID: addsubDto.SubscriptionID}
	log.Println("Added subscription", addsubDto.SubscriptionID, "to", addsubDto.TopicID)
	err := copier.Copy(replyDto, topic.Subscriptions[addsubDto.SubscriptionID])
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func (a *RPC) DeleteSubscription(subscriptionID string, replyDto *dtos.AddSubDto) error {
	if subscriptionID == "" {
		log.Println("SubscriptionID empty")
		return nil
	}
	for _, topic := range data.TopicList {
		for _, sub := range topic.Subscriptions {
			if sub.SubscriptionID == subscriptionID {
				delete(topic.Subscriptions, subscriptionID)
				*replyDto = dtos.AddSubDto{TopicID: topic.TopicID, SubscriptionID: subscriptionID}
				log.Println("Subscription deleted")
				return nil
			}
		}
	}
	log.Println("Subscription not found")
	return nil
}

func (a *RPC) Publish(publishdto dtos.PublishDto, replydto *dtos.PublishDto) error {
	topic := data.TopicList[publishdto.TopicID]
	if topic.TopicID == "" {
		return errors.New("TopicID not found")
	}
	var message data.Message
	err := copier.Copy(&message, &publishdto.Message)
	if err != nil {
		log.Println(err)
		return err
	}
	message.MessageID = uuid.New()
	for _, v := range data.TopicList[publishdto.TopicID].Subscriptions {
		go v.AddMessageToSubscription(message)
	}
	log.Println("Published message", publishdto.Message.MessageData, "to", publishdto.TopicID)
	err = copier.Copy(replydto, &message)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func (a *RPC) Ack(ackDto dtos.AckDto, replyAckDto *dtos.AckDto) error {
	for _, topic := range data.TopicList {
		for _, sub := range topic.Subscriptions {
			if sub.SubscriptionID == ackDto.SubscriptionID {
				replyMsg := sub.MarkWorkDone(sub.SubscriptionID, ackDto.MessageID)
				if replyMsg == nil {
					return errors.New("time limit to acknowledge exceeded")
				}
				err := copier.Copy(replyAckDto, replyMsg)
				if err != nil {
					log.Println(err)
					return err
				}
				return nil
			}
		}
	}
	return nil
}

func (a *RPC) GetNonProcessedMessage(subscriptionID string, replyMessage *dtos.MessageDto) error {
	for _, topic := range data.TopicList {
		for _, sub := range topic.Subscriptions {
			if sub.SubscriptionID == subscriptionID {
				nonProcessedMessage := sub.GetNonProcessedMessage()
				if nonProcessedMessage == nil {
					return errors.New("no non-processed message found")
				} else {
					err := copier.Copy(replyMessage, nonProcessedMessage)
					if err != nil {
						log.Println(err)
						return err
					}
					return nil
				}
			}
		}
	}
	return errors.New("no non-processed message found")
}
