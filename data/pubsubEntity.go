package data

import (
	"github.com/google/uuid"
	"log"
	"pub/constants"
	"sync"
	"time"
)

var TopicList = map[string]*Topic{}

type Topic struct {
	TopicID       string
	Subscriptions map[string]*Subscription
}

type Subscription struct {
	SubscriptionID string
	Messages       []Message
	mu             sync.Mutex
}

type Message struct {
	MessageID   uuid.UUID
	MessageData string
	IsDone      bool
	Time        time.Time
}

func (s *Subscription) AddMessageToSubscription(message Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Messages = append(s.Messages, message)
	log.Println("Added message", message.MessageData, "to subscriptionID", s.SubscriptionID)
}

func (s *Subscription) GetNonProcessedMessage() *Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	var replymsg *Message
	for i, msg := range s.Messages {
		if !msg.IsDone {
			msg.Time = time.Now().Add(constants.ACK_DEADLINE * time.Millisecond)
			s.Messages[i] = msg
			replymsg = &s.Messages[i]
			break
		}
	}
	return replymsg
}

func (s *Subscription) MarkWorkDone(subscriptionID string, messageID uuid.UUID) *Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	var replymsg *Message
	for i, msg := range s.Messages {
		if msg.MessageID == messageID {
			if time.Now().After(msg.Time) {
				log.Println("Acknowledgement Failed for message", msg.MessageData, "on subscriptionID", subscriptionID, "time:", time.Now().Sub(msg.Time), "late")
				return nil
			} else {
				msg.IsDone = true
				s.Messages[i] = msg
				log.Println("Acknowledged for message", msg.MessageData, "on subscriptionID", subscriptionID, "time:", msg.Time.Sub(time.Now()), "early")
				return &msg
			}
		}
	}
	return replymsg
}
