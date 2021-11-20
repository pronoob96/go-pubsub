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
	MessageID    uuid.UUID
	MessageData  string
	IsDone       bool
	DeadlineTime time.Time
}

func (s *Subscription) AddMessageToSubscription(message Message, wg *sync.WaitGroup) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Messages = append(s.Messages, message)
	log.Println("Added message", message.MessageData, "to subscriptionID", s.SubscriptionID)
	wg.Done()
}

func (s *Subscription) GetNonProcessedMessage() *Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, msg := range s.Messages {
		if !msg.IsDone {
			msg.DeadlineTime = time.Now().Add(constants.ACK_DEADLINE * time.Millisecond)
			s.Messages[i] = msg
			return &s.Messages[i]
		}
	}
	return nil
}

func (s *Subscription) MarkWorkDone(subscriptionID string, messageID uuid.UUID) *Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, msg := range s.Messages {
		if msg.MessageID == messageID {
			if time.Now().After(msg.DeadlineTime) {
				log.Println("Acknowledgement Failed for message", msg.MessageData, "on subscriptionID", subscriptionID, "time:", time.Now().Sub(msg.DeadlineTime), "late")
				return nil
			} else {
				msg.IsDone = true
				s.Messages[i] = msg
				log.Println("Acknowledged for message", msg.MessageData, "on subscriptionID", subscriptionID, "time:", msg.DeadlineTime.Sub(time.Now()), "early")
				return &s.Messages[i]
			}
		}
	}
	return nil
}
