package dtos

import (
	"github.com/google/uuid"
)

type TopicDto struct {
	TopicID       string
	Subscriptions map[string]*SubscriptionDto
}

type SubscriptionDto struct {
	SubscriptionID string
	Messages       []MessageDto
}

type AddSubDto struct {
	TopicID        string
	SubscriptionID string
}

type PublishDto struct {
	TopicID string
	Message MessageDto
}

type AckDto struct {
	SubscriptionID string
	MessageID      uuid.UUID
}

type MessageDto struct {
	MessageID   uuid.UUID
	MessageData string
}
