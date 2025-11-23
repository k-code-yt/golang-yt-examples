package shared

import (
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/k-code-yt/golang-yt-examples/internal/repo"
)

type Message struct {
	Metadata *kafka.TopicPartition
	Event    *repo.Event
}

func NewMessage(metadata *kafka.TopicPartition, data []byte) *Message {
	e := &repo.Event{}
	err := json.Unmarshal(data, e)
	if err != nil {
		panic(fmt.Sprintf("err unmarshalling event = %v\n", err))
	}
	return &Message{
		Metadata: metadata,
		Event:    e,
	}
}
