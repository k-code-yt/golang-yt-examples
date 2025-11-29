package producer

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/k-code-yt/golang-yt-examples/internal/shared"
	"github.com/sirupsen/logrus"
)

type KafkaProducer struct {
	producer *kafka.Producer
	topic    string
}

func NewKafkaProducer() *KafkaProducer {
	cfg := shared.NewKafkaConfig()

	topic := cfg.DefaultTopic
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": cfg.Host})
	if err != nil {
		panic(err)
	}

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					logrus.WithFields(logrus.Fields{
						"PRTN":   ev.TopicPartition.Partition,
						"OFFSET": ev.TopicPartition.Offset,
					}).Info("Delivered message")
				}
			}
		}
	}()

	return &KafkaProducer{
		producer: p,
		topic:    topic,
	}
}

func (p *KafkaProducer) Produce(msg []byte) {
	cfg := shared.NewKafkaConfig()
	topic := cfg.DefaultTopic
	p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          msg,
	}, nil)

}
