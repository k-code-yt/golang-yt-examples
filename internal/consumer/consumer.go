package consumer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/k-code-yt/golang-yt-examples/internal/shared"
	"github.com/sirupsen/logrus"
)

type KafkaConsumer struct {
	consumer *kafka.Consumer
	topic    string
	msgCH    chan<- string
	readyCH  chan struct{}
	isReady  bool
}

func NewKafkaConsumer(msgCH chan<- string) (*KafkaConsumer, error) {
	cfg := shared.NewKafkaConfig()
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.Host,
		"group.id":          cfg.ConsumerGroup,
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		return nil, err
	}

	consumer := &KafkaConsumer{
		consumer: c,
		topic:    cfg.Topic,
		msgCH:    msgCH,
		readyCH:  make(chan struct{}),
		isReady:  false,
	}
	consumer.initializeKafkaTopic(cfg.Host, cfg.Topic)

	err = c.SubscribeTopics([]string{cfg.Topic}, nil)

	if err != nil {
		return nil, err
	}

	go consumer.checkReadyToAccept()
	go consumer.readMsgLoop()

	return consumer, nil
}

func (c *KafkaConsumer) readMsgLoop() {
	defer c.consumer.Close()
	for {
		msg, err := c.consumer.ReadMessage(time.Second)
		if err != nil && err.(kafka.Error).IsTimeout() {
			continue
		}
		if err != nil && !err.(kafka.Error).IsTimeout() {
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			continue
		}

		payload := msg.Value
		c.msgCH <- string(payload)
	}
}

func (c *KafkaConsumer) initializeKafkaTopic(brokers, topicName string) error {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	if err != nil {
		return err
	}
	defer adminClient.Close()

	log.Printf("Creating topic '%s'...", topicName)
	topicSpec := kafka.TopicSpecification{
		Topic:             topicName,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{topicSpec})
	if err != nil {
		return err
	}

	for _, result := range results {
		if result.Error.Code() == kafka.ErrTopicAlreadyExists {
			logrus.Infof("Topic already exists: %v", result.Error)
			continue
		}
		if result.Error.Code() != kafka.ErrNoError {
			return fmt.Errorf("failed to create topic: %v", result.Error)
		}
		log.Printf("Topic '%s' created successfully", result.Topic)
	}

	return c.waitForTopicReady(brokers, topicName)
}

func (c *KafkaConsumer) waitForTopicReady(brokers, topicName string) error {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	if err != nil {
		return err
	}
	defer adminClient.Close()

	for {
		time.Sleep(1 * time.Second)
		metadata, err := adminClient.GetMetadata(&topicName, false, 5000)

		if err != nil {
			logrus.Errorf("Metadata fetch failed %v\n", err)
			continue
		}

		topicMeta, exists := metadata.Topics[topicName]
		if !exists {
			continue
		}

		if len(topicMeta.Partitions) > 0 {
			allPartitionsReady := true
			for _, partition := range topicMeta.Partitions {
				if partition.Error.Code() != kafka.ErrNoError {
					allPartitionsReady = false
					break
				}
				if partition.Leader == -1 {
					allPartitionsReady = false
					break
				}
			}

			logrus.WithField("IS_INITIALIZED", allPartitionsReady).Info("Cosumer Topic")

			if allPartitionsReady {
				return nil
			}
		}
	}
}

func (c *KafkaConsumer) checkReadyToAccept() error {
	defer func() {
		c.isReady = true
	}()
	for {
		select {
		case <-c.readyCH:
			return nil
		default:
			time.Sleep(1 * time.Second)
			isReady, err := c.readyCheck()
			if err != nil {
				logrus.Error("Error on consumer readycheck")
				return err
			}
			logrus.WithField("STATUS", isReady).Warn("Consumer ready to accept")

			if isReady {
				return nil
			}
		}

	}
}

func (c *KafkaConsumer) readyCheck() (bool, error) {
	assignment, err := c.consumer.Assignment()
	if err != nil {
		logrus.Errorf("Failed to get assignment: %v", err)
		return false, err
	}

	return len(assignment) > 0, nil
}
