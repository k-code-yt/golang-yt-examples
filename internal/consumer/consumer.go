package consumer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/k-code-yt/golang-yt-examples/internal/shared"
	"github.com/sirupsen/logrus"
)

type KafkaConsumer struct {
	consumer *kafka.Consumer
	topic    string
	msgCH    chan *shared.Message
	readyCH  chan struct{}
	exitCH   chan struct{}
	isReady  bool

	msgsStateMap map[kafka.Offset]bool
	mu           *sync.RWMutex

	commitDur time.Duration

	lastCommited kafka.Offset
	maxReceived  *kafka.TopicPartition
}

func NewKafkaConsumer(msgCH chan *shared.Message) (*KafkaConsumer, error) {
	cfg := shared.NewKafkaConfig()
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  cfg.Host,
		"group.id":           cfg.ConsumerGroup,
		"enable.auto.commit": false,
	})

	if err != nil {
		return nil, err
	}

	tp := kafka.TopicPartition{
		Topic:     &cfg.Topic,
		Partition: 0,
	}
	commited, err := c.Committed([]kafka.TopicPartition{tp}, int(time.Second)*5)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	latestComm := kafka.OffsetBeginning
	if len(commited) > 0 && commited[0].Offset != kafka.OffsetInvalid {
		latestComm = commited[len(commited)-1].Offset
	}
	logrus.WithField("OFFSET", latestComm).Info("starting POSITION")
	maxReceived := &kafka.TopicPartition{
		Topic:     tp.Topic,
		Partition: tp.Partition,
		Offset:    latestComm,
	}

	consumer := &KafkaConsumer{
		consumer: c,
		msgCH:    msgCH,
		readyCH:  make(chan struct{}),
		exitCH:   make(chan struct{}),
		isReady:  false,
		topic:    cfg.Topic,

		mu:           new(sync.RWMutex),
		msgsStateMap: map[kafka.Offset]bool{},
		lastCommited: latestComm,
		maxReceived:  maxReceived,
		commitDur:    15 * time.Second,
	}

	consumer.initializeKafkaTopic(cfg.Host, cfg.Topic)

	err = c.Assign([]kafka.TopicPartition{
		{
			Topic:     &consumer.topic,
			Partition: tp.Partition,
			Offset:    latestComm,
		},
	})

	if err != nil {
		return nil, err
	}

	go consumer.commitOffsetLoop()
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
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			continue
		}

		c.appendMsgState(&msg.TopicPartition)
		payload := shared.NewMessage(&msg.TopicPartition, msg.Value)
		c.msgCH <- payload
	}
}

func (c *KafkaConsumer) appendMsgState(tp *kafka.TopicPartition) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.msgsStateMap[tp.Offset] = false
	if c.maxReceived.Offset < tp.Offset {
		c.maxReceived = &kafka.TopicPartition{
			Topic:     tp.Topic,
			Partition: tp.Partition,
			Offset:    tp.Offset,
		}
	}
}

func (c *KafkaConsumer) MarkAsComplete(tp *kafka.TopicPartition) {
	logrus.WithField("OFFSET", tp.Offset).Info("MarkAsComplete")
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgsStateMap[tp.Offset] = true
}

func (c *KafkaConsumer) commitOffsetLoop() {
	ticker := time.NewTicker(c.commitDur)
	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			if c.maxReceived == nil {
				continue
			}
			latestToCommit := *c.maxReceived
			if c.lastCommited > c.maxReceived.Offset {
				panic("last commit above maxReceived")
			}

			if c.lastCommited == c.maxReceived.Offset {
				fmt.Println("lastCommit == maxReceived -> skipping")
				continue
			}

			for offset := c.lastCommited; offset < c.maxReceived.Offset; offset++ {
				completed, exists := c.msgsStateMap[offset]
				if !exists {
					continue
				}
				if completed {
					delete(c.msgsStateMap, offset)
					continue
				}
				latestToCommit.Offset = offset
				break
			}
			c.mu.Unlock()

			if latestToCommit.Offset == c.lastCommited {
				continue
			}

			_, err := c.consumer.CommitOffsets([]kafka.TopicPartition{latestToCommit})
			if err != nil {
				fmt.Printf("err commiting offset = %d, err = %v\n", latestToCommit.Offset, err)
				continue
			}

			c.mu.Lock()
			c.lastCommited = latestToCommit.Offset - 1
			fmt.Printf("state AFTER commit\n")
			for offset, v := range c.msgsStateMap {
				fmt.Printf("off = %d, v =%t\n", offset, v)
			}
			c.mu.Unlock()

			logrus.WithFields(
				logrus.Fields{
					"OFFSET": latestToCommit.Offset - 1,
				},
			).Warn("Commited on CRON")

		case <-c.exitCH:
			return
		}
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
