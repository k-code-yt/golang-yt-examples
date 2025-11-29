package consumer

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/k-code-yt/golang-yt-examples/internal/repo"
	"github.com/k-code-yt/golang-yt-examples/internal/shared"
	"github.com/sirupsen/logrus"
)

type MsgState = int32

const (
	MsgState_Pending MsgState = iota
	MsgState_Success MsgState = iota
	MsgState_Error   MsgState = iota
)

type KafkaConsumer struct {
	consumer *kafka.Consumer
	ID       string
	topic    string

	IsReady bool
	ReadyCH chan struct{}

	MsgCH  chan *shared.Message
	exitCH chan struct{}

	mu           *sync.RWMutex
	msgsStateMap map[int32]*PartitionState

	commitDur time.Duration
	cfg       *shared.KafkaConfig
}

func NewKafkaConsumer(msgCH chan *shared.Message) *KafkaConsumer {
	cfg := shared.NewKafkaConfig()
	ID := repo.GenerateRandomString(15)
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               cfg.Host,
		"group.id":                        cfg.ConsumerGroup,
		"enable.auto.commit":              false,
		"auto.offset.reset":               "earliest",
		"go.application.rebalance.enable": true,
		"partition.assignment.strategy":   string(cfg.ParititionAssignStrategy),
		// "debug":                           "consumer,cgrp,topic",
	})

	if err != nil {
		panic(err)
	}

	consumer := &KafkaConsumer{
		ID:           ID,
		consumer:     c,
		MsgCH:        msgCH,
		ReadyCH:      make(chan struct{}),
		exitCH:       make(chan struct{}),
		IsReady:      false,
		topic:        cfg.DefaultTopic,
		mu:           new(sync.RWMutex),
		commitDur:    10 * time.Second,
		msgsStateMap: map[int32]*PartitionState{},
		cfg:          cfg,
	}

	consumer.initializeKafkaTopic()

	err = c.SubscribeTopics([]string{consumer.topic}, consumer.rebalanceCB)
	if err != nil {
		panic(err)
	}

	return consumer
}

func (c *KafkaConsumer) RunConsumer() struct{} {
	go c.checkReadyToAccept()
	go c.consumeLoop()
	return <-c.exitCH
}

func (c *KafkaConsumer) UpdateState(tp *kafka.TopicPartition, newState MsgState) {
	c.mu.Lock()
	prtnState, ok := c.msgsStateMap[tp.Partition]
	c.mu.Unlock()
	if !ok {
		logrus.Errorf("state is missing for PRTN %d\n", tp.Partition)
		return
	}

	prtnState.mu.Lock()
	prtnState.state[tp.Offset] = newState
	prtnState.mu.Unlock()
}

func (c *KafkaConsumer) assignPrntCB(ev *kafka.AssignedPartitions) error {
	c.mu.Lock()
	committed, err := c.consumer.Committed(ev.Partitions, int(time.Second)*5)
	if err != nil {
		logrus.Errorf("Failed to get committed offsets: %v", err)
		committed = ev.Partitions
	}

	for _, tp := range committed {
		startOffset := tp.Offset
		if startOffset < 0 {
			startOffset = kafka.OffsetBeginning
		}

		logrus.WithFields(logrus.Fields{
			"PRTN":         tp.Partition,
			"START_OFFSET": startOffset,
		}).Info("✅ Assigned partition")

		tpCopy := kafka.TopicPartition{
			Topic:     tp.Topic,
			Partition: tp.Partition,
			Offset:    startOffset,
		}

		prtnState := NewPartitionState(&tpCopy)
		oldPS, exists := c.msgsStateMap[tp.Partition]
		if exists {
			oldPS.cancel()
			<-oldPS.exitCH
			oldPS = nil
		}
		c.msgsStateMap[tp.Partition] = prtnState
		go prtnState.commitOffsetLoop(c.commitDur, c)
	}

	c.mu.Unlock()

	if c.cfg.ParititionAssignStrategy == shared.CooperativeStickyStrategy {
		err = c.consumer.IncrementalAssign(ev.Partitions)
	} else {
		err = c.consumer.Assign(ev.Partitions)
	}
	if err != nil {
		logrus.Errorf("Failed to assign partitions: %v", err)
		return err
	}

	logrus.WithFields(logrus.Fields{
		"count":      len(ev.Partitions),
		"partitions": c.formatPartitions(ev.Partitions),
	}).Info("Successfully assigned partitions")
	return nil
}

func (c *KafkaConsumer) revokePrtnCB(ev *kafka.RevokedPartitions) error {
	var toCommit []kafka.TopicPartition
	for _, tp := range ev.Partitions {
		logrus.WithField("PRTN", tp.Partition).Info("❌ Revoking partition ❌")

		c.mu.RLock()
		partitionState, exists := c.msgsStateMap[tp.Partition]
		c.mu.RUnlock()
		if !exists {
			continue
		}
		partitionState.cancel()
		<-partitionState.exitCH

		latestToCommit, err := partitionState.findLatestToCommit()
		if err != nil {
			continue
		}

		if latestToCommit >= 0 {
			toCommit = append(toCommit, kafka.TopicPartition{
				Topic:     tp.Topic,
				Partition: tp.Partition,
				Offset:    latestToCommit,
			})
		}

		c.mu.Lock()
		delete(c.msgsStateMap, tp.Partition)
		for _, p := range c.msgsStateMap {
			fmt.Printf("PRTN after remove = %d, ConsumerID = %s\n", p.ID, c.ID)
		}
		c.mu.Unlock()
	}

	if len(toCommit) > 0 {
		_, err := c.consumer.CommitOffsets(toCommit)
		if err != nil {
			logrus.Errorf("Failed to commit on revoke: %v", err)
		} else {
			for _, tp := range toCommit {
				logrus.WithFields(logrus.Fields{
					"partition": tp.Partition,
					"offset":    tp.Offset - 1,
				}).Info("✅ Committed before revoke")
			}
		}
	}

	var err error
	if c.cfg.ParititionAssignStrategy == shared.CooperativeStickyStrategy {
		err = c.consumer.IncrementalUnassign(ev.Partitions)
	} else {
		err = c.consumer.Unassign()
	}
	if err != nil {
		logrus.Errorf("Failed to unassign partitions: %v", err)
		return err
	}

	logrus.Infof("Successfully revoked %d partitions", len(ev.Partitions))
	return nil
}

func (c *KafkaConsumer) rebalanceCB(_ *kafka.Consumer, event kafka.Event) error {
	switch ev := event.(type) {
	case kafka.AssignedPartitions:
		err := c.assignPrntCB(&ev)
		if err != nil {
			return err
		}
	case kafka.RevokedPartitions:
		err := c.revokePrtnCB(&ev)
		if err != nil {
			return err
		}
	default:
		logrus.Warnf("Unexpected event type: %T", ev)
	}
	return nil
}

func (c *KafkaConsumer) consumeLoop() {
	defer func() {
		c.consumer.Close()
		close(c.exitCH)
	}()
	firstMsg := true

	for {
		msg, err := c.consumer.ReadMessage(time.Second)
		if err != nil && err.(kafka.Error).IsTimeout() {
			continue
		}
		if err != nil && !err.(kafka.Error).IsTimeout() {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			continue
		}
		if msg == nil {
			continue
		}

		if firstMsg {
			close(c.ReadyCH)
		}

		firstMsg = false

		c.appendMsgState(&msg.TopicPartition)

		msgRequest := shared.NewMessage(&msg.TopicPartition, msg.Value)
		select {
		case c.MsgCH <- msgRequest:
		case <-time.After(5 * time.Second):
			logrus.Errorf("MsgCH blocked for 10s, dropping message offset=%d partition=%d",
				msg.TopicPartition.Offset, msg.TopicPartition.Partition)
			c.UpdateState(&msg.TopicPartition, MsgState_Error)
		}
	}

}

func (c *KafkaConsumer) appendMsgState(tp *kafka.TopicPartition) {
	c.mu.RLock()
	prtnState := c.msgsStateMap[tp.Partition]
	c.mu.RUnlock()

	prtnState.mu.Lock()
	defer prtnState.mu.Unlock()
	prtnState.state[tp.Offset] = MsgState_Pending
	currOffset := int32(tp.Offset)

	if prtnState.maxReceived == nil || prtnState.maxReceived.Load() < currOffset {
		prtnState.maxReceived.Store(currOffset)
	}
}

func (c *KafkaConsumer) initializeKafkaTopic() error {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": c.cfg.Host,
	})
	if err != nil {
		return err
	}
	defer adminClient.Close()
	topicName := c.cfg.DefaultTopic
	log.Printf("Creating topic '%s'...", topicName)
	topicSpec := kafka.TopicSpecification{
		Topic:         topicName,
		NumPartitions: c.cfg.NumPartitions,
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

	return c.waitForTopicReady()
}

func (c *KafkaConsumer) waitForTopicReady() error {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": c.cfg.Host,
	})
	if err != nil {
		return err
	}
	defer adminClient.Close()
	topicName := c.cfg.DefaultTopic
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
		c.IsReady = true
	}()
	for {
		select {
		case <-c.ReadyCH:
			return nil
		default:
			time.Sleep(1 * time.Second)
			IsReady, err := c.readyCheck()
			if err != nil {
				logrus.Error("Error on consumer readycheck")
				return err
			}
			logrus.WithField("STATUS", IsReady).Warn("Consumer ready to accept")

			if IsReady {
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

func (c *KafkaConsumer) formatPartitions(partitions []kafka.TopicPartition) string {
	parts := make([]string, len(partitions))
	for i, p := range partitions {
		parts[i] = fmt.Sprintf("%d@%d", p.Partition, p.Offset)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}
