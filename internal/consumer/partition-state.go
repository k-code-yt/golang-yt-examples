package consumer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
)

type PartitionState struct {
	ID    int32
	Topic *string

	mu    *sync.RWMutex
	state map[kafka.Offset]MsgState

	maxReceived  *atomic.Int32
	lastCommited *atomic.Int32

	ctx    context.Context
	cancel context.CancelFunc
	exitCH chan struct{}
}

func NewPartitionState(tp *kafka.TopicPartition) *PartitionState {
	ctx, cancel := context.WithCancel(context.Background())
	initialLastCommited := tp.Offset - 1
	if tp.Offset == kafka.OffsetBeginning || tp.Offset < 0 {
		initialLastCommited = -1
	}
	lastCommited := new(atomic.Int32)
	lastCommited.Store(int32(initialLastCommited))
	maxReceived := new(atomic.Int32)
	maxReceived.Store(int32(tp.Offset))
	return &PartitionState{
		ID:           tp.Partition,
		Topic:        tp.Topic,
		state:        map[kafka.Offset]MsgState{},
		maxReceived:  maxReceived,
		lastCommited: lastCommited,
		mu:           &sync.RWMutex{},
		ctx:          ctx,
		cancel:       cancel,
		exitCH:       make(chan struct{}),
	}
}

func (ps *PartitionState) commitOffsetLoop(commitDur time.Duration, c *KafkaConsumer) {
	ticker := time.NewTicker(commitDur)
	defer func() {
		close(ps.exitCH)
		ticker.Stop()
		logrus.WithFields(
			logrus.Fields{
				"PRTN": ps.ID,
			},
		).Info("EXIT commitOffsetLoop✅")
	}()
	for {
		logrus.WithField("PRTN", ps.ID).Warn("STARTING-COMMIT-LOOP")
		select {
		case <-ticker.C:
			select {
			case <-ps.ctx.Done():
				return
			default:
			}

			latestToCommit, err := ps.findLatestToCommit()
			if err != nil {
				continue
			}
			tp := kafka.TopicPartition{
				Topic:     ps.Topic,
				Partition: ps.ID,
				Offset:    latestToCommit,
			}
			_, err = c.consumer.CommitOffsets([]kafka.TopicPartition{tp})
			if err != nil {
				fmt.Printf("err commiting offset = %d, prtn = %d, err = %v\n", latestToCommit, ps.ID, err)
				continue
			}

			toCommit := int32(latestToCommit)
			ps.lastCommited.Store(toCommit)

			if toCommit > ps.maxReceived.Load() {
				ps.maxReceived.Store(toCommit)
			}

			ps.mu.Lock()
			logrus.WithFields(
				logrus.Fields{
					"COMMITED_OFFSET": latestToCommit,
					"MAX_OFFSET":      ps.maxReceived.Load(),
					"PRTN":            ps.ID,
					"STATE":           ps.state,
				},
			).Warn("Commited on CRON")
			ps.mu.Unlock()

		case <-ps.ctx.Done():
			return
		}
	}
}

func (ps *PartitionState) findLatestToCommit() (kafka.Offset, error) {
	if ps.maxReceived == nil {
		return -1, fmt.Errorf("maxRec is nil")
	}
	lastCommited := kafka.Offset(ps.lastCommited.Load())
	latestToCommit := kafka.Offset(ps.maxReceived.Load())
	if lastCommited > latestToCommit {
		fmt.Println("❌last commit above maxReceived❌")
		ps.maxReceived.Store(int32(lastCommited))
	}
	if lastCommited == latestToCommit {
		msg := fmt.Sprintf("lastCommit %d == maxReceived in prtn %d -> skipping\n", lastCommited, ps.ID)
		logrus.WithFields(
			logrus.Fields{
				"lastCommit": lastCommited,
				"PRTN":       ps.ID,
			},
		).Info("SKIPPING -> lastCommit == maxReceived")
		return -1, fmt.Errorf("%v", msg)
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	fmt.Printf("PRTN = %d, STATE = %+v\n", ps.ID, ps.state)

	for offset := lastCommited; offset <= latestToCommit; offset++ {
		msgState, exists := ps.state[offset]
		if !exists {
			continue
		}
		if msgState != MsgState_Pending {
			delete(ps.state, offset)
			logrus.WithFields(logrus.Fields{
				"OFFSET": offset,
				"PRTN":   ps.ID,
			}).Info("Removed offset")
			if len(ps.state) == 0 {
				latestToCommit = offset + 1
				break
			}
			continue
		}
		latestToCommit = offset
		break
	}
	return latestToCommit, nil
}
