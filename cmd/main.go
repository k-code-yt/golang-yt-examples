package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/k-code-yt/golang-yt-examples/internal/consumer"
	"github.com/k-code-yt/golang-yt-examples/internal/producer"
	"github.com/k-code-yt/golang-yt-examples/internal/repo"
	"github.com/k-code-yt/golang-yt-examples/internal/shared"
)

type Server struct {
	producer  *producer.KafkaProducer
	consumer  *consumer.KafkaConsumer
	msgCH     chan *shared.Message
	eventRepo *repo.EventRepo
}

func NewServer(eventRepo *repo.EventRepo) *Server {
	msgCH := make(chan *shared.Message, 64)
	c, err := consumer.NewKafkaConsumer(msgCH)
	if err != nil {
		panic(err)
	}
	return &Server{
		producer:  producer.NewKafkaProducer(""),
		consumer:  c,
		msgCH:     msgCH,
		eventRepo: eventRepo,
	}
}

func (s *Server) produceMsg() {
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()
	for range ticker.C {
		msg := repo.NewEvent()
		b, err := json.Marshal(msg)
		if err != nil {
			panic("unable to marshal json")
		}
		s.producer.Produce(b)
	}
}

func (s *Server) handleMsg(msg *shared.Message) {
	ctx := context.Background()
	rand := rand.IntN(5)
	time.Sleep(time.Duration(rand + 1))
	_, err := s.saveToDB(ctx, msg)
	if err != nil {
		fmt.Println("db err = %v\n", err)
	}
}

// 1. do we need to get -> NO
// 2. do we lock db, higher isolation level -> NO
// 3. ctx + tx -> YES
func (s *Server) saveToDB(ctx context.Context, msg *shared.Message) (string, error) {
	return repo.TxClosure(ctx, s.eventRepo, func(ctx context.Context, tx *sqlx.Tx) (string, error) {
		fmt.Printf("starting DB operation for OFFSET = %d, EventID = %s\n", msg.Metadata.Offset, msg.Event.EventId)
		// TODO -> how to handle insert error
		defer s.consumer.MarkAsComplete(msg.Metadata)
		// business logic
		event := s.eventRepo.Get(ctx, tx, msg.Event.EventId)
		if event != nil {
			eMsg := fmt.Sprintf("offset = %d, eventID %s already existing -> skipping\n", msg.Metadata.Offset, msg.Event.EventId)
			return "", errors.New(eMsg)
		}

		id, err := s.eventRepo.Insert(ctx, tx, msg.Event)
		if err != nil {
			return "", err
		}
		fmt.Printf("INSERT SUCCESS, EventID = %s, Offset = %d\n", id, msg.Metadata.Offset)
		return id, nil
	})
}

func main() {
	db, err := repo.NewDBConn()
	if err != nil {
		panic(err)
	}
	er := repo.NewEventRepo(db)
	s := NewServer(er)
	go s.produceMsg()
	for msg := range s.msgCH {
		go s.handleMsg(msg)
	}
}
