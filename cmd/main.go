package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/k-code-yt/golang-yt-examples/internal/consumer"
	"github.com/k-code-yt/golang-yt-examples/internal/producer"
	"github.com/k-code-yt/golang-yt-examples/internal/repo"
	"github.com/k-code-yt/golang-yt-examples/internal/shared"
)

type Server struct {
	addr      string
	producer  *producer.KafkaProducer
	consumer  *consumer.KafkaConsumer
	eventRepo *repo.EventRepo
	msgCH     chan *shared.Message
}

func NewServer(addr string, eventRepo *repo.EventRepo) *Server {
	return &Server{
		msgCH:     make(chan *shared.Message, 512),
		addr:      addr,
		eventRepo: eventRepo,
	}
}

func (s *Server) addProducer() *Server {
	shouldProduce := os.Getenv("SHOULD_PRODUCE") == "true"
	if !shouldProduce {
		shouldProduce = *flag.Bool("SHOULD_PRODUCE", false, "Enable message production")
		flag.Parse()
	}
	fmt.Printf("SHOULD_PRODUCE = %t\n", shouldProduce)

	if shouldProduce {
		s.producer = producer.NewKafkaProducer()
		go s.produceMsgs()
	}
	return s
}

func (s *Server) addConsumer() *Server {
	c := consumer.NewKafkaConsumer(s.msgCH)
	s.consumer = c
	return s
}

func (s *Server) handleMsg(msg *shared.Message) {
	<-s.consumer.ReadyCH
	r := time.Duration(rand.IntN(5))
	time.Sleep(r * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	_, err := s.saveToDB(ctx, msg)
	if err != nil {
		return
	}
}

func (s *Server) saveToDB(ctx context.Context, msg *shared.Message) (string, error) {
	return repo.TxClosure(ctx, s.eventRepo, func(ctx context.Context, tx *sqlx.Tx) (string, error) {
		id, err := s.eventRepo.Insert(ctx, tx, msg.Event)
		if err != nil {
			exists := repo.IsDuplicateKeyErr(err)
			if exists {
				eMsg := fmt.Sprintf("already exists OFFSET = %d, PRTN = %d, EventID = %s\n", msg.Metadata.Offset, msg.Metadata.Partition, msg.Event.EventId)
				s.consumer.UpdateState(msg.Metadata, consumer.MsgState_Success)
				return "", errors.New(eMsg)
			}
			s.consumer.UpdateState(msg.Metadata, consumer.MsgState_Error)
			return "", err
		}
		s.consumer.UpdateState(msg.Metadata, consumer.MsgState_Success)
		return id, nil
	})
}

func (s *Server) produceMsgs() {
	ticker := time.NewTicker(2 * time.Second)
	for range ticker.C {
		event := repo.NewEvent()
		b, err := json.Marshal(event)
		if err != nil {
			fmt.Printf("err marshaling event = %v\n", err)
			continue
		}
		s.producer.Produce(b)
	}
}

func main() {
	db, err := repo.NewDBConn()
	if err != nil {
		panic(fmt.Sprintf("unable to conn to db, err = %v\n", err))
	}
	defer db.Close()

	er := repo.NewEventRepo(db)
	s := NewServer(":7576", er).addConsumer().addProducer()

	go func() {
		for msg := range s.msgCH {
			go s.handleMsg(msg)
		}
	}()

	s.consumer.RunConsumer()
}
