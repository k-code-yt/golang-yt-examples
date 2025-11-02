package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

var (
	host         = "ws://localhost"
	roomID       = "ROOM_ONE"
	secondRoomID = "ROOM_TWO"
)

type TestConfig struct {
	clientCount    int
	wg             *sync.WaitGroup
	msgCount       *atomic.Int64
	msgCountROne   *atomic.Int64
	msgCountRTwo   *atomic.Int64
	targetMsgCount int
}

type TestClient struct {
	conn   *websocket.Conn
	msgCH  chan *ReqMsg
	ctx    context.Context
	roomID string
}

func NewTestClient(conn *websocket.Conn, ctx context.Context) *TestClient {
	return &TestClient{
		conn:  conn,
		msgCH: make(chan *ReqMsg, 64),
		ctx:   ctx,
	}
}

func (c *TestClient) writeLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case msg := <-c.msgCH:
			err := c.conn.WriteJSON(&msg)
			if err != nil {
				fmt.Printf("error sending msg %v\n", err)
				return
			}
		}
	}
}

func (c *TestClient) roomMsgLoop(counter1 *atomic.Int64, counter2 *atomic.Int64) {
	defer c.conn.Close()
	exit := make(chan struct{})
	go func() {
		defer close(exit)
		for {
			_, b, err := c.conn.ReadMessage()
			if err != nil {
				return
			}
			resp := new(RespMsg)

			err = json.Unmarshal(b, resp)
			if err != nil {
				log.Fatal(err)
			}

			if resp.RoomID == roomID {
				counter1.Add(1)
			} else {
				counter2.Add(1)
			}
		}
	}()

	select {
	case <-exit:
		return
	case <-c.ctx.Done():
		return
	}
}

func (c *TestClient) joinRoom(roomID string) {
	msg := &ReqMsg{
		MsgType: MsgType_JoinRoom,
		Data:    "wanna join room",
		RoomID:  roomID,
	}
	c.msgCH <- msg
}

func (c *TestClient) leaveRoom(roomID string) {
	msg := &ReqMsg{
		MsgType: MsgType_LeaveRoom,
		Data:    "wanna leave room",
		RoomID:  roomID,
	}
	c.msgCH <- msg
}

func (c *TestClient) sendRoomMsg(roomID string) {
	msg := &ReqMsg{
		MsgType: MsgType_RoomMsg,
		Data:    "wanna send room message",
		RoomID:  roomID,
	}
	c.msgCH <- msg
}

func joinServer() *websocket.Conn {
	dialer := websocket.DefaultDialer

	conn, _, err := dialer.Dial(fmt.Sprintf("%s%s", host, WSPort), nil)
	if err != nil {
		log.Fatal(err)
	}
	return conn
}

func DialServer(tc *TestConfig) *websocket.Conn {
	exit := make(chan struct{})
	dialer := websocket.DefaultDialer

	conn, _, err := dialer.Dial(fmt.Sprintf("%s%s", host, WSPort), nil)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			time.Sleep(2 * time.Second)
			if tc.targetMsgCount == int(tc.msgCount.Load()) {
				close(exit)
				return
			}
		}
	}()

	go func() {
		<-exit
		conn.Close()
		tc.wg.Done()
	}()

	go func() {
		for {
			_, b, err := conn.ReadMessage()
			if err != nil {
				return
			}

			if len(b) > 0 {
				tc.msgCount.Add(1)
			}
		}
	}()
	return conn
}

func TestConnection(t *testing.T) {
	s := NewServer()

	go s.createWSServer()
	ctx, cancel := context.WithCancel(context.Background())
	time.Sleep(1 * time.Second)
	clientCount := 500
	brCount := 100

	tc := TestConfig{
		clientCount:    clientCount,
		wg:             new(sync.WaitGroup),
		msgCount:       new(atomic.Int64),
		targetMsgCount: clientCount * brCount,
	}
	tc.wg.Add(tc.clientCount + 1)

	brConn := DialServer(&tc)
	brClient := NewTestClient(brConn, ctx)
	go brClient.writeLoop()

	for range tc.clientCount {
		go DialServer(&tc)
	}
	time.Sleep(1 * time.Second)

	for range brCount {
		msg := ReqMsg{
			MsgType: MsgType_Broadcast,
			Data:    "hello from tests",
		}
		brClient.msgCH <- &msg
	}

	tc.wg.Wait()
	cancel()

	time.Sleep(1 * time.Second)
	fmt.Println("exiting test")
}

// 1. join server
// 2. join room -> we need at least 2
// 3. send room msg -> make sure no race && we got correct count
// 4. leave room -> client count should be 0 in room
func TestRooms(t *testing.T) {
	s := NewServer()
	go s.createWSServer()
	ctx, cancel := context.WithCancel(context.Background())
	time.Sleep(1 * time.Second)
	clientCount := 10
	msgCount := 5

	tc := TestConfig{
		clientCount:    clientCount,
		wg:             new(sync.WaitGroup),
		msgCountROne:   new(atomic.Int64),
		msgCountRTwo:   new(atomic.Int64),
		targetMsgCount: (clientCount - 1) * msgCount,
	}

	clientRTwoCount := 0
	clients := []*TestClient{}
	for idx := range tc.clientCount {
		conn := joinServer()
		c := NewTestClient(conn, ctx)
		go c.roomMsgLoop(tc.msgCountROne, tc.msgCountRTwo)
		go c.writeLoop()

		clients = append(clients, c)
		rID := roomID
		if rand.IntN(10) < 5 || idx == 0 {
			rID = secondRoomID
			clientRTwoCount++
		}
		c.roomID = rID
		c.joinRoom(rID)
	}

	clientROneCount := tc.clientCount - clientRTwoCount
	expectedMsgCount1 := 0
	expectedMsgCount2 := 0

	for idx := range msgCount {
		if clients[idx].roomID == roomID {
			expectedMsgCount1 += clientROneCount - 1
		} else {
			expectedMsgCount2 += clientRTwoCount - 1
		}
	}

	for {
		time.Sleep(1 * time.Second)
		tr1 := s.GetRoomTestResults(roomID)
		tr2 := s.GetRoomTestResults(secondRoomID)
		fmt.Printf("curr client count = %d targer = %d\n", tr1.ClientsCount, clientROneCount)
		fmt.Printf("curr client count = %d targer = %d\n", tr2.ClientsCount, clientRTwoCount)

		if tr1.ClientsCount == clientROneCount && tr2.ClientsCount == clientRTwoCount {
			fmt.Println("exiting clientCount JoinLoop")
			break
		}
	}

	for idx := range msgCount {
		sender := clients[idx]
		sender.sendRoomMsg(sender.roomID)
	}

	for {
		time.Sleep(1 * time.Second)
		currCount1 := tc.msgCountROne.Load()
		currCount2 := tc.msgCountRTwo.Load()
		fmt.Printf("curr msg count = %d targer = %d\n", currCount1, expectedMsgCount1)
		fmt.Printf("curr msg count = %d targer = %d\n", currCount2, expectedMsgCount2)

		if currCount1 == int64(expectedMsgCount1) && currCount2 == int64(expectedMsgCount2) {
			break
		}
	}

	for _, c := range clients {
		go c.leaveRoom(c.roomID)
	}

	for {
		time.Sleep(1 * time.Second)
		tr := s.GetRoomTestResults(roomID)
		fmt.Println("curr client count = ", tr.ClientsCount)
		if tr.ClientsCount == 0 {
			fmt.Println("exiting clientCount LeaveLoop")
			break
		}
	}

	cancel()

	for {
		time.Sleep(1 * time.Second)
		clientCount := s.GetServerTestResults()
		fmt.Println("client count = ", clientCount)
		if clientCount == 0 {
			break
		}
	}

	fmt.Println("exiting test")
}
