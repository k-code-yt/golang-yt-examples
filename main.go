package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/gorilla/websocket"
)

var (
	WSPort = ":3223"
)

type MsgType string

const (
	MsgType_Broadcast MsgType = "broadcast"
	MsgType_JoinRoom  MsgType = "join-room"
	MsgType_LeaveRoom MsgType = "leave-room"
	MsgType_RoomMsg   MsgType = "room-message"
)

const (
	DefaultMsgPerSecond = 10
)

type ReqMsg struct {
	MsgType MsgType     `json:"type"`
	Data    interface{} `json:"data"`
	RoomID  string      `json:"roomID"`
	Client  *Client
}

type RespMsg struct {
	MsgType  MsgType     `json:"type"`
	Data     interface{} `json:"data"`
	SenderID string      `json:"senderID"`
	RoomID   string      `json:"roomID"`
}

func NewRespMsg(msg *ReqMsg) *RespMsg {
	return &RespMsg{
		MsgType:  msg.MsgType,
		Data:     msg.Data,
		SenderID: msg.Client.ID,
		RoomID:   msg.RoomID,
	}
}

type Client struct {
	ID        string
	mu        *sync.RWMutex
	conn      *websocket.Conn
	msgCH     chan *RespMsg
	done      chan struct{}
	throttler *Throttler
}

func NewClient(conn *websocket.Conn) *Client {
	ID := rand.Text()[:9]
	c := &Client{
		ID:    ID,
		mu:    new(sync.RWMutex),
		conn:  conn,
		msgCH: make(chan *RespMsg, 64),
		done:  make(chan struct{}),
	}

	t := NewThrottler(DefaultMsgPerSecond, c.done)
	c.throttler = t
	return c
}

func (c *Client) writeMsgLoop() {
	defer c.conn.Close()
	for {
		select {
		case <-c.done:
			return
		case msg := <-c.msgCH:
			err := c.conn.WriteJSON(msg)
			if err != nil {
				fmt.Printf("error sending msg to clientID = %s\n", c.ID)
				return
			}
		}
	}
}

func (c *Client) readMsgLoop(srv *Server) {
	defer func() {
		close(c.done)
		srv.leaveServerCH <- c
	}()

	go c.throttlingLoop(srv.handleMsg)

	for {
		_, b, err := c.conn.ReadMessage()
		if err != nil {
			return
		}

		msg := new(ReqMsg)
		err = json.Unmarshal(b, msg)
		if err != nil {
			fmt.Printf("unable to unmarshal the msg %v\n", err)
			continue
		}
		msg.Client = c
		// --- done with msg
		c.throttler.inputCH <- msg
	}
}

func (c *Client) throttlingLoop(handleMsg func(msg *ReqMsg)) {
	for {
		select {
		case <-c.done:
			return
		case msg, ok := <-c.throttler.outputCH:
			if !ok {
				return
			}
			handleMsg(msg)
		}
	}
}

func (srv *Server) handleMsg(msg *ReqMsg) {
	switch msg.MsgType {
	case MsgType_Broadcast:
		srv.broadcastCH <- msg
	case MsgType_JoinRoom:
		srv.joinRoomCH <- msg
	case MsgType_LeaveRoom:
		srv.leaveRoomCH <- msg
	case MsgType_RoomMsg:
		srv.roomMsgCH <- msg
	default:
		fmt.Println("unknown msg type -> ignoring it!")
		// TODO -> return err to client?
	}

}

type Room struct {
	clients map[string]*Client
	ID      string

	// test data
	clientsCount *atomic.Int64
}

func NewRoom(id string) *Room {
	return &Room{
		ID:           id,
		clients:      map[string]*Client{},
		clientsCount: new(atomic.Int64),
	}
}

type Server struct {
	clients       map[string]*Client
	rooms         map[string]*Room
	mu            *sync.RWMutex
	joinServerCH  chan *Client
	leaveServerCH chan *Client
	broadcastCH   chan *ReqMsg
	joinRoomCH    chan *ReqMsg
	leaveRoomCH   chan *ReqMsg
	roomMsgCH     chan *ReqMsg

	reqCH         chan struct{}
	clientCountCH chan int
}

func NewServer() *Server {
	return &Server{
		clients:       map[string]*Client{},
		rooms:         map[string]*Room{},
		mu:            new(sync.RWMutex),
		joinServerCH:  make(chan *Client, 64),
		leaveServerCH: make(chan *Client, 64),
		broadcastCH:   make(chan *ReqMsg, 64),
		joinRoomCH:    make(chan *ReqMsg, 64),
		leaveRoomCH:   make(chan *ReqMsg, 64),
		roomMsgCH:     make(chan *ReqMsg, 64),

		// for testing
		reqCH:         make(chan struct{}),
		clientCountCH: make(chan int),
	}
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{
		ReadBufferSize:  512,
		WriteBufferSize: 512,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Printf("Error on HTTP conn upgrade %v\n", err)
		return
	}

	client := NewClient(conn)
	s.joinServerCH <- client

	go client.writeMsgLoop()
	go client.readMsgLoop(s)
}

func (s *Server) AcceptLoop() {
	for {
		select {
		case c := <-s.joinServerCH:
			s.joinServer(c)
		case c := <-s.leaveServerCH:
			s.leaveServer(c)
		case msg := <-s.joinRoomCH:
			s.joinRoom(msg)
		case msg := <-s.leaveRoomCH:
			s.leaveRoom(msg)
		case msg := <-s.roomMsgCH:
			s.roomMsg(msg)
		case msg := <-s.broadcastCH:
			s.broadcast(msg)
		case <-s.reqCH:
			s.clientCountCH <- len(s.clients)
		}
	}
}

func (s *Server) joinServer(c *Client) {
	s.clients[c.ID] = c
	fmt.Printf("client joined the server, cID = %s\n", c.ID)
}

func (s *Server) leaveServer(c *Client) {
	delete(s.clients, c.ID)

	for _, r := range s.rooms {
		_, ok := r.clients[c.ID]
		if ok {
			delete(r.clients, c.ID)
		}
	}

	fmt.Printf("client left the server, cID = %s\n", c.ID)
}

func (s *Server) broadcast(msg *ReqMsg) {
	cls := map[string]*Client{}
	for id, c := range s.clients {
		if id != msg.Client.ID {
			cls[id] = c
		}
	}

	go s.sendMsg(msg, cls)
	fmt.Println("broadcast was sent")
}

func (s *Server) roomMsg(msg *ReqMsg) {
	rID := msg.RoomID
	room, ok := s.rooms[rID]
	if !ok {
		fmt.Printf("the room does not exist -> cannot send msg into it")
		return
	}

	_, ok = room.clients[msg.Client.ID]
	if !ok {
		fmt.Printf("the cleint = %s does not belong to the room %s -> cannot send msg into it\n", msg.Client.ID, rID)
		return

	}

	cls := map[string]*Client{}
	for id, c := range room.clients {
		if id != msg.Client.ID {
			cls[id] = c
		}
	}

	go s.sendMsg(msg, cls)
	fmt.Printf("the cleint = %s sent msg to the room %s\n", msg.Client.ID, rID)
}

func (s *Server) sendMsg(msg *ReqMsg, cls map[string]*Client) {
	resp := NewRespMsg(msg)
	for _, c := range cls {
		c.msgCH <- resp
	}
	cls = nil
}

func (s *Server) joinRoom(msg *ReqMsg) {
	rID := msg.RoomID
	room, ok := s.rooms[rID]
	if !ok {
		room = NewRoom(rID)
		s.rooms[rID] = room
	}

	room.clients[msg.Client.ID] = msg.Client
	room.clientsCount.Add(1)
	fmt.Printf("client joined the Room %s, cID = %s\n", rID, msg.Client.ID)
}

func (s *Server) leaveRoom(msg *ReqMsg) {
	rID := msg.RoomID
	room, ok := s.rooms[rID]
	if !ok {
		fmt.Printf("cannot leave room that does not exist rID = %s, cID = %s\n", rID, msg.Client.ID)
		return
	}
	delete(room.clients, msg.Client.ID)
	room.clientsCount.Add(-1)
	fmt.Printf("client left the room rID = %s, cID = %s\n", rID, msg.Client.ID)
}

type TestRoomResults struct {
	RoomID       string
	ClientsCount int
}

func (s *Server) GetRoomTestResults(roomID string) *TestRoomResults {
	room := s.rooms[roomID]
	return &TestRoomResults{
		RoomID:       roomID,
		ClientsCount: int(room.clientsCount.Load()),
	}
}

func (s *Server) GetServerTestResults() int {
	s.reqCH <- struct{}{}
	return <-s.clientCountCH
}

func (s *Server) createWSServer() {
	go s.AcceptLoop()
	http.HandleFunc("/", s.handleWS)

	fmt.Printf("starting server on port: %s\n", WSPort)
	log.Fatal(http.ListenAndServe(WSPort, nil))
}

// TODO
// [x] HTTP server
// [x] Upgrade it to WS once client connects
// [x] Add WS client
// [x] Add newly connected ws to server
// [x] Remove client on disconnect
// [x] Send broadcast msg -> no race conditions
// [x] join room
// [x] leave room
// [x] Send room msg -> no race conditions
// -----
// [] throttling
// -----
// [] rate-limiting
// [] test performance -> channels vs locks
// [] meamory leakage -> grafana/prom
func main() {
	s := NewServer()
	s.createWSServer()
}
