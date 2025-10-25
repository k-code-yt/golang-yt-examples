package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var (
	WSPort = ":3223"
)

type MsgType string

const (
	MsgType_Broadcast MsgType = "broadcast"
)

type ReqMsg struct {
	MsgType MsgType
	Client  *Client
	Data    string
}

type RespMsg struct {
	MsgType  MsgType
	Data     string
	SenderID string
}

func NewRespMsg(msg *ReqMsg) *RespMsg {
	return &RespMsg{
		MsgType:  msg.MsgType,
		Data:     msg.Data,
		SenderID: msg.Client.ID,
	}
}

type Client struct {
	ID    string
	mu    *sync.RWMutex
	conn  *websocket.Conn
	msgCH chan *RespMsg
	done  chan struct{}
}

func NewClient(conn *websocket.Conn) *Client {
	ID := rand.Text()[:9]
	return &Client{
		ID:    ID,
		mu:    new(sync.RWMutex),
		conn:  conn,
		msgCH: make(chan *RespMsg, 64),
		done:  make(chan struct{}),
	}

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

		srv.broadcastCH <- msg
	}
}

type Server struct {
	clients       map[string]*Client
	mu            *sync.RWMutex
	joinServerCH  chan *Client
	leaveServerCH chan *Client
	broadcastCH   chan *ReqMsg
}

func NewServer() *Server {
	return &Server{
		clients:       map[string]*Client{},
		mu:            new(sync.RWMutex),
		joinServerCH:  make(chan *Client, 64),
		leaveServerCH: make(chan *Client, 64),
		broadcastCH:   make(chan *ReqMsg, 64),
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
		case msg := <-s.broadcastCH:
			cls := map[string]*Client{}
			for id, c := range s.clients {
				if id != msg.Client.ID {
					cls[id] = c
				}
			}
			go s.broadcast(msg, cls)
		}
	}
}

func (s *Server) joinServer(c *Client) {
	s.clients[c.ID] = c
	fmt.Printf("client joined the server, cID = %s\n", c.ID)
}

func (s *Server) leaveServer(c *Client) {
	delete(s.clients, c.ID)
	fmt.Printf("client left the server, cID = %s\n", c.ID)
}

func (s *Server) broadcast(msg *ReqMsg, cls map[string]*Client) {
	resp := NewRespMsg(msg)
	for _, c := range cls {
		c.msgCH <- resp
	}

	fmt.Println("broadcast was sent")
}

func createWSServer() {
	s := NewServer()
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
// [] join room
// [] leave room
// [] Send room msg -> no race conditions
// [] meamory leakage -> grafana/prom
// [] test performance
func main() {
	createWSServer()
}
