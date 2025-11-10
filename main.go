package main

import (
	"fmt"
	"log"
	"net/http"
)

var (
	HTTPPort             string  = ":7571"
	MaxTokens            float64 = 2
	RefillRatePerSeconds float64 = 0.05
)

type HTTPServer struct {
	rl *ClientRateLimiter
}

func NewHTTPServer(rl *ClientRateLimiter) *HTTPServer {
	return &HTTPServer{
		rl: rl,
	}
}

func (s *HTTPServer) RunServer() {
	http.HandleFunc("/rl", s.handleReq)
	fmt.Println("server is running on = ", HTTPPort)
	log.Fatal(http.ListenAndServe(HTTPPort, nil))
}

func (s *HTTPServer) handleReq(w http.ResponseWriter, r *http.Request) {
	cID := r.Header.Get("x-client-id")
	if cID == "" {
		http.Error(w, "x-client-id is required", http.StatusBadRequest)
		return
	}
	isAllowed := s.rl.Allow(cID)

	if !isAllowed {
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func main() {
	rl := NewClientRateLimiter()
	s := NewHTTPServer(rl)
	s.RunServer()
}
