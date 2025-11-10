package main

import (
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type TestClient struct {
	id       int
	reqCount int
	rejCount *atomic.Int64
}

func NewTestClient(id int, reqCount int) *TestClient {
	return &TestClient{
		id:       id,
		reqCount: reqCount,
		rejCount: new(atomic.Int64),
	}
}

func (c *TestClient) sendReqs(gWG *sync.WaitGroup) {
	defer gWG.Done()
	wg := sync.WaitGroup{}
	wg.Add(c.reqCount)
	fmt.Printf("cID %d sending N# %d reqs\n", c.id, c.reqCount)
	for range c.reqCount {
		go func() {
			defer wg.Done()
			req, err := http.NewRequest("GET", fmt.Sprintf("http://localhost%s/rl", HTTPPort), nil)
			if err != nil {
				log.Fatal(err)
			}

			req.Header.Set("x-client-id", strconv.Itoa(c.id))

			httpClient := http.Client{}

			r, err := httpClient.Do(req)
			if err != nil {
				log.Fatal(err)
			}
			defer r.Body.Close()

			status := r.StatusCode

			if status == http.StatusTooManyRequests {
				c.rejCount.Add(1)
			}
			fmt.Printf("go response cID = %d, status = %d\n", c.id, status)
		}()
	}
	wg.Wait()
}

func TestRateLimiter(t *testing.T) {
	clientCount := 50
	clients := []*TestClient{}
	minReqs := 50
	wg := new(sync.WaitGroup)

	wg.Add(clientCount)

	rl := NewClientRateLimiter()
	s := NewHTTPServer(rl)
	go s.RunServer()
	time.Sleep(1 * time.Second)

	for idx := range clientCount {
		reqCount := minReqs + rand.IntN(minReqs)
		c := NewTestClient(idx, reqCount)
		clients = append(clients, c)
		go c.sendReqs(wg)
	}

	wg.Wait()

	for _, c := range clients {
		rc := c.rejCount.Load()
		assert.Greater(t, int(rc), 0, "rejected count is above 0")
		assert.Greater(t, c.reqCount, int(rc), "request count is above rejected count")
	}

	time.Sleep(2 * time.Second)
	wg.Add(1)
	c1 := clients[0]
	go c1.sendReqs(wg)
	wg.Wait()

	time.Sleep(2 * time.Second)

	s.rl.mu.Lock()
	limitersCount := len(s.rl.limiters)
	s.rl.mu.Unlock()

	assert.Equal(t, 1, limitersCount)
	fmt.Println("EXITING TEST")
}
