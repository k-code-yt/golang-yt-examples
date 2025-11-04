package main

import (
	"fmt"
	"math/rand/v2"
	"time"
)

type Throttler struct {
	inputCH  chan *ReqMsg
	outputCH chan *ReqMsg
	rate     time.Duration
}

func NewThrottler(msgPerSecond int, exit chan struct{}) *Throttler {
	rate := time.Second / time.Duration(msgPerSecond)
	t := &Throttler{
		inputCH:  make(chan *ReqMsg, 64),
		outputCH: make(chan *ReqMsg, 64),
		rate:     rate,
	}

	go t.leak(exit)
	return t
}

func (t *Throttler) leak(exit chan struct{}) {
	ticker := time.NewTicker(t.rate)
	defer func() {
		close(t.outputCH)
		ticker.Stop()
	}()

	for {
		select {
		case <-exit:
			close(t.inputCH)
			for msg := range t.inputCH {
				fmt.Printf("draining msg = %v\n", msg)
			}
			fmt.Println("draining done")
			return
		case <-ticker.C:
			select {
			case msg := <-t.inputCH:
				t.outputCH <- msg
			default:
				if rand.IntN(10) < 1 {
					fmt.Println("no msg in throttler -> skipping")
				}
			}
		}
	}
}
