package main

import (
	"fmt"
	"math"
	"sync"
	"time"
)

type RateLimiter struct {
	tokens     float64
	maxTokens  float64
	mu         *sync.RWMutex
	refillRate float64
	lastRefill time.Time
}

func NewRateLimiter(maxTokens float64, refillRatePerSeconds float64) *RateLimiter {
	return &RateLimiter{
		tokens:     maxTokens,
		maxTokens:  maxTokens,
		mu:         new(sync.RWMutex),
		refillRate: refillRatePerSeconds,
		lastRefill: time.Now(),
	}
}

func (r *RateLimiter) Allow() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refill()

	fmt.Printf("curr Tokens = %f\n", r.tokens)
	if r.tokens >= 1 {
		r.tokens--
		return true
	}

	return false
}

func (r *RateLimiter) refill() {
	ellapsed := time.Since(r.lastRefill).Seconds()
	toRefillTokens := ellapsed * r.refillRate
	r.tokens = math.Min(r.maxTokens, r.tokens+toRefillTokens)
	r.lastRefill = time.Now()
}

type ClientRateLimiter struct {
	limiters      map[string]*RateLimiter
	mu            *sync.RWMutex
	cleanUpTicker *time.Ticker
	maxInactive   time.Duration
}

func NewClientRateLimiter() *ClientRateLimiter {
	crl := &ClientRateLimiter{
		limiters:      map[string]*RateLimiter{},
		mu:            new(sync.RWMutex),
		cleanUpTicker: time.NewTicker(time.Second * 1),
		maxInactive:   time.Second * 3,
	}

	go crl.cleanUp()
	return crl
}

func (c *ClientRateLimiter) Allow(cID string) bool {
	c.mu.RLock()
	l, ok := c.limiters[cID]
	c.mu.RUnlock()
	if !ok {
		c.mu.Lock()
		l, ok = c.limiters[cID]
		if !ok {
			l = NewRateLimiter(MaxTokens, RefillRatePerSeconds)
			c.limiters[cID] = l

		}
		c.mu.Unlock()
	}

	return l.Allow()
}

func (c *ClientRateLimiter) cleanUp() {
	for range c.cleanUpTicker.C {
		fmt.Println("RUNNING CLEAN-UP")
		c.mu.Lock()
		for id, l := range c.limiters {
			ellapsed := time.Since(l.lastRefill)
			if ellapsed >= c.maxInactive {
				delete(c.limiters, id)
				fmt.Printf("removed cID %s due to inactivity\n", id)
			}
		}
		c.mu.Unlock()
	}
}
