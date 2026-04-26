package main

import "time"

type stoppableTicker struct {
	C <-chan time.Time
	t *time.Ticker
}

func newStoppableTicker(interval time.Duration) stoppableTicker {
	if interval <= 0 {
		interval = time.Second
	}

	ticker := time.NewTicker(interval)

	return stoppableTicker{
		C: ticker.C,
		t: ticker,
	}
}

func (t stoppableTicker) Stop() {
	t.t.Stop()
}
