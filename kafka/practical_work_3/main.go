package main

import (
	"context"
	"log"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.LUTC)

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 4)
	var wg sync.WaitGroup

	runComponent := func(name string, fn func(context.Context, Config) error) {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if err := fn(ctx, cfg); err != nil {
				errCh <- err
				log.Printf("%s: stopped with error: %v", name, err)
				stop()
			}
		}()
	}

	runComponent("block-list processor", RunBlockListProcessor)
	runComponent("banned-words processor", RunBannedWordsProcessor)
	runComponent("censorship processor", RunCensorshipProcessor)
	runComponent("block-filter processor", RunBlockFilterProcessor)

	go func() {
		wg.Wait()
		close(errCh)
	}()

	for err := range errCh {
		if err != nil {
			log.Printf("application: received component error: %v", err)
		}
	}

	log.Printf("application: shutdown completed")
}
