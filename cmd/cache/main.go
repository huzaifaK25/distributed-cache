// Command cache runs the Phase 3 TCP cache server.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributed-cache/internal/cache"
	"distributed-cache/internal/server"
)

// TODO: make func main strictly typedmeaning initialize forst with var and type, then assign values.
// TODO: add comments for each portion in main() for what it does. For example, explain what the flags are for, what the cache is for, and what the server is for.

func main() {
	config := server.DefaultConfig()
	var (
		maxItems        int
		cleanupInterval time.Duration
	)

	flag.StringVar(&config.Address, "addr", config.Address, "TCP listen address")
	flag.IntVar(&config.MaxConnections, "max-conns", config.MaxConnections, "maximum simultaneous clients")
	flag.IntVar(&config.WorkerCount, "workers", config.WorkerCount, "request worker goroutines")
	flag.IntVar(&config.QueueSize, "queue-size", config.QueueSize, "maximum queued requests")
	flag.DurationVar(&config.IdleTimeout, "idle-timeout", config.IdleTimeout, "close idle client connections after this duration")
	flag.DurationVar(&config.WriteTimeout, "write-timeout", config.WriteTimeout, "maximum time to write one response")
	flag.DurationVar(&config.ShutdownTimeout, "shutdown-timeout", config.ShutdownTimeout, "maximum graceful shutdown wait")
	flag.IntVar(&config.MaxRequestSize, "max-request-size", config.MaxRequestSize, "maximum command-line bytes")
	flag.IntVar(&maxItems, "max-items", 0, "maximum cache entries (<=0 means unlimited)")
	flag.DurationVar(&cleanupInterval, "cleanup-interval", 30*time.Second, "expired-key sweep interval (<=0 disables the worker)")
	flag.Parse()

	c := cache.New(cache.Config{
		MaxItems:        maxItems,
		CleanupInterval: cleanupInterval,
	})
	defer c.StopCleanupWorker()

	srv := server.New(c, config)
	if err := srv.Start(); err != nil {
		log.Fatalf("start: %v", err)
	}
	log.Printf("cache listening on %s (workers=%d, max-connections=%d)",
		srv.Addr(), config.WorkerCount, config.MaxConnections)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	<-signals

	log.Print("shutting down")
	if err := srv.Stop(); err != nil {
		log.Printf("shutdown: %v", err)
	}
	log.Print("stopped")
}
