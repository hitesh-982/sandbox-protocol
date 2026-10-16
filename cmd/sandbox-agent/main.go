package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/samsung/agentic-sandbox/pkg/agent"
)

func main() {
	addr := flag.String("addr", "0.0.0.0:9999", "In-pod agent listen address")
	workDir := flag.String("dir", "/workspace", "Default working directory")
	flag.Parse()

	log.Printf("[sandbox-agent] Starting on %s (default dir: %s)...", *addr, *workDir)

	ag := agent.NewAgent(agent.Config{
		ListenAddr: *addr,
		DefaultDir: *workDir,
	})

	if err := ag.Start(); err != nil {
		log.Fatalf("failed to start agent: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("[sandbox-agent] Shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = ag.Stop(ctx)
	log.Println("[sandbox-agent] Stopped.")
}
