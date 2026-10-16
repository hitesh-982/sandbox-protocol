package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/samsung/agentic-sandbox/pkg/poolmanager"
	"github.com/samsung/agentic-sandbox/pkg/store"
)

func main() {
	addr := flag.String("addr", "0.0.0.0:8080", "HTTP listen address")
	nodeID := flag.String("node-id", "host-1", "Identifier for this node/host")
	warmCount := flag.Int("warm-count", 2, "Number of pre-provisioned warm sandbox pods")
	idleTTL := flag.Duration("idle-ttl", 10*time.Minute, "Duration before an idle sandbox is marked idle")
	deleteAfterStop := flag.Duration("delete-after-stop", 60*time.Minute, "Duration before an idle sandbox is reaped")
	flag.Parse()

	log.Printf("[pool-manager] Starting on %s (node: %s, warm: %d)...", *addr, *nodeID, *warmCount)

	st := store.NewMemoryStore()
	prov, err := poolmanager.NewLocalProvisioner(*nodeID, *warmCount)
	if err != nil {
		log.Fatalf("failed to initialize provisioner: %v", err)
	}

	cfg := poolmanager.Config{
		ListenAddr:      *addr,
		IdleTTL:         *idleTTL,
		DeleteAfterStop: *deleteAfterStop,
		ReaperInterval:  10 * time.Second,
	}

	pm := poolmanager.New(cfg, st, prov)
	if err := pm.Start(); err != nil {
		log.Fatalf("failed to start pool manager: %v", err)
	}
	log.Printf("[pool-manager] Listening on %s", pm.Addr())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("[pool-manager] Shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = pm.Stop(ctx)
	log.Println("[pool-manager] Stopped.")
}
