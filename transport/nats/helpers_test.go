package nats

import (
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// startTestServer spins up an in-process NATS server with JetStream enabled
// against in-memory storage. It returns a connected *nats.Conn and a t.Cleanup
// hook that shuts both down deterministically.
//
// Embedded over testcontainers because nats-server boots in well under 50 ms
// in-process and needs no Docker. JetStream uses MemoryStore so tests are
// hermetic: nothing touches disk, parallel test runs cannot collide on a
// shared store directory.
func startTestServer(t testing.TB) *nats.Conn {
	t.Helper()

	opts := &natsserver.Options{
		Host:                  "127.0.0.1",
		Port:                  -1, // auto-pick a free port
		NoLog:                 true,
		NoSigs:                true,
		JetStream:             true,
		JetStreamMaxStore:     -1, // unlimited (in-memory)
		JetStreamMaxMemory:    -1,
		StoreDir:              t.TempDir(), // isolated per-test
		DisableShortFirstPing: true,
	}

	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("nats-server: %v", err)
	}
	srv.Start()

	if !srv.ReadyForConnections(5 * time.Second) {
		srv.Shutdown()
		t.Fatalf("nats-server did not become ready within 5s")
	}

	conn, err := nats.Connect(srv.ClientURL(),
		nats.Name("nats-test-"+t.Name()),
		// Make reconnect attempts visible if they ever happen in a test —
		// they shouldn't, since the server is in-process.
		nats.MaxReconnects(-1),
		nats.ReconnectWait(50*time.Millisecond),
	)
	if err != nil {
		srv.Shutdown()
		t.Fatalf("nats.Connect: %v", err)
	}

	t.Cleanup(func() {
		conn.Close()
		srv.Shutdown()
		srv.WaitForShutdown()
	})

	return conn
}

// eventually polls cond until it returns true or timeout elapses. Mirrors
// testutil.Eventually but lives in-package because importing
// internal/testutil from a transport sub-package crosses the
// public-API boundary downstream consumers may not want to depend on.
func eventually(t testing.TB, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	const poll = 5 * time.Millisecond
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("Eventually: %s (within %s)", msg, timeout)
		}
		time.Sleep(poll)
	}
}
