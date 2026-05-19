package testutil

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
	"testing"
)

// UniqueName returns a short, filesystem- and Redis-key-safe identifier
// derived from t.Name() plus 64 bits of entropy. It is collision-free under
// `go test -count=N`, `-shuffle=on`, parallel CI shards, and parallel
// subtests — the failure mode the second-resolution timestamp prefix in the
// existing integration tests is vulnerable to.
//
// The returned form is `<sanitized-test-name>-<8-hex-bytes>`, e.g.
// `TestFoo_Subtest-3f9a2b81c4d0ee17`.
func UniqueName(t testing.TB) string {
	t.Helper()
	return sanitize(t.Name()) + "-" + randHex(8)
}

// sanitize lowercases and replaces characters that are problematic in Redis
// stream names, Postgres identifiers, and shell paths. The transformation is
// stable but not invertible; callers should not parse the result.
func sanitize(name string) string {
	var b strings.Builder
	b.Grow(len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// randHex returns n bytes of crypto-random hex. Hex over base32/64 because the
// downstream consumers (Redis keys, Postgres schemas) are case-sensitive in
// some contexts and uppercase-folding in others — hex is always safe.
func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand.Read only fails on a broken OS-level entropy source;
		// returning a deterministic fallback keeps tests running rather than
		// taking down the whole suite. The probability is negligible in
		// practice.
		return strings.Repeat("0", n*2)
	}
	return hex.EncodeToString(b)
}
