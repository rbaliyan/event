package event

import (
	"context"
	"testing"

	"github.com/rbaliyan/event/v3/transport"
)

func TestRoutingKey(t *testing.T) {
	got := RoutingKey("region")
	want := "X-Route-region"
	if got != want {
		t.Errorf("RoutingKey(\"region\") = %q, want %q", got, want)
	}
}

func TestHasRoutingKeys(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]string
		want     bool
	}{
		{"nil metadata", nil, false},
		{"empty metadata", map[string]string{}, false},
		{"no routing keys", map[string]string{"Content-Type": "application/json"}, false},
		{"has routing key", map[string]string{"X-Route-region": "us-east"}, true},
		{"mixed keys", map[string]string{"Content-Type": "json", "X-Route-priority": "high"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := HasRoutingKeys(tt.metadata)
			if got != tt.want {
				t.Errorf("HasRoutingKeys() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMatchesRouteFilters(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]string
		filters  map[string]string
		want     bool
	}{
		{"nil filters", map[string]string{"X-Route-region": "us-east"}, nil, true},
		{"empty filters", map[string]string{"X-Route-region": "us-east"}, map[string]string{}, true},
		{"exact match", map[string]string{"X-Route-region": "us-east"}, map[string]string{"X-Route-region": "us-east"}, true},
		{"value mismatch", map[string]string{"X-Route-region": "eu-west"}, map[string]string{"X-Route-region": "us-east"}, false},
		{"key missing", map[string]string{}, map[string]string{"X-Route-region": "us-east"}, false},
		{"nil metadata", nil, map[string]string{"X-Route-region": "us-east"}, false},
		{"multiple filters all match", map[string]string{"X-Route-region": "us-east", "X-Route-priority": "high"}, map[string]string{"X-Route-region": "us-east", "X-Route-priority": "high"}, true},
		{"multiple filters partial match", map[string]string{"X-Route-region": "us-east"}, map[string]string{"X-Route-region": "us-east", "X-Route-priority": "high"}, false},
		{"extra metadata keys ok", map[string]string{"X-Route-region": "us-east", "Content-Type": "json"}, map[string]string{"X-Route-region": "us-east"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := transport.MatchesRouteFilters(tt.metadata, tt.filters)
			if got != tt.want {
				t.Errorf("MatchesRouteFilters() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestContextWithRoutingKey(t *testing.T) {
	ctx := context.Background()

	// Add a routing key
	ctx = ContextWithRoutingKey(ctx, "region", "us-east")
	meta := ContextMetadata(ctx)

	if meta == nil {
		t.Fatal("expected metadata, got nil")
	}
	if v := meta["X-Route-region"]; v != "us-east" {
		t.Errorf("expected X-Route-region=us-east, got %q", v)
	}

	// Add another routing key — should accumulate
	ctx = ContextWithRoutingKey(ctx, "priority", "high")
	meta = ContextMetadata(ctx)

	if v := meta["X-Route-region"]; v != "us-east" {
		t.Errorf("expected X-Route-region=us-east after second key, got %q", v)
	}
	if v := meta["X-Route-priority"]; v != "high" {
		t.Errorf("expected X-Route-priority=high, got %q", v)
	}
}

func TestContextWithRoutingKey_OverwriteExisting(t *testing.T) {
	ctx := context.Background()
	ctx = ContextWithRoutingKey(ctx, "region", "us-east")
	ctx = ContextWithRoutingKey(ctx, "region", "eu-west")

	meta := ContextMetadata(ctx)
	if v := meta["X-Route-region"]; v != "eu-west" {
		t.Errorf("expected overwritten value eu-west, got %q", v)
	}
}

func TestMatchesRouteFilters_Reexport(t *testing.T) {
	// Verify the re-exported function works identically
	meta := map[string]string{"X-Route-region": "us-east"}
	if !MatchesRouteFilters(meta, map[string]string{"X-Route-region": "us-east"}) {
		t.Error("expected match")
	}
	if MatchesRouteFilters(meta, map[string]string{"X-Route-region": "eu-west"}) {
		t.Error("expected no match")
	}
}

func TestWithRouteMatch_Composition(t *testing.T) {
	// Two predicates composed with AND semantics
	var opts []SubscribeOption[string]
	opts = append(opts,
		WithRouteMatch[string](func(meta map[string]string) bool {
			return meta["X-Route-region"] == "us-east"
		}),
		WithRouteMatch[string](func(meta map[string]string) bool {
			return meta["X-Route-priority"] == "high"
		}),
	)

	subOpts := newSubscribeOptions(opts...)

	// Both match
	if !subOpts.routeMatch(map[string]string{"X-Route-region": "us-east", "X-Route-priority": "high"}) {
		t.Error("expected match when both predicates satisfied")
	}
	// First fails
	if subOpts.routeMatch(map[string]string{"X-Route-region": "eu-west", "X-Route-priority": "high"}) {
		t.Error("expected no match when first predicate fails")
	}
	// Second fails
	if subOpts.routeMatch(map[string]string{"X-Route-region": "us-east", "X-Route-priority": "low"}) {
		t.Error("expected no match when second predicate fails")
	}
}
