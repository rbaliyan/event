package transport

import "testing"

func TestMatchesRouteFilters(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]string
		filters  map[string]string
		want     bool
	}{
		{"nil filters", map[string]string{"X-Route-region": "us-east"}, nil, true},
		{"empty filters", map[string]string{}, map[string]string{}, true},
		{"exact match", map[string]string{"X-Route-region": "us-east"}, map[string]string{"X-Route-region": "us-east"}, true},
		{"value mismatch", map[string]string{"X-Route-region": "eu-west"}, map[string]string{"X-Route-region": "us-east"}, false},
		{"key missing from metadata", map[string]string{}, map[string]string{"X-Route-region": "us-east"}, false},
		{"nil metadata with filters", nil, map[string]string{"X-Route-region": "us-east"}, false},
		{"nil metadata nil filters", nil, nil, true},
		{"multiple filters all match", map[string]string{"X-Route-region": "us-east", "X-Route-priority": "high"}, map[string]string{"X-Route-region": "us-east", "X-Route-priority": "high"}, true},
		{"multiple filters partial match", map[string]string{"X-Route-region": "us-east"}, map[string]string{"X-Route-region": "us-east", "X-Route-priority": "high"}, false},
		{"extra metadata ignored", map[string]string{"X-Route-region": "us-east", "Content-Type": "json"}, map[string]string{"X-Route-region": "us-east"}, true},
		{"empty string value matches", map[string]string{"X-Route-tag": ""}, map[string]string{"X-Route-tag": ""}, true},
		{"empty string vs missing", map[string]string{}, map[string]string{"X-Route-tag": ""}, true}, // Go map returns "" for missing key, which matches ""
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MatchesRouteFilters(tt.metadata, tt.filters)
			if got != tt.want {
				t.Errorf("MatchesRouteFilters() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHasRoutingKeys(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]string
		want     bool
	}{
		{"nil", nil, false},
		{"empty", map[string]string{}, false},
		{"no routing keys", map[string]string{"Content-Type": "json"}, false},
		{"has routing key", map[string]string{"X-Route-region": "us-east"}, true},
		{"prefix only key", map[string]string{"X-Route-": ""}, true},
		{"similar but wrong prefix", map[string]string{"X-Routeregion": "us-east"}, false},
		{"mixed", map[string]string{"foo": "bar", "X-Route-x": "y"}, true},
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

func TestWithRouteMatch_Composition(t *testing.T) {
	opts := DefaultSubscribeOptions()

	// First predicate: region must be us-east
	WithRouteMatch(func(meta map[string]string) bool {
		return meta["X-Route-region"] == "us-east"
	})(opts)

	// Second predicate: priority must be high (should AND with first)
	WithRouteMatch(func(meta map[string]string) bool {
		return meta["X-Route-priority"] == "high"
	})(opts)

	// Both match
	if !opts.RouteMatch(map[string]string{"X-Route-region": "us-east", "X-Route-priority": "high"}) {
		t.Error("expected match when both predicates satisfied")
	}

	// First fails
	if opts.RouteMatch(map[string]string{"X-Route-region": "eu-west", "X-Route-priority": "high"}) {
		t.Error("expected no match when first predicate fails")
	}

	// Second fails
	if opts.RouteMatch(map[string]string{"X-Route-region": "us-east", "X-Route-priority": "low"}) {
		t.Error("expected no match when second predicate fails")
	}
}

func TestWithRouteFilters(t *testing.T) {
	opts := DefaultSubscribeOptions()
	WithRouteFilters(map[string]string{"X-Route-region": "us-east"})(opts)

	if len(opts.RouteFilters) != 1 {
		t.Fatalf("expected 1 filter, got %d", len(opts.RouteFilters))
	}
	if opts.RouteFilters["X-Route-region"] != "us-east" {
		t.Errorf("expected us-east, got %s", opts.RouteFilters["X-Route-region"])
	}
}
