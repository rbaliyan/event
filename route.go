package event

import (
	"github.com/rbaliyan/event/v3/transport"
)

// RoutingKeyPrefix is the metadata key prefix for routing keys.
// Publishers set routing keys via metadata entries like "X-Route-region" = "us-east".
// Subscribers declare route filters to receive only matching messages.
const RoutingKeyPrefix = transport.RoutingKeyPrefix

// RoutingKey returns the full metadata key for a routing key name.
// For example, RoutingKey("region") returns "X-Route-region".
func RoutingKey(key string) string {
	return RoutingKeyPrefix + key
}

// HasRoutingKeys returns true if the metadata contains any routing keys
// (keys prefixed with X-Route-).
func HasRoutingKeys(metadata map[string]string) bool {
	return transport.HasRoutingKeys(metadata)
}

// MatchesRouteFilters checks whether message metadata satisfies all route filters.
// This is re-exported from the transport package for use in custom WithRouteMatch predicates.
func MatchesRouteFilters(metadata, filters map[string]string) bool {
	return transport.MatchesRouteFilters(metadata, filters)
}

// matchesSubscriptionRoute checks whether a message satisfies the subscription's
// route filters and route match predicate. Used as a transport-agnostic fallback
// in the event layer's subscribe loops — ensures routing works across all transports,
// not only the channel transport which filters at dispatch time.
func matchesSubscriptionRoute[T any](metadata map[string]string, subOpts *subscribeOptions[T]) bool {
	if len(subOpts.routeFilters) == 0 && subOpts.routeMatch == nil {
		return true
	}
	if !transport.MatchesRouteFilters(metadata, subOpts.routeFilters) {
		return false
	}
	if subOpts.routeMatch != nil && !subOpts.routeMatch(metadata) {
		return false
	}
	return true
}
