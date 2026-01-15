package base

import (
	"github.com/rbaliyan/event/v3/transport"
)

// GroupNamer generates consumer group names based on delivery mode and worker groups.
// This provides consistent group naming across all broker-based transports.
type GroupNamer struct {
	baseGroupID string
	separator   string
}

// NewGroupNamer creates a new group namer with the given base group ID.
// The separator is used between components (default "-").
func NewGroupNamer(baseGroupID string) *GroupNamer {
	return &GroupNamer{
		baseGroupID: baseGroupID,
		separator:   "-",
	}
}

// NewGroupNamerWithSeparator creates a new group namer with a custom separator.
func NewGroupNamerWithSeparator(baseGroupID, separator string) *GroupNamer {
	return &GroupNamer{
		baseGroupID: baseGroupID,
		separator:   separator,
	}
}

// GroupID returns the appropriate consumer group ID based on delivery mode and options.
//
// For WorkerPool mode:
//   - With named worker group: baseGroupID-eventName-workerGroupName
//   - Without worker group: baseGroupID-eventName (or just baseGroupID depending on includeEventName)
//
// For Broadcast mode:
//   - Unique group per subscriber: baseGroupID-eventName-subscriptionID
func (g *GroupNamer) GroupID(eventName string, opts *transport.SubscribeOptions, subscriptionID string) string {
	return g.GroupIDWithEventName(eventName, opts, subscriptionID, true)
}

// GroupIDWithEventName returns the group ID with optional event name inclusion.
// Some transports (like Kafka) may want different patterns.
func (g *GroupNamer) GroupIDWithEventName(eventName string, opts *transport.SubscribeOptions, subscriptionID string, includeEventName bool) string {
	if opts.DeliveryMode == transport.WorkerPool {
		if opts.WorkerGroup != "" {
			// WorkerPool with named group: workers in same group compete
			// Different groups each receive all messages
			return g.baseGroupID + g.separator + eventName + g.separator + opts.WorkerGroup
		}
		// WorkerPool default: all workers share the group
		if includeEventName {
			return g.baseGroupID + g.separator + eventName
		}
		return g.baseGroupID
	}

	// Broadcast: unique group per subscriber (fan-out)
	return g.baseGroupID + g.separator + eventName + g.separator + subscriptionID
}

// WorkerGroupID returns the group ID for WorkerPool mode specifically.
func (g *GroupNamer) WorkerGroupID(eventName, workerGroupName string) string {
	if workerGroupName != "" {
		return g.baseGroupID + g.separator + eventName + g.separator + workerGroupName
	}
	return g.baseGroupID + g.separator + eventName
}

// BroadcastGroupID returns the group ID for Broadcast mode specifically.
func (g *GroupNamer) BroadcastGroupID(eventName, subscriptionID string) string {
	return g.baseGroupID + g.separator + eventName + g.separator + subscriptionID
}

// BaseGroupID returns the base group ID.
func (g *GroupNamer) BaseGroupID() string {
	return g.baseGroupID
}

// ResourceNamer generates transport-specific resource names (streams, topics, subjects).
type ResourceNamer struct {
	prefix    string
	separator string
}

// NewResourceNamer creates a new resource namer.
// prefix: the prefix for all resources (e.g., "evt")
// separator: character between prefix and name (e.g., ":", ".", "_")
func NewResourceNamer(prefix, separator string) *ResourceNamer {
	return &ResourceNamer{
		prefix:    prefix,
		separator: separator,
	}
}

// Name returns the full resource name for an event.
func (r *ResourceNamer) Name(eventName string) string {
	if r.prefix == "" {
		return eventName
	}
	return r.prefix + r.separator + eventName
}

// Prefix returns the resource prefix.
func (r *ResourceNamer) Prefix() string {
	return r.prefix
}
