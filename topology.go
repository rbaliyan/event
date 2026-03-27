package event

import (
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/rbaliyan/event/v3/transport"
)

// SubscriptionInfo describes a live subscription on an event.
type SubscriptionInfo struct {
	SubscriptionID        string            `json:"subscription_id"`
	DeliveryMode          DeliveryMode      `json:"delivery_mode"`
	WorkerGroup           string            `json:"worker_group,omitempty"`
	SubscriberName        string            `json:"subscriber_name,omitempty"`
	SubscriberDescription string            `json:"subscriber_description,omitempty"`
	RouteFilters          map[string]string `json:"route_filters,omitempty"`
	HasRouteMatch         bool              `json:"has_route_match,omitempty"`
	StartedAt             time.Time         `json:"started_at"`
}

// EventInfo describes a registered event and its active subscriptions.
type EventInfo struct {
	Name            string             `json:"name"`
	SubscriberCount int                `json:"subscriber_count"`
	Subscriptions   []SubscriptionInfo `json:"subscriptions"`
}

// BusInfo describes a registered bus, its transport, and its events.
type BusInfo struct {
	ID            string      `json:"id"`
	Name          string      `json:"name"`
	Running       bool        `json:"running"`
	TransportName string      `json:"transport_name"`
	Events        []EventInfo `json:"events"`
	HasMonitor    bool        `json:"has_monitor"`
	HasDLQ        bool        `json:"has_dlq"`
	HasOutbox     bool        `json:"has_outbox"`
}

// eventTopology is implemented by eventImpl[T] for generic-free topology access.
type eventTopology interface {
	eventName() string
	subscriberCount() int64
	subscriptionInfos() []SubscriptionInfo
}

// Topology returns a snapshot of all registered buses, their events, and subscriptions.
func Topology() []BusInfo {
	var infos []BusInfo
	busRegistry.Range(func(key, value any) bool {
		bus := value.(*Bus)
		infos = append(infos, bus.Topology())
		return true
	})
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].Name < infos[j].Name
	})
	return infos
}

// Topology returns a snapshot of this bus, its events, and their subscriptions.
func (b *Bus) Topology() BusInfo {
	info := BusInfo{
		ID:            b.id,
		Name:          b.name,
		Running:       b.Running(),
		TransportName: transportName(b.transport),
		HasMonitor:    b.monitorStore != nil,
		HasDLQ:        b.dlqStore != nil,
		HasOutbox:     b.outboxStore != nil,
	}

	b.eventMutex.RLock()
	for _, ev := range b.events {
		if topo, ok := ev.(eventTopology); ok {
			subs := topo.subscriptionInfos()
			info.Events = append(info.Events, EventInfo{
				Name:            topo.eventName(),
				SubscriberCount: int(topo.subscriberCount()),
				Subscriptions:   subs,
			})
		}
	}
	b.eventMutex.RUnlock()

	sort.Slice(info.Events, func(i, j int) bool {
		return info.Events[i].Name < info.Events[j].Name
	})

	return info
}

// transportName returns a human-readable name for the transport.
func transportName(t transport.Transport) string {
	if t == nil {
		return ""
	}
	if named, ok := t.(transport.Named); ok {
		return named.Name()
	}
	// Fallback to reflected type name
	rt := reflect.TypeOf(t)
	if rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	return rt.Name()
}

// subscriptions tracks active subscription metadata on an eventImpl.
// Access must be guarded by the embedded sync.Map.
type subscriptionRegistry struct {
	m sync.Map // map[subscriptionID]*SubscriptionInfo
}

func (r *subscriptionRegistry) add(info *SubscriptionInfo) {
	r.m.Store(info.SubscriptionID, info)
}

func (r *subscriptionRegistry) remove(subscriptionID string) {
	r.m.Delete(subscriptionID)
}

func (r *subscriptionRegistry) all() []SubscriptionInfo {
	var infos []SubscriptionInfo
	r.m.Range(func(key, value any) bool {
		info := value.(*SubscriptionInfo)
		infos = append(infos, *info)
		return true
	})
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].StartedAt.Before(infos[j].StartedAt)
	})
	return infos
}
