package distributed

import (
	"context"
	"sort"
	"time"
)

// ListWorkers returns a paginated list of worker entries matching the filter.
func (s *MemoryStateManager) ListWorkers(_ context.Context, filter WorkerFilter) (*WorkerPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Take a single now-reading from the clock so all stale-filter comparisons
	// in this listing use a consistent cutoff. Routing through s.clk also
	// keeps the stale filter testable via clock.Fake.
	now := s.clk.Now()

	// Collect matching entries
	var all []*WorkerEntry
	for id, entry := range s.states {
		we := memoryEntryToWorkerEntry(id, entry)
		if !matchesWorkerFilter(we, filter, now) {
			continue
		}
		all = append(all, we)
	}

	// Sort by updated_at, then message_id
	if filter.OrderDesc {
		sort.Slice(all, func(i, j int) bool {
			if !all[i].UpdatedAt.Equal(all[j].UpdatedAt) {
				return all[i].UpdatedAt.After(all[j].UpdatedAt)
			}
			return all[i].MessageID > all[j].MessageID
		})
	} else {
		sort.Slice(all, func(i, j int) bool {
			if !all[i].UpdatedAt.Equal(all[j].UpdatedAt) {
				return all[i].UpdatedAt.Before(all[j].UpdatedAt)
			}
			return all[i].MessageID < all[j].MessageID
		})
	}

	// Apply cursor-based pagination
	startIdx := 0
	if filter.Cursor != "" {
		cur, err := decodeWorkerCursor(filter.Cursor)
		if err != nil {
			return nil, err
		}
		found := false
		for i, e := range all {
			if filter.OrderDesc {
				if e.UpdatedAt.Before(cur.UpdatedAt) || (e.UpdatedAt.Equal(cur.UpdatedAt) && e.MessageID < cur.ID) {
					startIdx = i
					found = true
					break
				}
			} else {
				if e.UpdatedAt.After(cur.UpdatedAt) || (e.UpdatedAt.Equal(cur.UpdatedAt) && e.MessageID > cur.ID) {
					startIdx = i
					found = true
					break
				}
			}
		}
		if !found {
			return &WorkerPage{Entries: nil, HasMore: false}, nil
		}
	}

	limit := filter.EffectiveLimit()
	end := startIdx + limit + 1 // +1 to detect hasMore
	if end > len(all) {
		end = len(all)
	}
	page := all[startIdx:end]

	hasMore := len(page) > limit
	if hasMore {
		page = page[:limit]
	}

	var nextCursor string
	if hasMore && len(page) > 0 {
		last := page[len(page)-1]
		nextCursor = encodeWorkerCursor(workerCursor{
			UpdatedAt: last.UpdatedAt,
			ID:        last.MessageID,
		})
	}

	return &WorkerPage{
		Entries:    page,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

// CountWorkers returns the number of worker entries matching the filter.
func (s *MemoryStateManager) CountWorkers(_ context.Context, filter WorkerFilter) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := s.clk.Now()
	var count int64
	for id, entry := range s.states {
		we := memoryEntryToWorkerEntry(id, entry)
		if matchesWorkerFilter(we, filter, now) {
			count++
		}
	}
	return count, nil
}

// GetWorker returns a single worker entry by message ID.
func (s *MemoryStateManager) GetWorker(_ context.Context, messageID string) (*WorkerEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, exists := s.states[messageID]
	if !exists {
		return nil, nil
	}
	return memoryEntryToWorkerEntry(messageID, entry), nil
}

func memoryEntryToWorkerEntry(id string, entry *stateEntry) *WorkerEntry {
	status := WorkerStateProcessing
	if entry.state == stateCompleted {
		status = WorkerStateCompleted
	}

	var eventName string
	var metadata map[string]string
	if entry.payload != nil {
		eventName = entry.payload.EventName
		metadata = entry.payload.Metadata
	}

	return &WorkerEntry{
		MessageID: id,
		Status:    status,
		EventName: eventName,
		Metadata:  metadata,
		ExpiresAt: entry.expiresAt,
		CreatedAt: entry.updatedAt, // memory backend doesn't track createdAt separately
		UpdatedAt: entry.updatedAt,
	}
}

// matchesWorkerFilter returns true if entry passes all conditions in filter.
// The `now` parameter is the reference time used for the StaleTimeout check —
// callers pass s.clk.Now() so tests using clock.Fake can drive the stale
// boundary deterministically. Taking `now` once at the caller (instead of
// reading time.Now() inside the loop) also gives a consistent cutoff across
// all entries in a single ListWorkers call.
func matchesWorkerFilter(entry *WorkerEntry, filter WorkerFilter, now time.Time) bool {
	if len(filter.Status) > 0 {
		matched := false
		for _, s := range filter.Status {
			if entry.Status == s {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	if filter.EventName != "" && entry.EventName != filter.EventName {
		return false
	}
	if filter.WorkerID != "" && entry.WorkerID != filter.WorkerID {
		return false
	}
	if filter.StaleTimeout > 0 {
		cutoff := now.Add(-filter.StaleTimeout)
		if entry.Status != WorkerStateProcessing || !entry.UpdatedAt.Before(cutoff) {
			return false
		}
	}
	if !filter.CreatedAfter.IsZero() && entry.CreatedAt.Before(filter.CreatedAfter) {
		return false
	}
	if !filter.CreatedBefore.IsZero() && !entry.CreatedAt.Before(filter.CreatedBefore) {
		return false
	}
	return true
}

// Compile-time check that MemoryStateManager implements WorkerStore.
var _ WorkerStore = (*MemoryStateManager)(nil)
