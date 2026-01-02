package payload

import "sync"

var (
	mu       sync.RWMutex
	registry = map[string]Codec{
		"application/json": JSON{},
	}
)

// Register adds a codec to the global registry.
// Codecs are looked up by their ContentType() during message decoding.
func Register(codec Codec) {
	mu.Lock()
	defer mu.Unlock()
	registry[codec.ContentType()] = codec
}

// Get retrieves a codec by content type from the global registry.
// Returns the codec and true if found, or nil and false if not found.
func Get(contentType string) (Codec, bool) {
	mu.RLock()
	defer mu.RUnlock()
	c, ok := registry[contentType]
	return c, ok
}

// MustGet retrieves a codec by content type, returning the default JSON codec
// if the requested content type is not found.
func MustGet(contentType string) Codec {
	if c, ok := Get(contentType); ok {
		return c
	}
	return JSON{}
}
