package cache // import "rs3.io/go/cache"

import (
	"errors"
	"fmt"
	"sync"
)

var ErrTypeMismatch = errors.New("cache: type mismatch")

func typeMismatchError(key string, value, zero any) error {
	const mismatch = "%w for key %q: got %T, want %T"
	return fmt.Errorf(mismatch, ErrTypeMismatch, key, value, zero)
}

// Cache represents a key-value cache backend.
type Cache interface {
	get(key string) (any, bool)
	set(key string, value any)
	delete(key string)
	getOrRegisterFlight(key string) (any, *flight, bool)
	resolveFlight(fl *flight, key string, value any, err error)
}

type flight struct {
	wg    sync.WaitGroup
	value any
	err   error
}

type SimpleCache struct {
	entries map[string]any
}

// New creates a simple, in-memory key-value cache.
//
// Entries remain in the cache indefinitely. If memory management or resource
// eviction is required, entries must be removed manually using [Delete].
//
// The returned cache is not safe for concurrent use, but should be faster
// than [SyncCache] in cases where synchronization is unnecessary.
func New() *SimpleCache {
	return &SimpleCache{
		entries: make(map[string]any),
	}
}

// Get gets the cache entry at key and casts it to type T.
//
// If the key is not found, it returns the zero value and false.
//
// If the key is found but the value cannot be cast to type T, it returns the
// zero value of type T, false, and an [ErrTypeMismatch].
func Get[T any](c Cache, key string) (T, bool, error) {
	var zero T
	if x, ok := c.get(key); ok {
		value, err := cacheHit[T](key, x)
		return value, err == nil, err
	}
	return zero, false, nil
}

// GetOrCreate gets the cache entry at key and casts it to type T.
//
// If the key is not found, it populates the entry using the provided function
// and returns the resulting value if the function succeeds.
//
// If that function returns an error, it returns the zero value of type T and
// that error.
//
// If the key is found but the value cannot be cast to type T, it returns the
// zero value of type T and an [ErrTypeMismatch].
func GetOrCreate[T any](c Cache, key string, f func() (T, error)) (T, error) {
	var zero T

	raw, fl, waiting := c.getOrRegisterFlight(key)
	if waiting && fl != nil {
		fl.wg.Wait()
		if fl.err != nil {
			return zero, fl.err
		}
		return cacheHit[T](key, fl.value)
	}
	if raw != nil {
		return cacheHit[T](key, raw)
	}
	value, err := f()
	c.resolveFlight(fl, key, value, err)
	if err != nil {
		return zero, err
	}
	return value, nil
}

func cacheHit[T any](key string, raw any) (T, error) {
	var zero T
	if value, ok := raw.(T); ok {
		return value, nil
	}
	return zero, typeMismatchError(key, raw, zero)
}

// Set sets the cache entry at key to value.
func Set[T any](c Cache, key string, value T) {
	c.set(key, value)
}

// Delete evicts the cache entry at key.
func Delete(c Cache, key string) {
	c.delete(key)
}

func (c *SimpleCache) get(key string) (any, bool) {
	value, ok := c.entries[key]
	return value, ok
}

func (c *SimpleCache) set(key string, value any) {
	c.entries[key] = value
}

func (c *SimpleCache) delete(key string) {
	delete(c.entries, key)
}

func (c *SimpleCache) getOrRegisterFlight(key string) (any, *flight, bool) {
	if value, ok := c.entries[key]; ok {
		return value, nil, false
	}
	return nil, nil, false
}

func (c *SimpleCache) resolveFlight(
	_ *flight, key string, value any, err error,
) {
	if err == nil {
		c.entries[key] = value
	}
}

type SyncCache struct {
	mu sync.RWMutex
	SimpleCache
}

// NewSync creates a simple, in-memory, concurrent-safe key-value cache.
//
// Entries remain in the cache indefinitely. If memory management or resource
// eviction is required, entries must be removed manually using [Delete].
func NewSync() *SyncCache {
	return &SyncCache{entries: make(map[string]any)}
}

func (c *SyncCache) get(key string) (any, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if x, ok := c.entries[key]; ok {
		if _, isFlight := x.(*flight); isFlight {
			return nil, false
		}
		return x, true
	}
	return nil, false
}

func (c *SyncCache) set(key string, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.SimpleCache.set(key, value)
}

func (c *SyncCache) delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.SimpleCache.delete(key)
}

func (c *SyncCache) getOrRegisterFlight(key string) (any, *flight, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	raw, ok := c.entries[key]
	if !ok {
		fl := &flight{}
		fl.wg.Add(1)
		c.entries[key] = fl
		return nil, fl, false
	}
	if fl, isFlight := raw.(*flight); isFlight {
		return nil, fl, true
	}
	return raw, nil, false
}

func (c *SyncCache) resolveFlight(
	fl *flight, key string, value any, err error,
) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if current, ok := c.entries[key]; ok && current == fl {
		if err != nil {
			delete(c.entries, key)
		} else {
			c.entries[key] = value
		}
	}
	if err != nil {
		fl.err = err
	} else {
		fl.value = value
	}
	fl.wg.Done()
}
