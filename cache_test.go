package cache

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type TestResource struct {
	Value string
}

type cacheConstructor struct {
	name string
	f    func() Cache
}

func getTestBackends() []cacheConstructor {
	return []cacheConstructor{
		{name: "SimpleCache", f: func() Cache { return New() }},
		{name: "SyncCache", f: func() Cache { return NewSync() }},
	}
}

func TestGet_TriState(t *testing.T) {
	for _, tc := range getTestBackends() {
		t.Run(tc.name, func(t *testing.T) {
			c := tc.f()
			key := "test-key"
			value := "hello"

			{
				// cache miss: key does not exist
				if _, ok, err := Get[string](c, key); ok || err != nil {
					t.Errorf(
						"expected miss: "+
							"got (ok=%v, err=%v), "+
							"want (ok=%v, err=%v)",
						ok, err, false, nil,
					)
				}
			}

			{
				// cache hit: key exists with correct type
				c.set(key, value)
				if v, ok, err := Get[string](c, key); !ok || err != nil {
					t.Fatalf(
						"expected hit: "+
							"got (ok=%v, err=%v), "+
							"want (ok=%v, err=%v)",
						ok, err, true, nil,
					)
				} else if v != value {
					t.Errorf("unexpected value: got %q, want %q", v, value)
				}
			}

			{
				// type mismatch: key exists but with incorrect type
				if _, ok, err := Get[int](c, key); ok || err == nil {
					t.Errorf(
						"expected mismatch: "+
							"got (ok=%v, err=%v), "+
							"want (ok=%v, err!=%v)",
						ok, err, false, nil,
					)
				}
			}
		})
	}
}

func TestGetOrCreate_CacheMiss(t *testing.T) {
	for _, tc := range getTestBackends() {
		t.Run(tc.name, func(t *testing.T) {
			c := tc.f()
			key := "miss-key"
			value := "hello"
			calls := 0

			f := func() (TestResource, error) {
				calls++
				return TestResource{Value: value}, nil
			}

			{
				// first call should run the initializer
				v, err := GetOrCreate(c, key, f)
				if err != nil {
					t.Fatalf("unexpected error on first call: %v", err)
				}
				if v.Value != value {
					t.Errorf("unexpected value: got %q, want %q", v.Value, value)
				}
				if calls != 1 {
					t.Errorf("expected initializer to be called exactly 1 time, got %d", calls)
				}
			}

			{
				// second call should hit the cache
				v, err := GetOrCreate(c, key, f)
				if err != nil {
					t.Fatalf("unexpected error on second call: %v", err)
				}
				if v.Value != value {
					t.Errorf("expected cached value: got %q, want %q", v.Value, value)
				}
				if calls != 1 {
					t.Errorf("expected initializer call count to stay at 1, got %d", calls)
				}
			}
		})
	}
}

func TestGetOrCreate_InitError(t *testing.T) {
	for _, tc := range getTestBackends() {
		t.Run(tc.name, func(t *testing.T) {
			c := tc.f()
			key := "error-key"
			expectedErr := errors.New("initialization failed")
			f := func() (TestResource, error) {
				return TestResource{}, expectedErr
			}

			_, err := GetOrCreate(c, key, f)
			if !errors.Is(err, expectedErr) {
				t.Fatalf("unexpected error: got %v, want %v", err, expectedErr)
			}
			if _, ok := c.get(key); ok {
				t.Error("expected key not to be cached after an initialization error")
			}
		})
	}
}

func TestDelete(t *testing.T) {
	for _, tc := range getTestBackends() {
		t.Run(tc.name, func(t *testing.T) {
			c := tc.f()
			key := "delete-key"
			calls := 0
			f := func() (string, error) {
				calls++
				return "data", nil
			}

			_, _ = GetOrCreate(c, key, f)
			Delete(c, key)
			_, _ = GetOrCreate(c, key, f)
			if calls != 2 {
				t.Errorf("expected initializer to be called 2 times after deletion, got %d", calls)
			}
		})
	}
}

func TestGetOrCreate_ThunderingHerd(t *testing.T) {
	c := NewSync()
	key := "herd-key"
	value := "shared-payload"

	var initCalls int64
	start := make(chan struct{})
	var wg sync.WaitGroup

	slowInit := func() (string, error) {
		atomic.AddInt64(&initCalls, 1)
		time.Sleep(20 * time.Millisecond)
		return value, nil
	}

	const N = 100

	for range N {
		wg.Go(func() {
			<-start
			v, err := GetOrCreate(c, key, slowInit)
			if err != nil {
				t.Errorf("unexpected flight error: %v", err)
				return
			}
			if v != value {
				t.Errorf("unexpected value: got %q, want %q", v, value)
			}
		})
	}
	close(start)
	wg.Wait()

	if n := atomic.LoadInt64(&initCalls); n != 1 {
		t.Errorf("CRITICAL: too many calls: got %d, want %d", n, 1)
	}
}

func TestGetOrCreate_ConcurrentDeleteRace(t *testing.T) {
	c := NewSync()
	key := "race-key"
	value := "race-payload"

	for epoch := range 10 {
		var wg sync.WaitGroup
		start := make(chan struct{})

		slowInit := func() (string, error) {
			time.Sleep(5 * time.Millisecond)
			return value, nil
		}

		wg.Go(func() {
			<-start
			_, _ = GetOrCreate(c, key, slowInit)
		})

		wg.Go(func() {
			<-start
			v, err := GetOrCreate(c, key, slowInit)
			if err == nil && v != value {
				t.Errorf("epoch %d: stale or corrupted value read during delete race", epoch)
			}
		})

		wg.Go(func() {
			<-start
			for range 5 {
				Delete(c, key)
				time.Sleep(1 * time.Millisecond)
			}
		})

		close(start)
		wg.Wait()
		Delete(c, key)
	}
}

func BenchmarkCacheHit(b *testing.B) {
	c := New()
	key := "bench-key"
	value := "some cached payload"
	f := func() (string, error) {
		return value, nil
	}

	Set(c, key, value)
	for b.Loop() {
		_, _ = GetOrCreate(c, key, f)
	}
}

func BenchmarkCacheHit_Sync(b *testing.B) {
	c := NewSync()
	key := "bench-key"
	value := "some cached payload"
	f := func() (string, error) {
		return value, nil
	}

	Set(c, key, value)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = GetOrCreate(c, key, f)
		}
	})
}

func ExampleGet() {
	c := New()
	key := "app_version"
	value := "v1.2.3"

	// Seed the cache manually for the example
	Set(c, key, value)

	// Look up the version string
	if version, found, err := Get[string](c, key); err != nil {
		fmt.Println("Error:", err)
	} else if found {
		fmt.Println("Found version:", version)
	}

	// Output:
	// Found version: v1.2.3
}

func ExampleGetOrCreate() {
	c := New()
	key := "user123"
	value := "user123@example.com"

	// Define an initializer function for a mock database fetch
	fetchUserEmail := func() (string, error) {
		return value, nil
	}

	// First pass: Cache miss, initializer runs
	email, _ := GetOrCreate(c, key, fetchUserEmail)
	fmt.Println("First fetch:", email)

	// Second pass: Cache hit, reads straight from memory
	cachedEmail, _ := GetOrCreate(c, key, fetchUserEmail)
	fmt.Println("Second fetch:", cachedEmail)

	// Output:
	// First fetch: user123@example.com
	// Second fetch: user123@example.com
}

func Example_heterogeneousCache() {
	c := NewSync()

	type User struct {
		Name string
		Age  int
	}

	type Config struct {
		MaxConnections int
		DebugMode      bool
	}

	// 1. Store values of multiple distinct types
	Set(c, "app:version", "v2.0.4")
	Set(c, "system:config", Config{MaxConnections: 100, DebugMode: true})
	Set(c, "active:users", []User{
		{Name: "Alice", Age: 30},
		{Name: "Bob", Age: 25},
	})

	// 2. Safely query typed values for each entry
	if version, ok, _ := Get[string](c, "app:version"); ok {
		fmt.Println("Version:", version)
	}
	if config, ok, _ := Get[Config](c, "system:config"); ok {
		fmt.Printf("Debug Mode: %t\n", config.DebugMode)
	}
	if users, ok, _ := Get[[]User](c, "active:users"); ok {
		fmt.Printf("Loaded %d users. First user: %s\n", len(users), users[0].Name)
	}

	// 3. Safely query value of incorrect type
	_, _, err := Get[int](c, "app:version")
	if err != nil && errors.Is(err, ErrTypeMismatch) {
		fmt.Println("Type verification caught error successfully.")
	}

	// Output:
	// Version: v2.0.4
	// Debug Mode: true
	// Loaded 2 users. First user: Alice
	// Type verification caught error successfully.
}

func Example_distinctKeys() {
	const M = 10
	const N = 5
	const payload = "payload"
	var resourcesCreated, totalCalls int64

	c := NewSync()
	wg := &sync.WaitGroup{}

	fetchResource := func() (string, error) {
		// The initializer runs exactly once under concurrent load
		time.Sleep(500 * time.Millisecond)
		atomic.AddInt64(&resourcesCreated, 1)
		return payload, nil
	}

	start := time.Now()
	// Create N distinct resources, and retrieve each one from M concurrent
	// callers; note that distinct keys do not interfere with one another's
	// flight-tracking.
	for n := range N {
		key := fmt.Sprintf("resource:%d", n)
		for range M {
			wg.Go(func() {
				if v, _ := GetOrCreate(c, key, fetchResource); v == payload {
					atomic.AddInt64(&totalCalls, 1)
				}
			})
		}
	}
	wg.Wait()
	elapsed := time.Since(start)
	ranInTime := elapsed < 600*time.Millisecond

	fmt.Printf("number of resources created: %d\n", resourcesCreated)
	fmt.Printf("number of correct results: %d\n", totalCalls)
	fmt.Printf("ran in approximately one initializer time: %v\n", ranInTime)

	// Output:
	// number of resources created: 5
	// number of correct results: 50
	// ran in approximately one initializer time: true
}
