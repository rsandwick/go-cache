# cache

[![Go Reference][godoc-img]][godoc-url]
[![License: MIT][license-img]][license-url]

A lightweight key-value store for type-safe access to values of arbitrary
types.

It reduces the boilerplate of cache hits/misses and type assertions using Go
generics, allowing a single cache instance to hold values of different types
simultaneously while maintaining compile-time type checks in cache accesses.

[godoc-img]: https://pkg.go.dev/badge/rs3.io/go/cache
[godoc-url]: https://pkg.go.dev/rs3.io/go/cache
[license-img]: https://img.shields.io/badge/License-MIT-yellow.svg
[license-url]: https://opensource.org/license/mit

## 🚀 Features

- **Heterogeneous Storage:** Store and retrieve primitives, structs, and slices
  in the same cache instance with compile-time type validation.
- **Cache Stampede Prevention:** Internal flight-tracking eliminates duplicate
  concurrent initializations.†
- **Zero Allocations on Hits:** Cache hits avoid heap allocations (0 B/op, 0
  allocs/op).
- **Zero Dependencies:** Implemented with Go standard library only.

†*only with `SyncCache`*

## 📦 Installation

```sh
go get rs3.io/go/cache
```

## ⚡ Quick Start

### Concurrent-Safe Heterogeneous Store

Use `cache.NewSync()` for concurrent workloads. A single store safely handles
diverse entry types:

```go
package main

import (
	"errors"
	"fmt"

	"rs3.io/go/cache"
)

type User struct {
	Name string
}

type Config struct {
	MaxConns int
}

func main() {
	c := cache.NewSync()

	// Store different types under different keys
	cache.Set(c, "app:version", "v1.0.0")
	cache.Set(c, "system:config", Config{MaxConns: 100})
	cache.Set(c, "active:user", User{Name: "Alice"})

	// Safely retrieve with explicit type guarantees
	if version, ok, _ := cache.Get[string](c, "app:version"); ok {
		fmt.Println("Version:", version)
	}
	if config, ok, _ := cache.Get[Config](c, "system:config"); ok {
		fmt.Printf("Max Connections: %d\n", config.MaxConns)
	}

	// Type mismatches are caught safely by a standard wrapped ErrTypeMismatch
	_, _, err := cache.Get[int](c, "app:version")
	if errors.Is(err, cache.ErrTypeMismatch) {
		fmt.Println("Caught invalid type cast safely!")
	}
}
```

### Cache Stampede Protection (`GetOrCreate`)

If multiple goroutines attempt to `GetOrCreate` a missing key concurrently, the
initialization function runs exactly once:

```go
package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"rs3.io/go/cache"
)

func main() {
	f := func(key string) func() (string, error) {
		return func() (string, error) {
			// The initializer runs exactly once under concurrent load
			fmt.Println("computing " + key)
			time.Sleep(500*time.Millisecond)
			return key, nil
		}
	}

	const M = 5
	const N = 10
	var n int64
	var wg sync.WaitGroup

	c := cache.NewSync()
	start := time.Now()
	// Create M distinct resources, and retrieve each one N times; note that
	// the total program time should still be about 500ms, since the distinct
	// keys do not interfere with one another's flight-tracking.
	for m := range M {
		key := fmt.Sprintf("resource:%d", m+1)
		for range N {
			wg.Go(func() {
				if v, _ := cache.GetOrCreate(c, key, f(key)); v == key {
					atomic.AddInt64(&n, 1)
				}
			})
		}
	}
	wg.Wait()

	fmt.Printf("retrieved %d resources in %v\n", n, time.Since(start))
}
```

## 🏎️ Performance Benchmarks

Evaluated on an Apple M1 processor running parallel load thread execution
groups:

| Operation                       | Throughput       | Memory | Allocations  |
| ------------------------------- | ---------------: | -----: | -----------: |
| **`SimpleCache` Hit**           |      ~9.45 ns/op | 0 B/op |  0 allocs/op |
| **`SyncCache` Hit (Contested)** |    ~119.70 ns/op | 0 B/op |  0 allocs/op |

*To run benchmarks locally:*

```sh
go test -bench=. -benchmem ./...
```

## ⚖️ License

Distributed under the MIT License. See [LICENSE](./LICENSE) for more information.
