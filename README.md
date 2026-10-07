# cache

[![Go Reference][badge-img]][badge-url]

A lightweight, type-safe, and generic key-value resource store for Go.

It handles the boilerplate of cache lookups, misses, and type assertions using
generics, allowing a single cache instance to hold values of different types
simultaneously.

[badge-img]: https://pkg.go.dev/badge/rs3.io/go/cache
[badge-url]: https://pkg.go.dev/rs3.io/go/cache

## Installation

```bash
go get rs3.io/go/cache
```

## Quick Start

```go
package main

import (
	"context"
	"fmt"

	"rs3.io/go/cache"
)

func main() {
	c := cache.New() // use cache.NewSync() for concurrent-safe workloads
	f := func(_ context.Context) (string, error) {
		return "user123@example.com", nil
	}

	// automatically manages lookups, cache misses, and initialization
	email, _ := cache.GetOrCreate(c, "user123", f)

	fmt.Println(email)
}
```

## License

MIT
