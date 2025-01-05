# Why this fork?

Basically, the original one is no longer maintained.

# concurrent map

As explained [here](http://golang.org/doc/faq#atomic_maps) and [here](http://blog.golang.org/go-maps-in-action), the `map` type in Go doesn't support concurrent reads and writes. `concurrent-map` provides a high-performance solution to this by sharding the map with minimal time spent waiting for locks.

Prior to Go 1.9, there was no concurrent map implementation in the stdlib. In Go 1.9, `sync.Map` was introduced. The new `sync.Map` has a few key differences from this map. The stdlib `sync.Map` is designed for append-only scenarios. So if you want to use the map for something more like in-memory db, you might benefit from using our version. You can read more about it in the golang repo, for example [here](https://github.com/golang/go/issues/21035) and [here](https://stackoverflow.com/questions/11063473/map-with-concurrent-access)

## Usage

Import the package:

```go
import (
	"github.com/guerinoni/concurrent-map"
)
```

```zsh
go get "github.com/guerinoni/concurrent-map"
```

The package is now imported under the "cmap" namespace.

## Example

```go
	// Create a new map.
	m := cmap.New[string, string]()

	// Sets item within map, sets "bar" under key "foo"
	m.Set("foo", "bar")

	// Retrieve item from map.
	bar, ok := m.Get("foo")

	// Removes item under key "foo"
	m.Remove("foo")
```

For more examples have a look at concurrent_map_test.go.

Running tests:

```zsh
go test ./...

go test -bench=. ./...
```

Comparing benchmarks:

```zsh
go test -bench=. -benchmem > before.txt
go test -bench=. -benchmem > after.txt

go install golang.org/x/perf/cmd/benchstat@latest

benchstat before.txt after.txt
```