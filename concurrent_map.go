package cmap

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/bits"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"unsafe"
)

var SHARD_COUNT = runtime.GOMAXPROCS(-1)

// ConcurrentMap is "thread" safe map for generic types.
// To avoid lock bottlenecks this map is dived to several (SHARD_COUNT) map shards.
type ConcurrentMap[K comparable, V any] struct {
	shards   []*ConcurrentMapShared[K, V]
	sharding func(K) uintptr
}

type ConcurrentMapShared[K comparable, V any] struct {
	items map[K]V
	sync.RWMutex
}

// New creates a new concurrent map with a generic key type.
func New[K comparable, V any]() *ConcurrentMap[K, V] {
	shared := make([]*ConcurrentMapShared[K, V], SHARD_COUNT)
	for i := 0; i < SHARD_COUNT; i++ {
		shared[i] = &ConcurrentMapShared[K, V]{items: make(map[K]V)}
	}
	return &ConcurrentMap[K, V]{
		shards:   shared,
		sharding: genHasher[K](),
	}
}

// NewWithCustomShardingFunction creates a new concurrent map with a generic key type and a custom sharding function.
func NewWithCustomShardingFunction[K comparable, V any](sharding func(key K) uintptr) ConcurrentMap[K, V] {
	shared := make([]*ConcurrentMapShared[K, V], SHARD_COUNT)
	for i := 0; i < SHARD_COUNT; i++ {
		shared[i] = &ConcurrentMapShared[K, V]{items: make(map[K]V)}
	}
	return ConcurrentMap[K, V]{
		shards:   shared,
		sharding: sharding,
	}
}

// GetShard returns shard under given key
func (m ConcurrentMap[K, V]) GetShard(key K) *ConcurrentMapShared[K, V] {
	return m.shards[uint(m.sharding(key))%uint(SHARD_COUNT)]
}

func (m ConcurrentMap[K, V]) MSet(data map[K]V) {
	for key, value := range data {
		shard := m.GetShard(key)
		shard.Lock()
		shard.items[key] = value
		shard.Unlock()
	}
}

// Sets the given value under the specified key.
func (m ConcurrentMap[K, V]) Set(key K, value V) {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	shard.items[key] = value
	shard.Unlock()
}

// Callback to return new element to be inserted into the map
// It is called while lock is held, therefore it MUST NOT
// try to access other keys in same map, as it can lead to deadlock since
// Go sync.RWLock is not reentrant
type UpsertCb[K comparable, V any] func(exist bool, valueInMap V, newValue V) V

// Insert or Update - updates existing element or inserts a new one using UpsertCb
func (m ConcurrentMap[K, V]) Upsert(key K, value V, cb UpsertCb[K, V]) (res V) {
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	res = cb(ok, v, value)
	shard.items[key] = res
	shard.Unlock()
	return res
}

// Sets the given value under the specified key if no value was associated with it.
func (m ConcurrentMap[K, V]) SetIfAbsent(key K, value V) bool {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	_, ok := shard.items[key]
	if !ok {
		shard.items[key] = value
	}
	shard.Unlock()
	return !ok
}

// Get retrieves an element from map under given key.
func (m ConcurrentMap[K, V]) Get(key K) (V, bool) {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// Get item from shard.
	val, ok := shard.items[key]
	shard.RUnlock()
	return val, ok
}

// Count returns the number of elements within the map.
func (m ConcurrentMap[K, V]) Count() int {
	count := 0
	for i := 0; i < SHARD_COUNT; i++ {
		shard := m.shards[i]
		shard.RLock()
		count += len(shard.items)
		shard.RUnlock()
	}
	return count
}

// Looks up an item under specified key
func (m ConcurrentMap[K, V]) Has(key K) bool {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// See if element is within shard.
	_, ok := shard.items[key]
	shard.RUnlock()
	return ok
}

// Remove removes an element from the map.
func (m ConcurrentMap[K, V]) Remove(key K) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	delete(shard.items, key)
	shard.Unlock()
}

// RemoveCb is a callback executed in a map.RemoveCb() call, while Lock is held
// If returns true, the element will be removed from the map
type RemoveCb[K comparable, V any] func(key K, v V, exists bool) bool

// RemoveCb locks the shard containing the key, retrieves its current value and calls the callback with those params
// If callback returns true and element exists, it will remove it from the map
// Returns the value returned by the callback (even if element was not present in the map)
func (m ConcurrentMap[K, V]) RemoveCb(key K, cb RemoveCb[K, V]) bool {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	remove := cb(key, v, ok)
	if remove && ok {
		delete(shard.items, key)
	}
	shard.Unlock()
	return remove
}

// Pop removes an element from the map and returns it
func (m ConcurrentMap[K, V]) Pop(key K) (v V, exists bool) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	v, exists = shard.items[key]
	delete(shard.items, key)
	shard.Unlock()
	return v, exists
}

// IsEmpty checks if map is empty.
func (m ConcurrentMap[K, V]) IsEmpty() bool {
	return m.Count() == 0
}

// Used by the Iter & IterBuffered functions to wrap two variables together over a channel,
type Tuple[K comparable, V any] struct {
	Key K
	Val V
}

// IterBuffered returns a buffered iterator which could be used in a for range loop.
func (m ConcurrentMap[K, V]) IterBuffered() <-chan Tuple[K, V] {
	chans := snapshot(m)
	total := 0
	for _, c := range chans {
		total += cap(c)
	}
	ch := make(chan Tuple[K, V], total)
	go fanIn(chans, ch)
	return ch
}

// Clear removes all items from map.
func (m ConcurrentMap[K, V]) Clear() {
	for item := range m.IterBuffered() {
		m.Remove(item.Key)
	}
}

// Returns a array of channels that contains elements in each shard,
// which likely takes a snapshot of `m`.
// It returns once the size of each buffered channel is determined,
// before all the channels are populated using goroutines.
func snapshot[K comparable, V any](m ConcurrentMap[K, V]) (chans []chan Tuple[K, V]) {
	//When you access map items before initializing.
	if len(m.shards) == 0 {
		panic(`cmap.ConcurrentMap is not initialized. Should run New() before usage.`)
	}
	chans = make([]chan Tuple[K, V], SHARD_COUNT)
	wg := sync.WaitGroup{}
	wg.Add(SHARD_COUNT)
	// Foreach shard.
	for index, shard := range m.shards {
		go func(index int, shard *ConcurrentMapShared[K, V]) {
			// Foreach key, value pair.
			shard.RLock()
			chans[index] = make(chan Tuple[K, V], len(shard.items))
			wg.Done()
			for key, val := range shard.items {
				chans[index] <- Tuple[K, V]{key, val}
			}
			shard.RUnlock()
			close(chans[index])
		}(index, shard)
	}
	wg.Wait()
	return chans
}

// fanIn reads elements from channels `chans` into channel `out`
func fanIn[K comparable, V any](chans []chan Tuple[K, V], out chan Tuple[K, V]) {
	wg := sync.WaitGroup{}
	wg.Add(len(chans))
	for _, ch := range chans {
		go func(ch chan Tuple[K, V]) {
			for t := range ch {
				out <- t
			}
			wg.Done()
		}(ch)
	}
	wg.Wait()
	close(out)
}

// Items returns all items as map[string]V
func (m ConcurrentMap[K, V]) Items() map[K]V {
	tmp := make(map[K]V)

	// Insert items to temporary map.
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}

	return tmp
}

// Iterator callbacalled for every key,value found in
// maps. RLock is held for all calls for a given shard
// therefore callback sess consistent view of a shard,
// but not across the shards
type IterCb[K comparable, V any] func(key K, v V)

// Callback based iterator, cheapest way to read
// all elements in a map.
func (m ConcurrentMap[K, V]) IterCb(fn IterCb[K, V]) {
	for idx := range m.shards {
		shard := (m.shards)[idx]
		shard.RLock()
		for key, value := range shard.items {
			fn(key, value)
		}
		shard.RUnlock()
	}
}

// Keys returns all keys as []string
func (m ConcurrentMap[K, V]) Keys() []K {
	count := m.Count()
	ch := make(chan K, count)
	go func() {
		// Foreach shard.
		wg := sync.WaitGroup{}
		wg.Add(SHARD_COUNT)
		for _, shard := range m.shards {
			go func(shard *ConcurrentMapShared[K, V]) {
				// Foreach key, value pair.
				shard.RLock()
				for key := range shard.items {
					ch <- key
				}
				shard.RUnlock()
				wg.Done()
			}(shard)
		}
		wg.Wait()
		close(ch)
	}()

	// Generate keys
	keys := make([]K, 0, count)
	for k := range ch {
		keys = append(keys, k)
	}
	return keys
}

// Reviles ConcurrentMap "private" variables to json marshal.
func (m ConcurrentMap[K, V]) MarshalJSON() ([]byte, error) {
	// Create a temporary map, which will hold all item spread across shards.
	tmp := make(map[K]V)

	// Insert items to temporary map.
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}
	return json.Marshal(tmp)
}

// Reverse process of Marshal.
func (m *ConcurrentMap[K, V]) UnmarshalJSON(b []byte) (err error) {
	tmp := make(map[K]V)

	// Unmarshal into a single map.
	if err := json.Unmarshal(b, &tmp); err != nil {
		return err
	}

	// foreach key,value pair in temporary map insert into our concurrent map.
	for key, val := range tmp {
		m.Set(key, val)
	}
	return nil
}

const (
	// hash input allowed sizes
	byteSize = 1 << iota
	wordSize
	dwordSize
	qwordSize
	owordSize
)
const (
	prime1 uint64 = 11400714785074694791
	prime2 uint64 = 14029467366897019727
	prime3 uint64 = 1609587929392839161
	prime4 uint64 = 9650029242287828579
	prime5 uint64 = 2870177450012600261
)
const intSizeBytes = strconv.IntSize >> 3

var prime1v = prime1

func u64(b []byte) uint64 { return binary.LittleEndian.Uint64(b) }
func u32(b []byte) uint32 { return binary.LittleEndian.Uint32(b) }
func round(acc, input uint64) uint64 {
	acc += input * prime2
	acc = rol31(acc)
	acc *= prime1
	return acc
}
func mergeRound(acc, val uint64) uint64 {
	val = round(0, val)
	acc ^= val
	acc = acc*prime1 + prime4
	return acc
}
func rol1(x uint64) uint64  { return bits.RotateLeft64(x, 1) }
func rol7(x uint64) uint64  { return bits.RotateLeft64(x, 7) }
func rol11(x uint64) uint64 { return bits.RotateLeft64(x, 11) }
func rol12(x uint64) uint64 { return bits.RotateLeft64(x, 12) }
func rol18(x uint64) uint64 { return bits.RotateLeft64(x, 18) }
func rol23(x uint64) uint64 { return bits.RotateLeft64(x, 23) }
func rol27(x uint64) uint64 { return bits.RotateLeft64(x, 27) }
func rol31(x uint64) uint64 { return bits.RotateLeft64(x, 31) }

// xxHash implementation for known key type sizes, minimal with no branching
var (
	// byte hasher, key size -> 1 byte
	byteHasher = func(key uint8) uintptr {
		h := prime5 + 1
		h ^= uint64(key) * prime5
		h = bits.RotateLeft64(h, 11) * prime1
		h ^= h >> 33
		h *= prime2
		h ^= h >> 29
		h *= prime3
		h ^= h >> 32
		return uintptr(h)
	}
	// word hasher, key size -> 2 bytes
	wordHasher = func(key uint16) uintptr {
		h := prime5 + 2
		h ^= (uint64(key) & 0xff) * prime5
		h = bits.RotateLeft64(h, 11) * prime1
		h ^= ((uint64(key) >> 8) & 0xff) * prime5
		h = bits.RotateLeft64(h, 11) * prime1
		h ^= h >> 33
		h *= prime2
		h ^= h >> 29
		h *= prime3
		h ^= h >> 32
		return uintptr(h)
	}
	// dword hasher, key size -> 4 bytes
	dwordHasher = func(key uint32) uintptr {
		h := prime5 + 4
		h ^= uint64(key) * prime1
		h = bits.RotateLeft64(h, 23)*prime2 + prime3
		h ^= h >> 33
		h *= prime2
		h ^= h >> 29
		h *= prime3
		h ^= h >> 32
		return uintptr(h)
	}
	// qword hasher, key size -> 8 bytes
	qwordHasher = func(key uint64) uintptr {
		k1 := key * prime2
		k1 = bits.RotateLeft64(k1, 31)
		k1 *= prime1
		h := (prime5 + 8) ^ k1
		h = bits.RotateLeft64(h, 27)*prime1 + prime4
		h ^= h >> 33
		h *= prime2
		h ^= h >> 29
		h *= prime3
		h ^= h >> 32
		return uintptr(h)
	}
	float32Hasher = func(key float32) uintptr {
		k := *(*uint32)(unsafe.Pointer(&key))
		h := prime5 + 4
		h ^= uint64(k) * prime1
		h = bits.RotateLeft64(h, 23)*prime2 + prime3
		h ^= h >> 33
		h *= prime2
		h ^= h >> 29
		h *= prime3
		h ^= h >> 32
		return uintptr(h)
	}
	float64Hasher = func(key float64) uintptr {
		k := *(*uint64)(unsafe.Pointer(&key))
		h := prime5 + 4
		h ^= uint64(k) * prime1
		h = bits.RotateLeft64(h, 23)*prime2 + prime3
		h ^= h >> 33
		h *= prime2
		h ^= h >> 29
		h *= prime3
		h ^= h >> 32
		return uintptr(h)
	}
)

func genHasher[K comparable]() func(K) uintptr {
	var hasher func(K) uintptr
	switch any(*new(K)).(type) {
	case string:
		// use default xxHash algorithm for key of any size for golang string data type
		hasher = func(key K) uintptr {
			sh := (*reflect.StringHeader)(unsafe.Pointer(&key))
			b := unsafe.Slice((*byte)(unsafe.Pointer(sh.Data)), sh.Len)
			n := sh.Len
			var h uint64
			if n >= 32 {
				v1 := prime1v + prime2
				v2 := prime2
				v3 := uint64(0)
				v4 := -prime1v
				for len(b) >= 32 {
					v1 = round(v1, u64(b[0:8:len(b)]))
					v2 = round(v2, u64(b[8:16:len(b)]))
					v3 = round(v3, u64(b[16:24:len(b)]))
					v4 = round(v4, u64(b[24:32:len(b)]))
					b = b[32:len(b):len(b)]
				}
				h = rol1(v1) + rol7(v2) + rol12(v3) + rol18(v4)
				h = mergeRound(h, v1)
				h = mergeRound(h, v2)
				h = mergeRound(h, v3)
				h = mergeRound(h, v4)
			} else {
				h = prime5
			}
			h += uint64(n)
			i, end := 0, len(b)
			for ; i+8 <= end; i += 8 {
				k1 := round(0, u64(b[i:i+8:len(b)]))
				h ^= k1
				h = rol27(h)*prime1 + prime4
			}
			if i+4 <= end {
				h ^= uint64(u32(b[i:i+4:len(b)])) * prime1
				h = rol23(h)*prime2 + prime3
				i += 4
			}
			for ; i < end; i++ {
				h ^= uint64(b[i]) * prime5
				h = rol11(h) * prime1
			}
			h ^= h >> 33
			h *= prime2
			h ^= h >> 29
			h *= prime3
			h ^= h >> 32
			return uintptr(h)
		}
	case int, uint, uintptr:
		switch intSizeBytes {
		case 2:
			// word hasher
			hasher = *(*func(K) uintptr)(unsafe.Pointer(&wordHasher))
		case 4:
			// Dword hasher
			hasher = *(*func(K) uintptr)(unsafe.Pointer(&dwordHasher))
		case 8:
			// Qword Hash
			hasher = *(*func(K) uintptr)(unsafe.Pointer(&qwordHasher))
		}
	case int8, uint8:
		// byte hasher
		hasher = *(*func(K) uintptr)(unsafe.Pointer(&byteHasher))
	case int16, uint16:
		// word hasher
		hasher = *(*func(K) uintptr)(unsafe.Pointer(&wordHasher))
	case int32, uint32:
		// Dword hasher
		hasher = *(*func(K) uintptr)(unsafe.Pointer(&dwordHasher))
	case float32:
		hasher = *(*func(K) uintptr)(unsafe.Pointer(&float32Hasher))
	case int64, uint64:
		// Qword hasher
		hasher = *(*func(K) uintptr)(unsafe.Pointer(&qwordHasher))
	case float64:
		hasher = *(*func(K) uintptr)(unsafe.Pointer(&float64Hasher))
	case complex64:
		hasher = func(key K) uintptr {
			k := *(*uint64)(unsafe.Pointer(&key))
			h := prime5 + 4
			h ^= uint64(k) * prime1
			h = bits.RotateLeft64(h, 23)*prime2 + prime3
			h ^= h >> 33
			h *= prime2
			h ^= h >> 29
			h *= prime3
			h ^= h >> 32
			return uintptr(h)
		}
	case complex128:
		// Oword hasher, key size -> 16 bytes
		hasher = func(key K) uintptr {
			b := *(*[owordSize]byte)(unsafe.Pointer(&key))
			h := prime5 + 16
			val := uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
				uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
			k1 := val * prime2
			k1 = bits.RotateLeft64(k1, 31)
			k1 *= prime1
			h ^= k1
			h = bits.RotateLeft64(h, 27)*prime1 + prime4
			val = uint64(b[8]) | uint64(b[9])<<8 | uint64(b[10])<<16 | uint64(b[11])<<24 |
				uint64(b[12])<<32 | uint64(b[13])<<40 | uint64(b[14])<<48 | uint64(b[15])<<56
			k1 = val * prime2
			k1 = bits.RotateLeft64(k1, 31)
			k1 *= prime1
			h ^= k1
			h = bits.RotateLeft64(h, 27)*prime1 + prime4
			h ^= h >> 33
			h *= prime2
			h ^= h >> 29
			h *= prime3
			h ^= h >> 32
			return uintptr(h)
		}
	default:
		hasher = func(key K) uintptr {
			v := reflect.ValueOf(key)
			var k uintptr

			if v.Kind() == reflect.Ptr { // Check if the key is a pointer
				k = v.Pointer() // Get the pointer's address
			} else {
				// Handle non-pointer keys
				// Convert the key into a string representation for hashing
				keyStr := fmt.Sprintf("%v", key)

				// Generate a hash value for the string representation of the key
				h := fnv.New64a()
				h.Write([]byte(keyStr))
				k = uintptr(h.Sum64())
			}

			switch intSizeBytes {
			case 2:
				return wordHasher(uint16(k))
			case 4:
				return dwordHasher(uint32(k))
			case 8:
				return qwordHasher(uint64(k))
			default:
				return uintptr(unsafe.Pointer(&key)) // Default handle logging
			}
		}
	}
	return hasher
}
