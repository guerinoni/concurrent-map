package cmap

import (
	"encoding/json"
	"sort"
	"strconv"
	"testing"
)

type Animal struct {
	name string
}

func TestMapCreation(t *testing.T) {
	m := New[string, string]()
	if m.shards == nil {
		t.Error("map is null.")
	}

	if m.Count() != 0 {
		t.Error("new map should be empty.")
	}
}

func TestInsert(t *testing.T) {
	m := New[string, Animal]()
	elephant := Animal{"elephant"}
	monkey := Animal{"monkey"}

	m.Set("elephant", elephant)
	m.Set("monkey", monkey)

	if m.Count() != 2 {
		t.Error("map should contain exactly two elements.")
	}
}

func TestInsertAbsent(t *testing.T) {
	m := New[string, Animal]()
	elephant := Animal{"elephant"}
	monkey := Animal{"monkey"}

	m.SetIfAbsent("elephant", elephant)
	if ok := m.SetIfAbsent("elephant", monkey); ok {
		t.Error("map set a new value even the entry is already present")
	}
}

func TestGet(t *testing.T) {
	m := New[string, Animal]()

	// Get a missing element.
	val, ok := m.Get("Money")

	if ok == true {
		t.Error("ok should be false when item is missing from map.")
	}

	if (val != Animal{}) {
		t.Error("Missing values should return as null.")
	}

	elephant := Animal{"elephant"}
	m.Set("elephant", elephant)

	// Retrieve inserted element.
	elephant, ok = m.Get("elephant")
	if ok == false {
		t.Error("ok should be true for item stored within the map.")
	}

	if elephant.name != "elephant" {
		t.Error("item was modified.")
	}
}

func TestHas(t *testing.T) {
	m := New[string, Animal]()

	// Get a missing element.
	if m.Has("Money") == true {
		t.Error("element shouldn't exists")
	}

	elephant := Animal{"elephant"}
	m.Set("elephant", elephant)

	if m.Has("elephant") == false {
		t.Error("element exists, expecting Has to return True.")
	}
}

func TestRemove(t *testing.T) {
	m := New[string, Animal]()

	monkey := Animal{"monkey"}
	m.Set("monkey", monkey)

	m.Remove("monkey")

	if m.Count() != 0 {
		t.Error("Expecting count to be zero once item was removed.")
	}

	temp, ok := m.Get("monkey")

	if ok != false {
		t.Error("Expecting ok to be false for missing items.")
	}

	if (temp != Animal{}) {
		t.Error("Expecting item to be nil after its removal.")
	}

	// Remove a none existing element.
	m.Remove("noone")
}

func TestRemoveCb(t *testing.T) {
	m := New[string, Animal]()

	monkey := Animal{"monkey"}
	m.Set("monkey", monkey)
	elephant := Animal{"elephant"}
	m.Set("elephant", elephant)

	var (
		mapKey   string
		mapVal   Animal
		wasFound bool
	)
	cb := func(key string, val Animal, exists bool) bool {
		mapKey = key
		mapVal = val
		wasFound = exists

		return val.name == "monkey"
	}

	// Monkey should be removed
	result := m.RemoveCb("monkey", cb)
	if !result {
		t.Errorf("Result was not true")
	}

	if mapKey != "monkey" {
		t.Error("Wrong key was provided to the callback")
	}

	if mapVal != monkey {
		t.Errorf("Wrong value was provided to the value")
	}

	if !wasFound {
		t.Errorf("Key was not found")
	}

	if m.Has("monkey") {
		t.Errorf("Key was not removed")
	}

	// Elephant should not be removed
	result = m.RemoveCb("elephant", cb)
	if result {
		t.Errorf("Result was true")
	}

	if mapKey != "elephant" {
		t.Error("Wrong key was provided to the callback")
	}

	if mapVal != elephant {
		t.Errorf("Wrong value was provided to the value")
	}

	if !wasFound {
		t.Errorf("Key was not found")
	}

	if !m.Has("elephant") {
		t.Errorf("Key was removed")
	}

	// Unset key should remain unset
	result = m.RemoveCb("horse", cb)
	if result {
		t.Errorf("Result was true")
	}

	if mapKey != "horse" {
		t.Error("Wrong key was provided to the callback")
	}

	if (mapVal != Animal{}) {
		t.Errorf("Wrong value was provided to the value")
	}

	if wasFound {
		t.Errorf("Key was found")
	}

	if m.Has("horse") {
		t.Errorf("Key was created")
	}
}

func TestPop(t *testing.T) {
	m := New[string, Animal]()

	monkey := Animal{"monkey"}
	m.Set("monkey", monkey)

	v, exists := m.Pop("monkey")

	if !exists || v != monkey {
		t.Error("Pop didn't find a monkey.")
	}

	v2, exists2 := m.Pop("monkey")

	if exists2 || v2 == monkey {
		t.Error("Pop keeps finding monkey")
	}

	if m.Count() != 0 {
		t.Error("Expecting count to be zero once item was Pop'ed.")
	}

	temp, ok := m.Get("monkey")

	if ok != false {
		t.Error("Expecting ok to be false for missing items.")
	}

	if (temp != Animal{}) {
		t.Error("Expecting item to be nil after its removal.")
	}
}

func TestCount(t *testing.T) {
	m := New[string, Animal]()
	for i := 0; i < 100; i++ {
		m.Set(strconv.Itoa(i), Animal{strconv.Itoa(i)})
	}

	if m.Count() != 100 {
		t.Error("Expecting 100 element within map.")
	}
}

func TestIsEmpty(t *testing.T) {
	m := New[string, Animal]()

	if m.IsEmpty() == false {
		t.Error("new map should be empty")
	}

	m.Set("elephant", Animal{"elephant"})

	if m.IsEmpty() != false {
		t.Error("map shouldn't be empty.")
	}
}

func TestBufferedIterator(t *testing.T) {
	m := New[string, Animal]()

	// Insert 100 elements.
	for i := 0; i < 100; i++ {
		m.Set(strconv.Itoa(i), Animal{strconv.Itoa(i)})
	}

	counter := 0
	// Iterate over elements.
	for item := range m.IterBuffered() {
		val := item.Val

		if (val == Animal{}) {
			t.Error("Expecting an object.")
		}
		counter++
	}

	if counter != 100 {
		t.Error("We should have counted 100 elements.")
	}
}

func TestClear(t *testing.T) {
	m := New[string, Animal]()

	// Insert 100 elements.
	for i := 0; i < 100; i++ {
		m.Set(strconv.Itoa(i), Animal{strconv.Itoa(i)})
	}

	m.Clear()

	if m.Count() != 0 {
		t.Error("We should have 0 elements.")
	}
}

func TestIterCb(t *testing.T) {
	m := New[string, Animal]()

	// Insert 100 elements.
	for i := 0; i < 100; i++ {
		m.Set(strconv.Itoa(i), Animal{strconv.Itoa(i)})
	}

	counter := 0
	// Iterate over elements.
	m.IterCb(func(key string, v Animal) {
		counter++
	})
	if counter != 100 {
		t.Error("We should have counted 100 elements.")
	}
}

func TestItems(t *testing.T) {
	m := New[string, Animal]()

	// Insert 100 elements.
	for i := 0; i < 100; i++ {
		m.Set(strconv.Itoa(i), Animal{strconv.Itoa(i)})
	}

	items := m.Items()

	if len(items) != 100 {
		t.Error("We should have counted 100 elements.")
	}
}

func TestConcurrent(t *testing.T) {
	m := New[string, int]()
	ch := make(chan int)
	const iterations = 1000
	var a [iterations]int

	// Using go routines insert 1000 ints into our map.
	go func() {
		for i := 0; i < iterations/2; i++ {
			// Add item to map.
			m.Set(strconv.Itoa(i), i)

			// Retrieve item from map.
			val, _ := m.Get(strconv.Itoa(i))

			// Write to channel inserted value.
			ch <- val
		} // Call go routine with current index.
	}()

	go func() {
		for i := iterations / 2; i < iterations; i++ {
			// Add item to map.
			m.Set(strconv.Itoa(i), i)

			// Retrieve item from map.
			val, _ := m.Get(strconv.Itoa(i))

			// Write to channel inserted value.
			ch <- val
		} // Call go routine with current index.
	}()

	// Wait for all go routines to finish.
	counter := 0
	for elem := range ch {
		a[counter] = elem
		counter++
		if counter == iterations {
			break
		}
	}

	// Sorts array, will make is simpler to verify all inserted values we're returned.
	sort.Ints(a[0:iterations])

	// Make sure map contains 1000 elements.
	if m.Count() != iterations {
		t.Error("Expecting 1000 elements.")
	}

	// Make sure all inserted values we're fetched from map.
	for i := 0; i < iterations; i++ {
		if i != a[i] {
			t.Error("missing value", i)
		}
	}
}

func TestJsonMarshal(t *testing.T) {
	SHARD_COUNT = 2
	defer func() {
		SHARD_COUNT = 32
	}()
	expected := "{\"a\":1,\"b\":2}"
	m := New[string, int]()
	m.Set("a", 1)
	m.Set("b", 2)
	j, err := json.Marshal(m)
	if err != nil {
		t.Error(err)
	}

	if string(j) != expected {
		t.Error("json", string(j), "differ from expected", expected)
		return
	}
}

func TestKeys(t *testing.T) {
	m := New[string, Animal]()

	// Insert 100 elements.
	for i := 0; i < 100; i++ {
		m.Set(strconv.Itoa(i), Animal{strconv.Itoa(i)})
	}

	keys := m.Keys()
	if len(keys) != 100 {
		t.Error("We should have counted 100 elements.")
	}
}

func TestMInsert(t *testing.T) {
	animals := map[string]Animal{
		"elephant": {"elephant"},
		"monkey":   {"monkey"},
	}
	m := New[string, Animal]()
	m.MSet(animals)

	if m.Count() != 2 {
		t.Error("map should contain exactly two elements.")
	}
}

func TestUpsert(t *testing.T) {
	dolphin := Animal{"dolphin"}
	whale := Animal{"whale"}
	tiger := Animal{"tiger"}
	lion := Animal{"lion"}

	cb := func(exists bool, valueInMap Animal, newValue Animal) Animal {
		if !exists {
			return newValue
		}
		valueInMap.name += newValue.name
		return valueInMap
	}

	m := New[string, Animal]()
	m.Set("marine", dolphin)
	m.Upsert("marine", whale, cb)
	m.Upsert("predator", tiger, cb)
	m.Upsert("predator", lion, cb)

	if m.Count() != 2 {
		t.Error("map should contain exactly two elements.")
	}

	marineAnimals, ok := m.Get("marine")
	if marineAnimals.name != "dolphinwhale" || !ok {
		t.Error("Set, then Upsert failed")
	}

	predators, ok := m.Get("predator")
	if !ok || predators.name != "tigerlion" {
		t.Error("Upsert, then Upsert failed")
	}
}

func TestKeysWhenRemoving(t *testing.T) {
	m := New[string, Animal]()

	// Insert 100 elements.
	Total := 100
	for i := 0; i < Total; i++ {
		m.Set(strconv.Itoa(i), Animal{strconv.Itoa(i)})
	}

	// Remove 10 elements concurrently.
	Num := 10
	for i := 0; i < Num; i++ {
		go func(c *ConcurrentMap[string, Animal], n int) {
			c.Remove(strconv.Itoa(n))
		}(m, i)
	}
	keys := m.Keys()
	for _, k := range keys {
		if k == "" {
			t.Error("Empty keys returned")
		}
	}
}

func TestUnDrainedIterBuffered(t *testing.T) {
	m := New[string, Animal]()
	// Insert 100 elements.
	Total := 100
	for i := 0; i < Total; i++ {
		m.Set(strconv.Itoa(i), Animal{strconv.Itoa(i)})
	}
	counter := 0
	// Iterate over elements.
	ch := m.IterBuffered()
	for item := range ch {
		val := item.Val

		if (val == Animal{}) {
			t.Error("Expecting an object.")
		}
		counter++
		if counter == 42 {
			break
		}
	}
	for i := Total; i < 2*Total; i++ {
		m.Set(strconv.Itoa(i), Animal{strconv.Itoa(i)})
	}
	for item := range ch {
		val := item.Val

		if (val == Animal{}) {
			t.Error("Expecting an object.")
		}
		counter++
	}

	if counter != 100 {
		t.Error("We should have been right where we stopped")
	}

	counter = 0
	for item := range m.IterBuffered() {
		val := item.Val

		if (val == Animal{}) {
			t.Error("Expecting an object.")
		}
		counter++
	}

	if counter != 200 {
		t.Error("We should have counted 200 elements.")
	}
}

func TestMapGenericInt(t *testing.T) {
	m := New[int, string]()
	if m.shards == nil {
		t.Error("map is null.")
	}
	if m.Count() != 0 {
		t.Error("new map should be empty.")
	}
	m.Set(1, "elephant")
	m.Set(2, "monkey")
	value, ok := m.Get(1)
	if !ok || value != "elephant" {
		t.Error("Cannot get value of key 1, which should be elephant")
	}
	value, ok = m.Get(2)
	if !ok || value != "monkey" {
		t.Error("Cannot get value of key 2, which should be monkey")
	}
	if m.Count() != 2 {
		t.Error("map should contain exactly two elements.")
	}
	m.Remove(1)
	m.Remove(2)
	if m.Count() != 0 {
		t.Error("Expecting count to be zero once item was removed.")
	}
}

func TestMapGenericStruct(t *testing.T) {
	type TmpStruct struct {
		Name string
	}
	m := New[TmpStruct, string]()
	if m.shards == nil {
		t.Error("map is null.")
	}
	if m.Count() != 0 {
		t.Error("new map should be empty.")
	}
	m.Set(TmpStruct{"elephant"}, "elephant")
	m.Set(TmpStruct{"monkey"}, "monkey")
	value, ok := m.Get(TmpStruct{"elephant"})
	if !ok || value != "elephant" {
		t.Error("Cannot get value of key elephant, which should be elephant")
	}
	value, ok = m.Get(TmpStruct{"monkey"})
	if !ok || value != "monkey" {
		t.Error("Cannot get value of key monkey, which should be monkey")
	}
	if m.Count() != 2 {
		t.Error("map should contain exactly two elements.")
	}
	m.Remove(TmpStruct{"elephant"})
	m.Remove(TmpStruct{"monkey"})
	if m.Count() != 0 {
		t.Error("Expecting count to be zero once item was removed.")
	}
}

func Test_genHasher(t *testing.T) {
	hasher := genHasher[string]()
	if hasher("499") != hasher("499") {
		t.Error("Expected hasher(\"499\") to equal itself")
	}
	if hasher("499") == hasher("500") {
		t.Error("Expected hasher(\"499\") to not equal hasher(\"500\")")
	}

	hasher1 := genHasher[int]()
	if hasher1(499) != hasher1(499) {
		t.Error("Expected hasher1(499) to equal itself")
	}
	if hasher1(499) == hasher1(500) {
		t.Error("Expected hasher1(499) to not equal hasher1(500)")
	}

	hasher2 := genHasher[uint]()
	if hasher2(499) != hasher2(499) {
		t.Error("Expected hasher2(499) to equal itself")
	}
	if hasher2(499) == hasher2(500) {
		t.Error("Expected hasher2(499) to not equal hasher2(500)")
	}

	hasher3 := genHasher[uintptr]()
	if hasher3(128) != hasher3(128) {
		t.Error("Expected hasher3(128) to equal itself")
	}
	if hasher3(128) == hasher3(129) {
		t.Error("Expected hasher3(128) to not equal hasher3(129)")
	}

	hasher4 := genHasher[byte]()
	if hasher4(128) != hasher4(128) {
		t.Error("Expected hasher4(128) to equal itself")
	}
	if hasher4(128) == hasher4(129) {
		t.Error("Expected hasher4(128) to not equal hasher4(129)")
	}

	hasher5 := genHasher[int8]()
	if hasher5(100) != hasher5(100) {
		t.Error("Expected hasher5(100) to equal itself")
	}
	if hasher5(100) == hasher5(101) {
		t.Error("Expected hasher5(100) to not equal hasher5(101)")
	}

	hasher6 := genHasher[uint8]()
	if hasher6(128) != hasher6(128) {
		t.Error("Expected hasher6(128) to equal itself")
	}
	if hasher6(128) == hasher6(129) {
		t.Error("Expected hasher6(128) to not equal hasher6(129)")
	}

	hasher7 := genHasher[int16]()
	if hasher7(100) != hasher7(100) {
		t.Error("Expected hasher7(100) to equal itself")
	}
	if hasher7(100) == hasher7(101) {
		t.Error("Expected hasher7(100) to not equal hasher7(101)")
	}

	hasher8 := genHasher[uint16]()
	if hasher8(128) != hasher8(128) {
		t.Error("Expected hasher8(128) to equal itself")
	}
	if hasher8(128) == hasher8(129) {
		t.Error("Expected hasher8(128) to not equal hasher8(129)")
	}

	hasher9 := genHasher[int32]()
	if hasher9(100) != hasher9(100) {
		t.Error("Expected hasher9(100) to equal itself")
	}
	if hasher9(100) == hasher9(101) {
		t.Error("Expected hasher9(100) to not equal hasher9(101)")
	}

	hasher10 := genHasher[uint32]()
	if hasher10(128) != hasher10(128) {
		t.Error("Expected hasher10(128) to equal itself")
	}
	if hasher10(128) == hasher10(129) {
		t.Error("Expected hasher10(128) to not equal hasher10(129)")
	}

	hasher11 := genHasher[float32]()
	if hasher11(128) != hasher11(128) {
		t.Error("Expected hasher11(128) to equal itself")
	}
	if hasher11(128) == hasher11(129) {
		t.Error("Expected hasher11(128) to not equal hasher11(129)")
	}

	hasher12 := genHasher[int64]()
	if hasher12(100) != hasher12(100) {
		t.Error("Expected hasher12(100) to equal itself")
	}
	if hasher12(100) == hasher12(101) {
		t.Error("Expected hasher12(100) to not equal hasher12(101)")
	}

	hasher13 := genHasher[uint64]()
	if hasher13(128) != hasher13(128) {
		t.Error("Expected hasher13(128) to equal itself")
	}
	if hasher13(128) == hasher13(129) {
		t.Error("Expected hasher13(128) to not equal hasher13(129)")
	}

	hasher14 := genHasher[float64]()
	if hasher14(128) != hasher14(128) {
		t.Error("Expected hasher14(128) to equal itself")
	}
	if hasher14(128) == hasher14(129) {
		t.Error("Expected hasher14(128) to not equal hasher14(129)")
	}

	hasher15 := genHasher[complex64]()
	if hasher15(complex(1, 2)) != hasher15(complex(1, 2)) {
		t.Error("Expected hasher15(complex(1, 2)) to equal itself")
	}
	if hasher15(complex(1, 2)) == hasher15(complex(1, 3)) {
		t.Error("Expected hasher15(complex(1, 2)) to not equal hasher15(complex(1, 3))")
	}

	hasher16 := genHasher[complex128]()
	if hasher16(complex(1, 2)) != hasher16(complex(1, 2)) {
		t.Error("Expected hasher16(complex(1, 2)) to equal itself")
	}
	if hasher16(complex(1, 2)) == hasher16(complex(1, 3)) {
		t.Error("Expected hasher16(complex(1, 2)) to not equal hasher16(complex(1, 3))")
	}

	type S struct {
		a int
	}
	hasher17 := genHasher[*S]()
	s1 := &S{a: 1}
	s2 := &S{a: 2}
	if hasher17(s1) != hasher17(s1) {
		t.Error("Expected hasher17(s1) to equal itself")
	}
	if hasher17(s1) == hasher17(s2) {
		t.Error("Expected hasher17(s1) to not equal hasher17(s2)")
	}

	hasher18 := genHasher[chan int]()
	ch1 := make(chan int)
	ch2 := make(chan int)
	if hasher18(ch1) != hasher18(ch1) {
		t.Error("Expected hasher18(ch1) to equal itself")
	}
	if hasher18(ch1) == hasher18(ch2) {
		t.Error("Expected hasher18(ch1) to not equal hasher18(ch2)")
	}
}
