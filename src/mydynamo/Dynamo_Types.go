package mydynamo

import (
	"sync"
)

//Placeholder type for RPC functions that don't need an argument list or a return value
type Empty struct{}

//Context associated with some value
type Context struct {
	Clock VectorClock
}

//Information needed to connect to a DynamoNOde
type DynamoNode struct {
	Address string
	Port    string
}

//A single value, as well as the Context associated with it
type ObjectEntry struct {
	Context Context
	Value   []byte
}

//Result of a Get operation, a list of ObjectEntry structs
type DynamoResult struct {
	EntryList []ObjectEntry
}

//Arguments required for a Put operation: the key, the context, and the value
type PutArgs struct {
	Key     string
	Context Context
	Value   []byte
}

//Map type to store string type key and object entry pairs
//It provides methods to lock entries and be safe for concurrent use by multiple goroutines
type ObjectEntriesMap struct {
	entriesMap        *map[string][]ObjectEntry
	entriesMapMutex   *sync.RWMutex
	entriesRWMutexMap *sync.Map
}

//Return a new ObjectEntriesMap
func NewObjectEntriesMap() ObjectEntriesMap {
	return ObjectEntriesMap{
		entriesMap:        &map[string][]ObjectEntry{},
		entriesMapMutex:   &sync.RWMutex{},
		entriesRWMutexMap: &sync.Map{},
	}
}

// Get the keys of entries in the map
func (m *ObjectEntriesMap) GetKeys() []string {
	m.entriesMapMutex.RLock()
	defer m.entriesMapMutex.RUnlock()

	keys := make([]string, 0, len(*m.entriesMap))
	for key := range *m.entriesMap {
		keys = append(keys, key)
	}
	return keys
}

// Get the entries associated with the given key
func (m *ObjectEntriesMap) Get(key string) []ObjectEntry {
	m.entriesMapMutex.RLock()
	defer m.entriesMapMutex.RUnlock()

	var entries []ObjectEntry
	var ok bool

	if entries, ok = (*m.entriesMap)[key]; !ok {
		entries = make([]ObjectEntry, 0)
		(*m.entriesMap)[key] = entries
	}
	return entries
}

// Put the entries associated with the given key to the map
func (m *ObjectEntriesMap) Put(key string, entries []ObjectEntry) {
	m.entriesMapMutex.Lock()
	defer m.entriesMapMutex.Unlock()

	(*m.entriesMap)[key] = entries
}

// Locks RWMutex associated with the given key for writing
func (m *ObjectEntriesMap) Lock(key string) {
	mu, _ := m.entriesRWMutexMap.LoadOrStore(key, &sync.RWMutex{})
	mu.(*sync.RWMutex).Lock()
}

// Locks RWMutex associated with the given key for reading
func (m *ObjectEntriesMap) RLock(key string) {
	mu, _ := m.entriesRWMutexMap.LoadOrStore(key, &sync.RWMutex{})
	mu.(*sync.RWMutex).RLock()
}

// Unlocks RWMutex associated with the given key for writing
func (m *ObjectEntriesMap) Unlock(key string) {
	mu, _ := m.entriesRWMutexMap.LoadOrStore(key, &sync.RWMutex{})
	mu.(*sync.RWMutex).Unlock()
}

// Undoes a single RLock call on the RWMutex associated with the given key
func (m *ObjectEntriesMap) RUnlock(key string) {
	mu, _ := m.entriesRWMutexMap.LoadOrStore(key, &sync.RWMutex{})
	mu.(*sync.RWMutex).RUnlock()
}
