package utils

import (
	"fmt"
	"testing"
)

func TestPutGet(t *testing.T) {
	m := NewHashMap[int]()
	m.Put("a", 1)
	m.Put("b", 2)
	if v, ok := m.Get("a"); !ok || v != 1 {
		t.Errorf("Get(a) = %v, %v", v, ok)
	}
	if _, ok := m.Get("missing"); ok {
		t.Error("Get of a missing key should return false")
	}
	if m.Len() != 2 {
		t.Errorf("Len = %d, want 2", m.Len())
	}
}

func TestOverwrite(t *testing.T) {
	m := NewHashMap[string]()
	m.Put("k", "old")
	m.Put("k", "new")
	if v, _ := m.Get("k"); v != "new" {
		t.Errorf("got %q, want new", v)
	}
	if m.Len() != 1 {
		t.Errorf("overwrite must not grow the map: Len = %d", m.Len())
	}
}

// Chiavi che finiscono nello stesso bucket: verifica la gestione delle collisioni.
func collidingKeys(m *HashMap[int], n int) []string {
	target := m.index("seed")
	keys := []string{"seed"}
	for i := 0; len(keys) < n; i++ {
		k := fmt.Sprintf("k%d", i)
		if m.index(k) == target {
			keys = append(keys, k)
		}
	}
	return keys
}

func TestCollisions(t *testing.T) {
	m := NewHashMap[int]()
	keys := collidingKeys(m, 4) // pochi, per non far scattare il resize
	for i, k := range keys {
		m.Put(k, i)
	}
	for i, k := range keys {
		if v, ok := m.Get(k); !ok || v != i {
			t.Errorf("Get(%q) = %v, %v; want %d", k, v, ok, i)
		}
	}

	// cancella in mezzo, in testa e in coda alla catena
	for _, k := range []string{keys[1], keys[3], keys[0]} {
		if !m.Delete(k) {
			t.Errorf("Delete(%q) returned false", k)
		}
		if m.Has(k) {
			t.Errorf("%q still present after Delete", k)
		}
	}
	if v, ok := m.Get(keys[2]); !ok || v != 2 {
		t.Errorf("surviving key lost: %v, %v", v, ok)
	}
	if m.Len() != 1 {
		t.Errorf("Len = %d, want 1", m.Len())
	}
}

func TestDeleteMissing(t *testing.T) {
	m := NewHashMap[int]()
	if m.Delete("nope") {
		t.Error("Delete of a missing key should return false")
	}
}

func TestResizeKeepsEverything(t *testing.T) {
	m := NewHashMap[int]()
	const n = 100000
	for i := 0; i < n; i++ {
		m.Put(fmt.Sprintf("key%d", i), i)
	}
	if m.Len() != n {
		t.Fatalf("Len = %d, want %d", m.Len(), n)
	}
	for i := 0; i < n; i++ {
		if v, ok := m.Get(fmt.Sprintf("key%d", i)); !ok || v != i {
			t.Fatalf("key%d: got %v, %v", i, v, ok)
		}
	}
	if len(m.GetKeys()) != n || len(m.GetValues()) != n || len(m.GetItems()) != n {
		t.Error("GetKeys/GetValues/GetItems must return all entries")
	}
}

func TestEmptyMap(t *testing.T) {
	m := NewHashMap[int]()
	if m.Len() != 0 || len(m.GetKeys()) != 0 {
		t.Error("new map should be empty")
	}
}
