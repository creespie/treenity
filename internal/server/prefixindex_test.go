package server

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
)

// ---------- helper ----------

// Un consumer "finto": per l'indice contano solo nome e prefisso.
func fakeConsumer(id, prefix string) *consumer {
	return &consumer{clientID: id, prefix: prefix}
}

// Nomi dei consumer restituiti da match, ordinati (l'ordine non conta).
func matchIDs(p *prefixIndex, key string) []string {
	var ids []string
	for _, c := range p.match([]byte(key)) {
		ids = append(ids, c.clientID)
	}
	sort.Strings(ids)
	return ids
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ---------- 1. Basic prefix matching ----------

func TestBasicPrefixMatching(t *testing.T) {
	p := newPrefixIndex()
	p.add(fakeConsumer("c", "user.create"))

	tests := []struct {
		name  string
		key   string
		match bool
	}{
		{"exact match", "user.create", true},
		{"key longer than prefix", "user.create.v2", true},
		{"key shorter than prefix", "user.cr", false},
		{"different key", "user.delete", false},
		{"unrelated key", "order.create", false},
		{"prefix in the middle of the key", "x.user.create", false},
		{"case sensitive", "User.create", false},
		{"empty key", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := len(p.match([]byte(tt.key))) == 1
			if got != tt.match {
				t.Errorf("key %q: match=%v, want %v", tt.key, got, tt.match)
			}
		})
	}
}

func TestNestedPrefixes(t *testing.T) {
	p := newPrefixIndex()
	for _, pre := range []string{"u", "us", "user", "user.", "user.create", "user.d", "order"} {
		p.add(fakeConsumer(pre, pre))
	}
	got := matchIDs(p, "user.create")
	want := []string{"u", "us", "user", "user.", "user.create"}
	if !equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestSamePrefixManyConsumers(t *testing.T) {
	p := newPrefixIndex()
	p.add(fakeConsumer("a", "log"))
	p.add(fakeConsumer("b", "log"))
	p.add(fakeConsumer("c", "log"))
	if got := matchIDs(p, "log.error"); !equal(got, []string{"a", "b", "c"}) {
		t.Errorf("got %v", got)
	}
}

func TestBinaryKeys(t *testing.T) {
	p := newPrefixIndex()
	p.add(fakeConsumer("bin", "\x00\xff"))
	if len(p.match([]byte{0x00, 0xff, 0x10})) != 1 {
		t.Error("binary prefix should match")
	}
	if len(p.match([]byte{0x00, 0xfe})) != 0 {
		t.Error("different binary key should not match")
	}
}

// ---------- 2. Empty prefix handling ----------

func TestEmptyPrefixReceivesEverything(t *testing.T) {
	p := newPrefixIndex()
	p.add(fakeConsumer("all", ""))
	for _, key := range []string{"", "a", "user.create", "\x00binary", strings.Repeat("x", 1024)} {
		if got := matchIDs(p, key); !equal(got, []string{"all"}) {
			t.Errorf("key %q: got %v, want [all]", key, got)
		}
	}
}

func TestEmptyPrefixGoesToDirectList(t *testing.T) {
	p := newPrefixIndex()
	p.add(fakeConsumer("all", ""))
	if len(p.direct) != 1 {
		t.Errorf("empty-prefix consumer should be in the direct list")
	}
	if p.byPrefix.Len() != 0 {
		t.Errorf("empty-prefix consumer should not be in the prefix map")
	}
}

// ---------- 3. Data structure operations ----------

func TestRemove(t *testing.T) {
	p := newPrefixIndex()
	a := fakeConsumer("a", "user")
	b := fakeConsumer("b", "user")
	all := fakeConsumer("all", "")
	p.add(a)
	p.add(b)
	p.add(all)

	p.remove(a)
	if got := matchIDs(p, "user.x"); !equal(got, []string{"all", "b"}) {
		t.Errorf("after removing a: got %v", got)
	}

	p.remove(b) // ultimo con prefisso "user": la voce va tolta dalla mappa
	if p.byPrefix.Has("user") {
		t.Error("empty prefix list should be deleted from the map")
	}

	p.remove(all)
	if got := matchIDs(p, "user.x"); len(got) != 0 {
		t.Errorf("after removing everyone: got %v", got)
	}

	p.remove(fakeConsumer("ghost", "user")) // non presente: non deve esplodere
}

func TestFindAndList(t *testing.T) {
	p := newPrefixIndex()
	a := fakeConsumer("a", "x")
	b := fakeConsumer("b", "")
	p.add(a)
	p.add(b)

	if p.find("a") != a || p.find("b") != b {
		t.Error("find should return the registered consumer")
	}
	if p.find("nobody") != nil {
		t.Error("find of an unknown name should return nil")
	}
	if len(p.list()) != 2 {
		t.Errorf("list: got %d consumers, want 2", len(p.list()))
	}

	l := p.list()
	l[0] = nil // list() deve restituire una copia
	if p.find("a") == nil {
		t.Error("modifying the result of list() changed the index")
	}
}

// ---------- 4. Edge cases ----------

func TestEmptyIndex(t *testing.T) {
	p := newPrefixIndex()
	if got := p.match([]byte("anything")); len(got) != 0 {
		t.Errorf("empty index matched %d consumers", len(got))
	}
	if len(p.list()) != 0 || p.find("x") != nil {
		t.Error("empty index should have no consumers")
	}
}

func TestMinimalDataset(t *testing.T) {
	p := newPrefixIndex()
	p.add(fakeConsumer("one", "a"))
	if got := matchIDs(p, "a"); !equal(got, []string{"one"}) {
		t.Errorf("got %v", got)
	}
	if got := matchIDs(p, "b"); len(got) != 0 {
		t.Errorf("got %v", got)
	}
}

func TestLargeScale(t *testing.T) {
	p := newPrefixIndex()
	const n = 10000
	for i := 0; i < n; i++ {
		p.add(fakeConsumer(fmt.Sprintf("c%d", i), fmt.Sprintf("p%d", i)))
	}
	for i := 0; i < 100; i++ {
		p.add(fakeConsumer(fmt.Sprintf("all%d", i), ""))
	}

	// "p1234" è prefissato da p1, p12, p123, p1234
	got := p.match([]byte("p1234"))
	if len(got) != 100+4 {
		t.Errorf("got %d matches, want %d", len(got), 104)
	}
	if len(p.list()) != n+100 {
		t.Errorf("list: got %d, want %d", len(p.list()), n+100)
	}
}

// ---------- 5. Integration: complete filtering with mixed prefixes ----------

// Confronta l'indice con l'algoritmo "ingenuo" (strings.HasPrefix su tutti)
// su molti consumer e chiavi casuali, con prefissi vuoti, corti, lunghi e binari.
func TestFilteringMatchesBruteForce(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	alphabet := []byte("ab.\x00")
	randStr := func(max int) string {
		b := make([]byte, rng.Intn(max+1))
		for i := range b {
			b[i] = alphabet[rng.Intn(len(alphabet))]
		}
		return string(b)
	}

	p := newPrefixIndex()
	var consumers []*consumer
	for i := 0; i < 300; i++ {
		c := fakeConsumer(fmt.Sprintf("c%d", i), randStr(4))
		consumers = append(consumers, c)
		p.add(c)
	}
	// togliamone alcuni, per testare anche remove nel flusso completo
	for i := 0; i < 300; i += 7 {
		p.remove(consumers[i])
	}

	for k := 0; k < 2000; k++ {
		key := randStr(8)
		var want []string
		for i, c := range consumers {
			if i%7 == 0 {
				continue // rimosso
			}
			if strings.HasPrefix(key, c.prefix) {
				want = append(want, c.clientID)
			}
		}
		sort.Strings(want)
		if got := matchIDs(p, key); !equal(got, want) {
			t.Fatalf("key %q: got %v, want %v", key, got, want)
		}
	}
}

// Il filtro usato per i messaggi vecchi (backlog) deve dare lo stesso
// risultato dell'indice: prefisso e offset di partenza.
func TestBacklogFiltering(t *testing.T) {
	topic := &Topic{}
	for i, k := range []string{"user.create", "user.update", "order.new", "user.create", "user"} {
		topic.Messages = append(topic.Messages, Message{Key: []byte(k), Offset: uint32(i)})
	}

	offsets := func(start uint32, prefix string) []uint32 {
		var out []uint32
		for _, d := range topic.backlogLocked(start, prefix) {
			out = append(out, d.Offset)
		}
		return out
	}
	check := func(name string, got, want []uint32) {
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("%s: got %v, want %v", name, got, want)
		}
	}

	check("empty prefix, from 0", offsets(0, ""), []uint32{0, 1, 2, 3, 4})
	check("user.create, from 0", offsets(0, "user.create"), []uint32{0, 3})
	check("user, from 2", offsets(2, "user"), []uint32{3, 4})
	check("no match", offsets(0, "payment"), nil)
	check("start past end", offsets(10, ""), nil)
}
