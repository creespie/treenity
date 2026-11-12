package utils

type entry[V any] struct {
	key   string
	value V
	next  *entry[V]
}

type bucket[V any] struct {
	head *entry[V]
}

type HashMap[V any] struct {
	buckets []bucket[V]
	size    int64
}

type Item[V any] struct {
	Key   string
	Value V
}

func NewHashMap[V any]() *HashMap[V] {
	return &HashMap[V]{
		buckets: make([]bucket[V], 64),
		size:    0,
	}
}

func hash(key string) uint64 {
	h := uint64(5381)
	for i := 0; i < len(key); i++ {
		h = h*33 + uint64(key[i])
	}
	return h
}

func (m *HashMap[V]) index(key string) uint64 {
	return hash(key) % uint64(len(m.buckets))
}

func (m *HashMap[V]) Put(key string, value V) {
	i := m.index(key)

	for e := m.buckets[i].head; e != nil; e = e.next {
		if e.key == key {
			e.value = value
			return
		}
	}

	m.buckets[i].head = &entry[V]{key, value, m.buckets[i].head}
	m.size++

	if m.size*4 > int64(len(m.buckets))*3 {
		m.resize()
	}
}

func (m *HashMap[V]) resize() {
	oldBuckets := m.buckets
	m.buckets = make([]bucket[V], len(oldBuckets)*2)

	// reinserisco direttamente: le chiavi sono già uniche, size non cambia
	for _, b := range oldBuckets {
		for e := b.head; e != nil; e = e.next {
			i := m.index(e.key)
			m.buckets[i].head = &entry[V]{e.key, e.value, m.buckets[i].head}
		}
	}
}

func (m *HashMap[V]) Get(key string) (V, bool) {
	for e := m.buckets[m.index(key)].head; e != nil; e = e.next {
		if e.key == key {
			return e.value, true
		}
	}
	var zero V
	return zero, false
}

func (m *HashMap[V]) Has(key string) bool {
	_, ok := m.Get(key)
	return ok
}

func (m *HashMap[V]) Delete(key string) bool {
	i := m.index(key)
	var prev *entry[V]
	for e := m.buckets[i].head; e != nil; e = e.next {
		if e.key == key {
			if prev == nil {
				m.buckets[i].head = e.next // seera la prima del secchio
			} else {
				prev.next = e.next
			}
			m.size--
			return true
		}
		prev = e
	}
	return false
}

func (m *HashMap[V]) Len() int64 {
	return m.size
}

func (m *HashMap[V]) ForEach(f func(key string, value V)) {
	for _, b := range m.buckets {
		for e := b.head; e != nil; e = e.next {
			f(e.key, e.value)
		}
	}
}

func (m *HashMap[V]) GetItems() []Item[V] {
	items := make([]Item[V], 0, m.size)
	m.ForEach(func(k string, v V) {
		items = append(items, Item[V]{k, v})
	})
	return items
}

func (m *HashMap[V]) GetKeys() []string {
	keys := make([]string, 0, m.size)
	m.ForEach(func(k string, _ V) {
		keys = append(keys, k)
	})
	return keys
}

func (m *HashMap[V]) GetValues() []V {
	values := make([]V, 0, m.size)
	m.ForEach(func(_ string, v V) {
		values = append(values, v)
	})
	return values
}
