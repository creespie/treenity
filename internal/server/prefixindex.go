package server

import "treenity/internal/utils"

type prefixIndex struct {
	byPrefix *utils.HashMap[[]*consumer]
	direct   []*consumer
	all      []*consumer
}

func newPrefixIndex() *prefixIndex {
	return &prefixIndex{byPrefix: utils.NewHashMap[[]*consumer]()}
}

func (p *prefixIndex) add(c *consumer) {
	p.all = append(p.all, c)

	if c.prefix == "" {
		p.direct = append(p.direct, c)
		return
	}

	list, _ := p.byPrefix.Get(c.prefix)
	p.byPrefix.Put(c.prefix, append(list, c))
}

func (p *prefixIndex) remove(c *consumer) {
	p.all = removeConsumer(p.all, c)

	if c.prefix == "" {
		p.direct = removeConsumer(p.direct, c)
		return
	}

	list, ok := p.byPrefix.Get(c.prefix)
	if !ok {
		return
	}

	list = removeConsumer(list, c)
	if len(list) == 0 {
		p.byPrefix.Delete(c.prefix)
	} else {
		p.byPrefix.Put(c.prefix, list)
	}
}

func (p *prefixIndex) match(key []byte) []*consumer {
	out := make([]*consumer, 0, len(p.direct)+4)
	out = append(out, p.direct...)

	for i := 1; i <= len(key); i++ {
		if list, ok := p.byPrefix.Get(string(key[:i])); ok {
			out = append(out, list...)
		}
	}
	return out
}

func (p *prefixIndex) list() []*consumer {
	out := make([]*consumer, len(p.all))
	copy(out, p.all)
	return out
}

func (p *prefixIndex) find(clientID string) *consumer {
	for _, c := range p.all {
		if c.clientID == clientID {
			return c
		}
	}
	return nil
}

func removeConsumer(list []*consumer, c *consumer) []*consumer {
	for i, other := range list {
		if other == c {
			return append(list[:i], list[i+1:]...)
		}
	}
	return list
}
