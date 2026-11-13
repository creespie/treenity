package server

import "treenity/internal/utils"

type prefixIndex struct {
	byPrefix *utils.HashMap[[]*consumer] //i consmer per prefix in lista
	direct   []*consumer                 //i consmer senza prefix che piano tutto
	all      []*consumer                 //una lista di entrabi i consumer per lo shutdown
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

// trova chi matcha quella chiave nei ocnumer, se la chiave é ciao
// matchiano tutto quelli che hanno come prefix
// c / ci / cia / ciao
func (p *prefixIndex) match(key []byte) []*consumer {
	out := append([]*consumer{}, p.direct...)

	for i := 1; i <= len(key); i++ {
		//tagli la hiave da user , u , us, usr,user per trovre i prfessi
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


//toglie elemento. da qualsiais lista gli si passa
func removeConsumer(list []*consumer, c *consumer) []*consumer {
	for i, other := range list {
		if other == c {
			return append(list[:i], list[i+1:]...)
		}
	}
	return list
}
