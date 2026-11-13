package server

import (
	"sync"

	"treenity/internal"
)

type Topic struct {
	mu        sync.RWMutex
	Messages  []Message
	consumers *prefixIndex

	incoming chan Message
}

type ClientData struct {
	ClientID string
	Topic    string
	Offset   uint32
	Prefix   string
	IPCPath  string
	Active   bool
}

type Message struct {
	Key    []byte
	Value  []byte
	Offset uint32
}

func (m Message) delivery() internal.Delivery {
	return internal.Delivery{
		Offset: m.Offset,
		Key:    m.Key,
		Value:  m.Value,
	}
}
