package server

import (
	"encoding/json"
	"os"
	"sync"
	"syscall"

	"treenity/internal"
)

type consumer struct {
	clientID string
	prefix   string
	path     string

	fifo    *os.File
	writeMu sync.Mutex
}

func newConsumer(clientID, prefix, path string) (*consumer, error) {
	os.Remove(path)

	if err := syscall.Mkfifo(path, 0666); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		os.Remove(path)
		return nil, err
	}

	return &consumer{clientID: clientID, prefix: prefix, path: path, fifo: f}, nil
}

func (c *consumer) send(d internal.Delivery) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	c.sendLocked(d)
}

func (c *consumer) sendLocked(d internal.Delivery) {
	data, _ := json.Marshal(d)
	c.fifo.Write(append(data, '\n'))
}

func (c *consumer) sendBacklog(backlog []internal.Delivery) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	for _, d := range backlog {
		c.sendLocked(d)
	}
}

func (c *consumer) close() {
	c.fifo.Close()
	os.Remove(c.path)
}
