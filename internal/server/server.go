package server

import (
	"fmt"
	"os"
	"regexp"
	"sync"
	"syscall"
	"time"

	"treenity/internal"
	"treenity/internal/utils"
)

const bufferReadSize = 4096

const shutdownTimeout = 5 * time.Second

var nameRegex = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,32}$`)

type Server struct {
	path string
	fifo *os.File

	topics      *utils.HashMap[*Topic]
	topicOrder  []string
	topicsMutex sync.RWMutex
	closing     bool // impostato da shutdown(): niente più messaggi nei topic

	clients      *utils.HashMap[ClientData]
	clientsMutex sync.RWMutex

	wg        sync.WaitGroup
	closeOnce sync.Once
}

func New() (*Server, error) {
	path := fmt.Sprintf("/tmp/operator.server.%d", os.Getpid())

	os.Remove(path)

	if err := syscall.Mkfifo(path, 0666); err != nil {
		return nil, err
	}

	fifo, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		os.Remove(path)
		return nil, err
	}

	return &Server{
		path:    path,
		fifo:    fifo,
		topics:  utils.NewHashMap[*Topic](),
		clients: utils.NewHashMap[ClientData](),
	}, nil
}

func (s *Server) Path() string {
	return s.path
}

func (s *Server) Close() {
	s.closeOnce.Do(s.shutdown)
}

func (s *Server) shutdown() {
	// 1. Da qui nessun CmdMsg può più mandare su t.incoming: prendere il
	//    lock in SCRITTURA aspetta anche che finiscano gli invii in corso.
	s.topicsMutex.Lock()
	s.closing = true
	topics := s.topics.GetValues()
	s.topicsMutex.Unlock()

	// 2. Avvisiamo ogni consumer (in coda, dopo i messaggi già pendenti)
	//    e chiudiamo i channel dei topic: ora è sicuro.
	var consumers []*consumer
	for _, t := range topics {
		t.mu.RLock()
		list := t.consumers.list()
		t.mu.RUnlock()
		for _, c := range list {
			c.enqueue(internal.Delivery{Shutdown: true})
		}
		consumers = append(consumers, list...)
		close(t.incoming)
	}

	// 3. Aspettiamo che le goroutine finiscano, al massimo 5 secondi.
	//    Intanto Serve() continua a ricevere i commit dei client.
	finished := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(shutdownTimeout):
		fmt.Fprintln(os.Stderr, "shutdown timeout reached, exiting anyway")
	}

	// 4. Chi non ha finito (client bloccato) viene fermato: chiudere la
	//    FIFO sblocca la Write. Poi via tutte le FIFO.
	for _, c := range consumers {
		c.stop()
		os.Remove(c.path)
	}
	s.fifo.Close()
	os.Remove(s.path)
}