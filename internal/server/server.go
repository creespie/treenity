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
	s.topicsMutex.RLock()
	topics := s.topics.GetValues()
	s.topicsMutex.RUnlock()

	//notifichiamo ogni consumer dei topic
	for _, t := range topics {
		t.mu.RLock()
		for _, c := range t.consumers.list() {
			c.send(internal.Delivery{Shutdown: true})
		}
		t.mu.RUnlock()
		//chiudiamo i chanell di incmoing alle go rouotine
		close(t.incoming)
	}

	finished := make(chan struct{})
	//qui facciamo partire wg wait
	//si sblocca solo se tutte le goroutine hanno finito
	//a quel punto fa cloase di finisce che il select sotto capta
	go func() {
		s.wg.Wait()
		close(finished)
	}()

	//questo é come uno switch ma quantistico
	//ossia osserva contemporaneamente i casi che sono channels
	//e il primo che manda un segnale sblocca il case
	//stato fatto perche per chiudere gracefully il server
	//il subject voleva che si aspettassero max 5 seoncdi che le goroutine
	//finissero il loro lavoro, quindi o finisco e il server si spegne
	//o passano 5 seocndi e si spegno comqunue

	select {
	case <-finished:
	case <-time.After(shutdownTimeout):
		fmt.Fprintln(os.Stderr, "shutdown timeout reached, exiting anyway")
	}
	//chiudimao il Fifo di ogni consumer e distruggiamolo pure
	for _, t := range topics {
		t.mu.RLock()
		for _, c := range t.consumers.list() {
			c.close()
		}
		t.mu.RUnlock()
	}

	s.fifo.Close()
	os.Remove(s.path)
}
