// pachetto che dontiene la struttaura generlae del server come
// la lista di cliens di topcs ecc ecc inilizlizza anche il propio fifo
// per la ricezione e ha metodo per chiduelo bene
package server

import (
	"fmt"
	"os"
	"regexp"
	"sync"
	"syscall"

	"treenity/internal/utils"
)

const isDev = true

const bufferReadSize = 4096

var clientNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,32}$`)

type Server struct {
	path string
	fifo *os.File

	topics      map[string]*Topic
	topicsMutex sync.RWMutex

	clients *utils.HashMap[ClientData]
}

func New() (*Server, error) {
	path := fmt.Sprintf("/tmp/operator.server.%d", os.Getpid())

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
		topics:  make(map[string]*Topic),
		clients: utils.NewHashMap[ClientData](),
	}, nil
}

func (s *Server) Path() string {
	return s.path
}

func (s *Server) Close() {
	s.fifo.Close()
	os.Remove(s.path)
}
