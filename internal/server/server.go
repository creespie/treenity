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

const bufferReadSize = 4096
const isDev = true

var clientNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,32}$`)

type Server struct {
	devMode bool
	path    string
	fifo    *os.File

	topics      *utils.HashMap[*Topic]
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
		devMode: isDev,
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
	s.fifo.Close()
	os.Remove(s.path)
}
