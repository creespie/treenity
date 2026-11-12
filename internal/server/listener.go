package server

import (
	"bytes"
	"fmt"
	"os"
)

// legge le richieste de fFifo e quando ha una riga di richiesta pronta
// quidni che ha trovato un /n la manda all'ahanler per gestirla
func (s *Server) Serve() {
	buf := make([]byte, bufferReadSize)
	var pending []byte

	for {
		n, err := s.fifo.Read(buf)
		pending = append(pending, buf[:n]...)

		for {
			i := bytes.IndexByte(pending, '\n')
			if i < 0 {
				break
			}
			line := pending[:i]
			pending = pending[i+1:]
			s.handleRequest(line)
		}

		if len(pending) > 1024*1024 {
			fmt.Fprintln(os.Stderr, "Request too large, dropping buffer")
			pending = nil
		}

		if err != nil {
			break
		}
	}
}
