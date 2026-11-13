package server

import (
	"bytes"
	"fmt"
	"os"
)

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

			line := make([]byte, i)
			copy(line, pending[:i])
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
