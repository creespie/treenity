package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	"treenity/internal"
)

const replyOpenTimeout = 2 * time.Second

func sendResponse(path string, response internal.Response) {
	if path == "" {
		return
	}

	data, err := json.Marshal(response)
	if err != nil {
		fmt.Fprintln(os.Stderr, "JSON Error:", err)
		return
	}

	f, err := openReply(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Reply FIFO Error:", err)
		return
	}
	defer f.Close()

	if _, err := f.Write(append(data, '\n')); err != nil {
		fmt.Fprintln(os.Stderr, "Reply Error:", err)
	}
}

func openReply(path string) (*os.File, error) {
	deadline := time.Now().Add(replyOpenTimeout)

	for {
		f, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0)
		if err == nil {
			return f, nil
		}

		if !errors.Is(err, syscall.ENXIO) || time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(2 * time.Millisecond)
	}
}
