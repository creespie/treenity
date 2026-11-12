package server

//parte che si occupa diinootlre le risposte al fifo de lciebnts
import (
	"encoding/json"
	"fmt"
	"os"

	"treenity/internal/shared/protocol"
)

func sendResponse(path string, response protocol.Response) {
	if path == "" {
		return
	}

	data, err := json.Marshal(response)
	if err != nil {
		fmt.Fprintln(os.Stderr, "JSON Error:", err)
		return
	}

	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Reply FIFO Error:", err)
		return
	}
	defer f.Close()

	if _, err := f.Write(append(data, '\n')); err != nil {
		fmt.Fprintln(os.Stderr, "Reply Error:", err)
	}
}
