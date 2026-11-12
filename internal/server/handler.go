package server

import (
	"encoding/json"
	"fmt"
	"os"

	"treenity/internal/shared/protocol"
)

func (s *Server) handleRequest(data []byte) {
	var req protocol.Request
	if err := json.Unmarshal(data, &req); err != nil {
		fmt.Fprintln(os.Stderr, "Invalid IPC request:", err)
		return
	}

	var resp protocol.Response

	switch req.Command {
	case protocol.CreateTopic:
		if err := s.createTopic(req.Topic); err != nil {
			resp = protocol.Failure(2, err.Error())
		} else {
			resp = protocol.Ok("topic created")
		}

	case protocol.ListTopics:
		// TODO

	case protocol.Produce:
		// TODO

	case protocol.RegisterClient:
		// TODO

	case protocol.GetInfo:
		// TODO

	case protocol.Ack:
		return

	case protocol.Disconnect:
		return

	default:
		fmt.Fprintln(os.Stderr, "Unknown command:", req.Command)
		resp = protocol.Failure(1, "unknown command")
	}

	sendResponse(req.ReplyPath, resp)
}
