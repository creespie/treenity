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
		resp = protocol.Ok(s.listTopics())

	case protocol.Produce:
		t, ok := s.topics.Get(req.Topic)
		if !ok {
			resp = protocol.Failure(2, "topic does not exist")
			break
		}
		if req.Key == nil && req.Value == nil {
			resp = protocol.Ok("")
			break
		}
		if len(req.Key)+len(req.Value) > 1024 {
			resp = protocol.Failure(1, "message too large")
			break
		}
		off := t.append(req.Key, req.Value)
		resp = protocol.Ok("")
		resp.Offset = off

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
