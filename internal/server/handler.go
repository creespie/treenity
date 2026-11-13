package server

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"treenity/internal"
)

const cmdDisconnect = "disconnect"

type clientInfo struct {
	Client string `json:"client"`
	Topic  string `json:"topic"`
	Offset uint32 `json:"offset"`
	Prefix string `json:"prefix"`
	IPC    string `json:"ipc"`
}

func ok(data string) internal.Response {
	return internal.Response{OK: true, Data: data}
}

func fail(code int, msg string) internal.Response {
	return internal.Response{OK: false, Code: code, Error: msg}
}

func (s *Server) handleRequest(data []byte) {
	var req internal.Request
	if err := json.Unmarshal(data, &req); err != nil {
		fmt.Fprintln(os.Stderr, "Invalid IPC request:", err)
		return
	}

	var resp internal.Response

	switch req.Cmd {
	case internal.CmdCreate:
		if !nameRegex.MatchString(req.Topic) {
			resp = fail(1, "invalid topic name")
			break
		}

		s.topicsMutex.Lock()
		if s.topics.Has(req.Topic) {
			s.topicsMutex.Unlock()
			resp = fail(2, "topic already exists")
			break
		}

		t := &Topic{
			consumers: newPrefixIndex(),
			incoming:  make(chan Message, 64),
		}
		s.topics.Put(req.Topic, t)
		s.topicOrder = append(s.topicOrder, req.Topic)
		s.topicsMutex.Unlock()

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			for msg := range t.incoming {
				t.mu.Lock()
				msg.Offset = uint32(len(t.Messages))
				t.Messages = append(t.Messages, msg)
				targets := t.consumers.match(msg.Key)
				t.mu.Unlock()

				for _, c := range targets {
					c.send(msg.delivery())
				}
			}
		}()

		resp = ok("topic created")

	case internal.CmdList:

		s.topicsMutex.RLock()
		names := make([]string, len(s.topicOrder))
		copy(names, s.topicOrder)
		s.topicsMutex.RUnlock()

		resp = ok(strings.Join(names, ","))

	case internal.CmdProduce: //vede se ce un topic
		if _, found := s.getTopic(req.Topic); !found {
			resp = fail(2, "topic does not exist")
			break
		}
		resp = ok("")

	case internal.CmdMsg:
		t, found := s.getTopic(req.Topic)
		if !found {
			fmt.Fprintln(os.Stderr, "produce on unknown topic:", req.Topic)
			return
		}

		t.incoming <- Message{Key: req.Key, Value: req.Value}
		return

	case internal.CmdSubscribe:

		if !nameRegex.MatchString(req.Name) {
			resp = fail(1, "invalid client name")
			break
		}

		t, found := s.getTopic(req.Topic)
		if !found {
			resp = fail(2, "topic does not exist")
			break
		}

		s.clientsMutex.Lock()
		old, known := s.clients.Get(req.Name)
		if known && old.Active {
			s.clientsMutex.Unlock()
			resp = fail(2, "client already subscribed")
			break
		}

		var start uint32
		switch {
		case req.Offset != nil:
			start = *req.Offset
		case known && old.Topic == req.Topic:
			start = old.Offset
		}

		path := s.consumerPath(req.Name)
		c, err := newConsumer(req.Name, req.Prefix, path)
		if err != nil {
			s.clientsMutex.Unlock()
			fmt.Fprintln(os.Stderr, "consumer fifo:", err)
			resp = fail(3, "cannot create consumer channel")
			break
		}

		s.clients.Put(req.Name, ClientData{
			ClientID: req.Name,
			Topic:    req.Topic,
			Offset:   start,
			Prefix:   req.Prefix,
			IPCPath:  path,
			Active:   true,
		})
		s.clientsMutex.Unlock()

		t.mu.Lock()
		backlog := t.backlogLocked(start, req.Prefix)
		t.consumers.add(c)
		t.mu.Unlock()

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			c.sendBacklog(backlog)
		}()

		resp = ok("subscribed to " + req.Topic)

	case internal.CmdInfo:
		s.clientsMutex.RLock()
		cd, found := s.clients.Get(req.Name)
		s.clientsMutex.RUnlock()

		if !found {
			resp = fail(2, "client not found")
			break
		}

		info, _ := json.Marshal(clientInfo{
			Client: cd.ClientID,
			Topic:  cd.Topic,
			Offset: cd.Offset,
			Prefix: cd.Prefix,
			IPC:    cd.IPCPath,
		})
		resp = ok(string(info))

	case internal.CmdCommit:

		if req.Offset == nil {
			return
		}
		s.clientsMutex.Lock()
		if cd, found := s.clients.Get(req.Name); found {
			if *req.Offset > cd.Offset {
				cd.Offset = *req.Offset
				s.clients.Put(req.Name, cd)
			}
		}
		s.clientsMutex.Unlock()
		return

	case cmdDisconnect:
		s.clientsMutex.Lock()
		cd, found := s.clients.Get(req.Name)
		if found {
			cd.Active = false
			s.clients.Put(req.Name, cd)
		}
		s.clientsMutex.Unlock()

		if !found {
			return
		}
		if t, exists := s.getTopic(cd.Topic); exists {
			t.mu.Lock()
			c := t.consumers.find(req.Name)
			if c != nil {
				t.consumers.remove(c)
			}
			t.mu.Unlock()

			if c != nil {
				c.close()
			}
		}
		return

	default:
		fmt.Fprintln(os.Stderr, "Unknown command:", req.Cmd)
		resp = fail(1, "unknown command")
	}

	sendResponse(req.Reply, resp)
}

func (s *Server) getTopic(name string) (*Topic, bool) {
	s.topicsMutex.RLock()
	defer s.topicsMutex.RUnlock()

	return s.topics.Get(name)
}

func (s *Server) consumerPath(name string) string {
	//non serve mandarla al client perche esso se la ricrea con lo standard
	return s.path + "." + name
}

func (t *Topic) backlogLocked(start uint32, prefix string) []internal.Delivery {
	var out []internal.Delivery

	for _, m := range t.Messages {
		if m.Offset < start {
			continue
		}
		if prefix != "" && !strings.HasPrefix(string(m.Key), prefix) {
			continue
		}
		out = append(out, m.delivery())
	}
	return out
}
