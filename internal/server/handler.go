package server

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"treenity/internal"
)

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
					c.enqueue(msg.delivery()) // non blocca mai
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
		// Teniamo il lock in lettura mentre inviamo: shutdown() prende il
		// lock in scrittura prima di chiudere i channel, quindi non può
		// chiuderli mentre siamo qui (niente "send on closed channel").
		s.topicsMutex.RLock()
		if s.closing {
			s.topicsMutex.RUnlock()
			return
		}
		t, found := s.topics.Get(req.Topic)
		if found {
			t.incoming <- Message{Key: req.Key, Value: req.Value}
		}
		s.topicsMutex.RUnlock()
		if !found {
			fmt.Fprintln(os.Stderr, "produce on unknown topic:", req.Topic)
		}
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
			// Attivo secondo noi, ma è ancora vivo? (es. ucciso con kill -9)
			if readerAlive(old.IPCPath) {
				s.clientsMutex.Unlock()
				resp = fail(2, "client already subscribed")
				break
			}
			s.dropConsumer(old.Topic, req.Name) // era morto: liberiamo il nome
		}

		var start uint32
		switch {
		case req.Offset != nil:
			start = *req.Offset
		case known && old.Topic == req.Topic:
			start = old.Offset
		}

		path := s.consumerPath(req.Name)

		// Sotto t.mu: backlog e aggiunta all'indice insieme, così nessun
		// messaggio va perso o arriva due volte, e il backlog sta in coda
		// PRIMA di qualsiasi messaggio nuovo (ordine per offset garantito).
		t.mu.Lock()
		backlog := t.backlogLocked(start, req.Prefix)
		c, err := newConsumer(req.Name, req.Topic, req.Prefix, path, start, backlog)
		if err != nil {
			t.mu.Unlock()
			s.clientsMutex.Unlock()
			fmt.Fprintln(os.Stderr, "consumer fifo:", err)
			resp = fail(3, "cannot create consumer channel")
			break
		}
		t.consumers.add(c)
		t.mu.Unlock()

		s.clients.Put(req.Name, ClientData{
			ClientID: req.Name,
			Topic:    req.Topic,
			Offset:   start,
			Prefix:   req.Prefix,
			IPCPath:  path,
			Active:   true,
		})
		s.clientsMutex.Unlock()

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			c.run(s.consumerDead)
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

	case internal.CmdDisconnect:
		s.clientsMutex.Lock()
		cd, found := s.clients.Get(req.Name)
		if found && cd.Active {
			cd.Active = false
			s.clients.Put(req.Name, cd)
			s.dropConsumer(cd.Topic, req.Name)
		}
		s.clientsMutex.Unlock()
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

// Toglie il consumer dall'indice del topic, lo ferma e aspetta che la
// sua goroutine abbia finito (e cancellato la sua FIFO).
func (s *Server) dropConsumer(topic, name string) {
	t, exists := s.getTopic(topic)
	if !exists {
		return
	}
	t.mu.Lock()
	c := t.consumers.find(name)
	if c != nil {
		t.consumers.remove(c)
	}
	t.mu.Unlock()
	if c != nil {
		c.stopAndWait(time.Second)
	}
}

// Chiamata dalla goroutine di un consumer quando scopre che il client è
// morto senza disconnettersi. Lo segna come non attivo, così il nome torna
// libero e il segnalibro resta salvato.
func (s *Server) consumerDead(c *consumer) {
	fmt.Fprintln(os.Stderr, "consumer gone:", c.clientID)

	s.clientsMutex.Lock()
	defer s.clientsMutex.Unlock()

	t, exists := s.getTopic(c.topic)
	if !exists {
		return
	}
	t.mu.Lock()
	current := t.consumers.find(c.clientID) == c // è ancora lui, non uno nuovo con lo stesso nome?
	if current {
		t.consumers.remove(c)
	}
	t.mu.Unlock()

	if current {
		if cd, found := s.clients.Get(c.clientID); found {
			cd.Active = false
			s.clients.Put(c.clientID, cd)
		}
	}
}