package server

//serie di operazioni sulla logica dei tobox
import (
	"errors"
	"strings"
)

func (s *Server) createTopic(name string) error {
	if !clientNameRegex.MatchString(name) {
		return errors.New("invalid topic name")
	}

	s.topicsMutex.Lock()
	defer s.topicsMutex.Unlock()

	if s.topics.Has(name) {
		return errors.New("topic already exists")
	}

	s.topics.Put(name, &Topic{Name: name})
	return nil
}

func (s *Server) listTopics() string {
	s.topicsMutex.RLock()
	defer s.topicsMutex.RUnlock()

	return strings.Join(s.topics.GetKeys(), ",")
}
