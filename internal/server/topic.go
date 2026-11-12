package server

//serie di operazioni sulla logica dei tobox
import "errors"

func (s *Server) createTopic(name string) error {
	if !clientNameRegex.MatchString(name) {
		return errors.New("invalid topic name")
	}

	newTopic := new(Topic)
	newTopic.Name = name

	return nil
}
