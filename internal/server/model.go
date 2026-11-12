package server

// modello per le sturtture dati di passaggio
type Topic struct {
	Name     string
	Messages []Message
}

type ClientData struct {
	ClientID string
	Topic    string
	Offset   uint32
	Prefix   string
	IPCPath  string
	Active   bool
}

type Message struct {
	Key    []byte
	Value  []byte
	Offset uint32
}
