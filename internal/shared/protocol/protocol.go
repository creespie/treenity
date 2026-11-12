// pchetto epr definire i dati di scambio tra client e server
package protocol

// comandi mandati dal client al server sinon e reuqest.
const (
	CreateTopic = iota
	ListTopics
	Produce
	RegisterClient
	GetInfo
	Ack
	Disconnect
)

type Request struct {
	Command   int    `json:"command"`
	ClientID  string `json:"client_id,omitempty"`
	Topic     string `json:"topic,omitempty"`
	IPCPath   string `json:"ipc_path,omitempty"`
	ReplyPath string `json:"reply_path,omitempty"`
	Prefix    string `json:"prefix,omitempty"`
	Offset    uint32 `json:"offset,omitempty"`
	HasOffset bool   `json:"has_offset,omitempty"`
	Raw       bool   `json:"raw,omitempty"`
	Key       []byte `json:"key,omitempty"`
	Value     []byte `json:"value,omitempty"`
}

// dati mandati srtver al client ossia le reponse
type Response struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Code    int    `json:"code"`
	Offset  uint32 `json:"offset"`
}

func Failure(code int, msg string) Response {
	return Response{Success: false, Message: msg, Code: code}
}

func Ok(msg string) Response {
	return Response{Success: true, Message: msg}
}
