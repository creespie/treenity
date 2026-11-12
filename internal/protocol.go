package internal

type Request struct {
	Reply  string  `json:"reply,omitempty"`  
	Cmd    string  `json:"cmd"`              
	Topic  string  `json:"topic,omitempty"`  
	Name   string  `json:"name,omitempty"`   
	Key    []byte  `json:"key,omitempty"`    
	Value  []byte  `json:"value,omitempty"`  
	Prefix string  `json:"prefix,omitempty"` 
	Offset *uint32 `json:"offset,omitempty"` 
}

type Response struct {
	OK    bool   `json:"ok"`              
	Code  int    `json:"code,omitempty"`  
	Data  string `json:"data,omitempty"`  
	Error string `json:"error,omitempty"` 
}


const (
	CmdList   = "list"
	CmdCreate = "create"
	CmdInfo   = "info"
	CmdProduce = "produce"
	CmdSubscribe = "subscribe"
)