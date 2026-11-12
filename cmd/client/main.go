package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
 
	"treenity/internal"
)
 
var reg = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,32}$`)

func replyFifoPath(serverPath string) string {
	return fmt.Sprintf("%s.@%d", serverPath, os.Getpid())
}

func connectServer(serverPath string) (*os.File, error) {
	info, err := os.Stat(serverPath)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeNamedPipe == 0 {
		return nil, fmt.Errorf("%s is not a FIFO", serverPath)
	}

	server, err := os.OpenFile(serverPath, os.O_WRONLY|syscall.O_NONBLOCK, 0)
	if errors.Is(err, syscall.ENXIO) {
		return nil, fmt.Errorf("no server is listening on %s", serverPath)
	}
	if err != nil {
		return nil, err
	}
	return server, nil
}

func request(server *os.File, req internal.Request) (internal.Response, error) {
	defer server.Close()

	if err := syscall.Mkfifo(req.Reply, 0600); err != nil {
		return internal.Response{}, err
	}
	defer os.Remove(req.Reply)

	toSend, err := json.Marshal(req)
	if err != nil {
		return internal.Response{}, err
	}
	if _, err := fmt.Fprintf(server, "%s\n", toSend); err != nil {
		return internal.Response{}, err
	}

	reply, err := os.OpenFile(req.Reply, os.O_RDONLY, 0)
	if err != nil {
		return internal.Response{}, err
	}
	defer reply.Close()

	scan := bufio.NewScanner(reply)
	if !scan.Scan() {
		if err := scan.Err(); err != nil {
			return internal.Response{}, err
		}
		return internal.Response{}, errors.New("server closed the connection without replying")
	}
	var ans internal.Response
	if err := json.Unmarshal(scan.Bytes(), &ans); err != nil {
		return internal.Response{}, err
	}
	return ans, nil
}

func runRequest(serverPath string, req internal.Request) int {
	serverFile, err := connectServer(serverPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}
	req.Reply = replyFifoPath(serverPath)
	answer, err := request(serverFile, req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 3
	}

	if answer.OK {
		fmt.Println(answer.Data)
		return 0
	}
	fmt.Fprintln(os.Stderr, "error:", answer.Error)
	if answer.Code != 0 {
		return answer.Code
	}
	return 3
}

func doList(serverPath string, args []string) int {
	if len(args) != 0 {
		fmt.Fprintln(os.Stderr, "usage: <ipc> list")
		return 1
	}
	req := internal.Request{Cmd: internal.CmdList}
	return runRequest(serverPath, req)
}

func doCreate(serverPath string, args []string) int {
	if len(args) != 1 || !reg.MatchString(args[0]) {
		fmt.Fprintln(os.Stderr, "usage: <ipc> create <topic>  (topic: ^[a-zA-Z0-9_.-]{1,32}$)")
		return 1
	}
	req := internal.Request{Cmd: internal.CmdCreate, Topic: args[0]}
	return runRequest(serverPath, req)
}

func doInfo(serverPath string, args []string) int {
	if len(args) != 1 || !reg.MatchString(args[0]) {
		fmt.Fprintln(os.Stderr, "usage: <ipc> info <name>  (name: ^[a-zA-Z0-9_.-]{1,32}$)")
		return 1
	}
	req := internal.Request{Cmd: internal.CmdInfo, Name: args[0]}
	return runRequest(serverPath, req)
}

func readTextMessage(scan *bufio.Scanner) ([]byte, []byte, error) {
	if !scan.Scan() {
		if err := scan.Err(); err != nil {
			return nil, nil, err
		}
		return nil, nil, io.EOF
	}
	line := scan.Text()
	key, value, found := strings.Cut(line, ":")
	if !found {
		return nil, nil, fmt.Errorf("expected key:value, got %q", line)
	}
	return []byte(key), []byte(value), nil
}

// Legge un messaggio in formato --raw:
// [keysize:uint32 LE][key][valuesize:uint32 LE][value]
// Restituisce io.EOF solo se lo stdin finisce PRIMA di un nuovo record.
func readRawMessage(r io.Reader) ([]byte, []byte, error) {
	size := make([]byte, 4)

	if _, err := io.ReadFull(r, size); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil, io.EOF
		}
		return nil, nil, errors.New("truncated record (key size)")
	}
	keySize := binary.LittleEndian.Uint32(size)
	if keySize > maxMessageSize {
		return nil, nil, fmt.Errorf("key too long (%d bytes)", keySize)
	}

	key := make([]byte, keySize)
	if _, err := io.ReadFull(r, key); err != nil {
		return nil, nil, errors.New("truncated record (key)")
	}

	if _, err := io.ReadFull(r, size); err != nil {
		return nil, nil, errors.New("truncated record (value size)")
	}
	valueSize := binary.LittleEndian.Uint32(size)
	if uint64(keySize)+uint64(valueSize) > maxMessageSize {
		return nil, nil, fmt.Errorf("message too long (%d bytes)", uint64(keySize)+uint64(valueSize))
	}

	value := make([]byte, valueSize)
	if _, err := io.ReadFull(r, value); err != nil {
		return nil, nil, errors.New("truncated record (value)")
	}
	return key, value, nil
}

const maxMessageSize = 1024

func doProduce(serverPath string, args []string) int {
	if len(args) < 1 || len(args) > 2 || !reg.MatchString(args[0]) || (len(args) == 2 && args[1] != "--raw") {
		fmt.Fprintln(os.Stderr, "usage: <ipc_identifier> produce <topic_name> [--raw]  (topic_name: ^[a-zA-Z0-9_.-]{1,32}$)")
		return 1
	}
	topic := args[0]
	raw := len(args) == 2

	serverFile, err := connectServer(serverPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}
	req := internal.Request{Cmd: internal.CmdProduce, Topic: topic, Reply: replyFifoPath(serverPath)}
	resp, err := request(serverFile, req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 3
	}
	if !resp.OK {
		fmt.Fprintln(os.Stderr, "error:", resp.Error)
		if resp.Code != 0 {
			return resp.Code
		}
		return 3
	}

	serverFile, err = connectServer(serverPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 3
	}
	defer serverFile.Close()

	stdin := bufio.NewReader(os.Stdin)
	scan := bufio.NewScanner(stdin)
	for {
		var key, value []byte
		if raw {
			key, value, err = readRawMessage(stdin)
		} else {
			key, value, err = readTextMessage(scan)
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			return 1
		}
		if len(key)+len(value) > maxMessageSize {
			fmt.Fprintln(os.Stderr, "error: message too long, max", maxMessageSize, "bytes")
			return 1
		}

		msg := internal.Request{Cmd: internal.CmdMsg, Topic: topic, Key: key, Value: value}
		toSend, err := json.Marshal(msg)
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			return 1
		}
		if _, err := fmt.Fprintf(serverFile, "%s\n", toSend); err != nil {
			fmt.Fprintln(os.Stderr, "error: server disconnected:", err)
			return 3
		}
	}
	return 0
}

func encodeRawDelivery(d internal.Delivery) []byte {
	buf := make([]byte, 0, 12+len(d.Key)+len(d.Value))
	num := make([]byte, 4)

	binary.LittleEndian.PutUint32(num, d.Offset)
	buf = append(buf, num...)

	binary.LittleEndian.PutUint32(num, uint32(len(d.Key)))
	buf = append(buf, num...)
	buf = append(buf, d.Key...)

	binary.LittleEndian.PutUint32(num, uint32(len(d.Value)))
	buf = append(buf, num...)
	buf = append(buf, d.Value...)

	return buf
}

func doSubscribe(serverPath string, args []string) int {
	if len(args) < 2 || len(args) > 7 || !reg.MatchString(args[0]) || !reg.MatchString(args[1]) {
		fmt.Fprintln(os.Stderr, "usage: <ipc_identifier> subscribe <topic_name> <subscriber_name> [--prefix <prefix>] [--offset <offset>] [--raw]")
		return 1
	}
	var raw, prefixFound, offsetFound bool
	var prefix string
	var offset *uint32
	for i := 2; i < len(args); i++ {
		if args[i] == "--raw" {
			if raw {
				fmt.Fprintln(os.Stderr, "Max one raw flag") // ← CAMBIATO: stderr invece di stdout
				return 1
			}
			raw = true
		} else if args[i] == "--prefix" {
			if prefixFound || i+1 == len(args) {
				fmt.Fprintln(os.Stderr, "Max one prefix flag, and needs a following argument")
				return 1
			}
			prefixFound = true
			if args[i+1] == "--raw" || args[i+1] == "--prefix" || args[i+1] == "--offset" {
				fmt.Fprintln(os.Stderr, "expected argument after flag")
				return 1
			}
			prefix = args[i+1]
			i++
		} else if args[i] == "--offset" {
			if offsetFound || i+1 == len(args) {
				fmt.Fprintln(os.Stderr, "Max one offset flag, and needs a following argument")
				return 1
			}
			offsetFound = true
			if args[i+1] == "--raw" || args[i+1] == "--prefix" || args[i+1] == "--offset" {
				fmt.Fprintln(os.Stderr, "expected argument after flag")
				return 1
			}
			n, err := strconv.ParseUint(args[i+1], 10, 32)
			if err != nil {
				fmt.Fprintln(os.Stderr, "invalid offset number")
				return 1
			}
			off := uint32(n)
			offset = &off
			i++
		} else {
			fmt.Fprintln(os.Stderr, "Unrecognised argument or flag")
			return 1
		}
	}
	topic := args[0]
	name := args[1]

	// --- registrazione presso il server ---
	serverFile, err := connectServer(serverPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}
	req := internal.Request{
		Cmd:    internal.CmdSubscribe,
		Topic:  topic,
		Name:   name,
		Prefix: prefix,
		Offset: offset,
		Reply:  replyFifoPath(serverPath),
	}
	resp, err := request(serverFile, req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 3
	}
	if !resp.OK {
		fmt.Fprintln(os.Stderr, "error:", resp.Error)
		if resp.Code != 0 {
			return resp.Code
		}
		return 3
	}

	// --- connessione verso il server, per i commit (resta aperta) ---
	serverFile, err = connectServer(serverPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 3
	}
	defer serverFile.Close()

	// La FIFO del subscriber l'ha creata il server.
	// La apriamo in SOLA LETTURA: così, se il server muore, Scan() riceve EOF.
	subPath := serverPath + "." + name
	subFile, err := os.OpenFile(subPath, os.O_RDONLY, 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 3
	}
	defer subFile.Close()

	fmt.Println("subscribed to", topic)

	signal.Reset(syscall.SIGINT, syscall.SIGTERM) // toglie la gestione generica del main
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM) // da qui i segnali arrivano a noi
	interrupted := make(chan struct{})
	go func() {
		<-stop
		close(interrupted) // "è arrivato un segnale" (lo controlliamo dopo il ciclo)
		subFile.Close()    // sblocca scan.Scan(), che è fermo in lettura
	}()
	scan := bufio.NewScanner(subFile)
	for scan.Scan() {
		var d internal.Delivery
		if err := json.Unmarshal(scan.Bytes(), &d); err != nil {
			fmt.Fprintln(os.Stderr, "error: invalid message from server:", err)
			return 3
		}
		if d.Shutdown {
			return 0
		}
		if raw {
			if _, err := os.Stdout.Write(encodeRawDelivery(d)); err != nil {
				fmt.Fprintln(os.Stderr, "Unable to uncode raw messag from server")
				return 1
			}
		} else {
			fmt.Printf("%s:%s\n", d.Key, d.Value)
		}
		next := d.Offset + 1
		commit := internal.Request{Cmd: internal.CmdCommit, Name: name, Offset: &next}
		toSend, err := json.Marshal(commit)
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			return 1
		}
		if _, err := fmt.Fprintf(serverFile, "%s\n", toSend); err != nil {
			fmt.Fprintln(os.Stderr, "error: server disconnected:", err)
			return 3
		}
	}
	select {
	case <-interrupted:
		bye := internal.Request{Cmd: internal.CmdDisconnect, Name: name}
		if toSend, err := json.Marshal(bye); err == nil {
			fmt.Fprintf(serverFile, "%s\n", toSend)
		}
		return 0
	default:
	}

	if err := scan.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
	} else {
		fmt.Fprintln(os.Stderr, "error: server disconnected")
	}
	return 3
}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: ./client <ipc> <create|list|produce|subscribe|info> [args...]")
		os.Exit(1)
	}
	serverPath := os.Args[1]
	command := os.Args[2]
	args := os.Args[3:]

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		os.Remove(replyFifoPath(serverPath))
		os.Exit(0)
	}()

	switch command {
	case "list":
		os.Exit(doList(serverPath, args))
	case "create":
		os.Exit(doCreate(serverPath, args))
	case "info":
		os.Exit(doInfo(serverPath, args))
	case "produce":
		os.Exit(doProduce(serverPath, args))
	case "subscribe":
		os.Exit(doSubscribe(serverPath, args))
	default:
		fmt.Fprintln(os.Stderr, "unknown command:", command)
		os.Exit(1)
	}
}
