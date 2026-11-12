package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"syscall"
	"regexp"

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
	req.Reply =  replyFifoPath(serverPath)
	answer, err := request(serverFile, req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 3
	}

	if answer.OK{
		fmt.Println(answer.Data)
		return 0
	}
	fmt.Fprintln(os.Stderr, "error:", answer.Error)
	if answer.Code != 0{
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

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "errore: not enough arguments")
		os.Exit(1)
	}
	serverPath := os.Args[1]
	info, err := os.Stat(serverPath)
	if err != nil || info.Mode()&os.ModeNamedPipe == 0 {
		fmt.Fprintln(os.Stderr, "Not a valid server path:", err)
		os.Exit(1)
	}
	x, err := os.OpenFile(serverPath, os.O_WRONLY|syscall.O_NONBLOCK, 0)
	if errors.Is(err, syscall.ENXIO) {
		fmt.Fprintln(os.Stderr, "Server is offline:", err)
		os.Exit(1)
	}
	command := os.Args[2]
	pid := os.Getpid()
	path := fmt.Sprintf("/tmp/operator.client.%d", pid)
	err = syscall.Mkfifo(path, 0666)
	if err != nil {
		fmt.Fprintln(os.Stderr, "errore:", err)
		os.Exit(1)
	}
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		s := <-sigs
		fmt.Fprintln(os.Stderr, "ricevuto", s)
		os.Remove(path)
		os.Exit(0)
	}()

	switch command {
	case "create":
		os.Exit(doCreate(serverPath, os.Args[3:]))
	case "list":
		os.Exit(doList(serverPath, os.Args[3:]))
	case "produce":
		if len(os.Args) < 4 || len(os.Args) > 5 {
			fmt.Fprintln(os.Stderr, "Wrong produce format\nExpected: <ipc> produce <topic>\nWith optional flag --raw", err)
			os.Remove(path)
			os.Exit(1)
		}
		if len(os.Args) == 4 {
			doProduce()
		} else {
			if os.Args[4] != "--raw" {
				fmt.Fprintln(os.Stderr,
					"Wrong produce format\nExpected: <ipc> produce <topic>\nWith optional flag --raw", err)
				os.Remove(path)
				os.Exit(1)
			}
			doProduceRaw()
		}
	case "subscribe":
		doSubscribe()
	case "info":
		os.Exit(doInfo(serverPath, os.Args[3:]))
	default:
		fmt.Fprintln(os.Stderr, "Unrecognised command")
		os.Remove(path)
		os.Exit(1)
	}
	defer os.Remove(path)
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, "errore:", err)
		os.Exit(1)
	}
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		line := scan.Text()
		fmt.Printf("%s\n", line)
	}
}
