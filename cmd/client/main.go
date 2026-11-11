package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
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

func request(server *os.File, serverPath string, msg string) (string, error) {
	defer server.Close()

	replyPath := replyFifoPath(serverPath)
	if err := syscall.Mkfifo(replyPath, 0600); err != nil {
		return "", err
	}
	defer os.Remove(replyPath)

	if _, err := fmt.Fprintf(server, "%s %s\n", replyPath, msg); err != nil {
		return "", err
	}

	reply, err := os.OpenFile(replyPath, os.O_RDONLY, 0)
	if err != nil {
		return "", err
	}
	defer reply.Close()

	scan := bufio.NewScanner(reply)
	if !scan.Scan() {
		if err := scan.Err(); err != nil {
			return "", err
		}
		return "", errors.New("server closed the connection without replying")
	}
	return scan.Text(), nil
}

func doList(serverPath string, args []string) int {
	if len(args) != 0 {
		fmt.Fprint(os.Stderr,
			"Wrong list format\nExpected: <ipc> list\n")
		return 1
	}
	serverFile, err := connectServer(serverPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}
	answer, err := request(serverFile, serverPath, "list")
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 3
	}
	parts := strings.SplitN(answer, " ", 2)
	switch parts[0] {
	case "ERR":
		if len(parts) != 1 {
			fmt.Fprintln(os.Stderr, "error:", parts[1])
		} else {
			fmt.Fprintln(os.Stderr, "Error in the retrieving of the data")
		}
		return 2
	case "OK":
		if len(parts) != 1 {
			fmt.Println(parts[1])
		} else {
			fmt.Println("")
		}
	default:
		fmt.Fprintln(os.Stderr, "Unexpected format in retrieved data")
		return 3
	}
	return 0
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
		if len(os.Args) != 4 || !reg.MatchString(os.Args[3]) {
			fmt.Fprintln(os.Stderr,
				"Wrong create format\nExpected: <ipc> create <topic>\nTopic must respect the regex ^[a-zA-Z0-9_.-]{1,32}$ \n", err)
			os.Remove(path)
			os.Exit(1)
		}
		doCreate()
	case "list":
		if len(os.Args) != 3 {
			fmt.Fprintln(os.Stderr,
				"Wrong list format\nExpected: <ipc> list\n", err)
			os.Remove(path)
			os.Exit(1)
		}
		doList()
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
		if len(os.Args) != 4 {
			fmt.Fprintln(os.Stderr, "Wrong info format\nExpected: <ipc> info <name>\n", err)
			os.Remove(path)
			os.Exit(1)
		}
		doInfo()
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
