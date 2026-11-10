package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"strings"
)

func main() {
	pid := os.Getpid()

	path := fmt.Sprintf("/tmp/operator.server.%d", pid)
	err := syscall.Mkfifo(path, 0666)
	if err != nil {
		fmt.Fprintln(os.Stderr, "errore:", err)
		os.Exit(1)
	}
	defer os.Remove(path)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		s := <-sigs
		fmt.Fprintln(os.Stderr, "ricevuto", s)
		os.Remove(path)
		os.Exit(0)
	}()
	fmt.Println(path)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, "errore:", err)
		os.Exit(1)
	}
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		line := scan.Text()
		parts := strings.SplitN(line, " ", 2)
		client, err := os.OpenFile(parts[0], os.O_WRONLY, 0)
		if err != nil {
			fmt.Fprintln(os.Stderr, "errore:", err)
			os.Exit(1)
		}
		if parts[1] == "ping"{
			client.WriteString("pong\n")
		}else{
			client.WriteString("Thought we were playing ping pong\n")
		}
		fmt.Printf("%s \n", parts[1])
		client.Close()
	}
}
