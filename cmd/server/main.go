package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"treenity/internal/server"
)

func main() {
	srv, err := server.New()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Server Error:", err)
		os.Exit(1)
	}
	defer srv.Close()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigs)

	fmt.Println(srv.Path())

	go func() {
		<-sigs
		srv.Close()
	}()

	srv.Serve()
}
