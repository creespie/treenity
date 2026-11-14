GO      := go
SERVER  := server
CLIENT  := client
SOURCES := $(shell find cmd internal -name '*.go' -not -name '*_test.go') go.mod

all: $(SERVER) $(CLIENT)

$(SERVER): $(SOURCES)
	$(GO) build -o $(SERVER) ./cmd/server

$(CLIENT): $(SOURCES)
	$(GO) build -o $(CLIENT) ./cmd/client

clean:
	rm -f $(SERVER) $(CLIENT)

re: clean all

test:
	$(GO) test -v ./...

.PHONY: all clean re test
