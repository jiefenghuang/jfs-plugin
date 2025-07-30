BIN_DIR := bin
SRC := $(wildcard *.go) $(wildcard */*.go)  
LDFLAGS := -s -w

ifdef STATIC
	LDFLAGS += -linkmode external -extldflags '-static'
	export CC := /usr/bin/musl-gcc
endif

.PHONY: all clean plugin

all: plugin

$(BIN_DIR):
	@mkdir -p $@

$(BIN_DIR)/plugin: $(SRC) go.mod go.sum | $(BIN_DIR)
	go build -ldflags="$(LDFLAGS)" -o $@ .

plugin: $(BIN_DIR)/plugin

clean:
	rm -rf $(BIN_DIR)

go.mod go.sum:
	go mod tidy