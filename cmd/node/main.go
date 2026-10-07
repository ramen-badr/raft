package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"raft/api"
	"raft/raft"
)

const readHeaderTimeout = 5 * time.Second

var errBadPeers = errors.New("неверный -peers")

func main() {
	id := flag.Int("id", 1, "ID этого узла")
	httpAddr := flag.String("http", "127.0.0.1:8001", "адрес HTTP-сервера узла")
	peersFlag := flag.String("peers", "", "все узлы кластера в формате id=host:port через запятую")

	flag.Parse()

	addrs, err := parsePeers(*peersFlag)
	if err != nil {
		log.Fatal(err)
	}

	if _, ok := addrs[*id]; !ok {
		log.Fatalf("узел %d отсутствует в -peers", *id)
	}

	peers := make([]int, 0, len(addrs)-1)

	for pid := range addrs {
		if pid != *id {
			peers = append(peers, pid)
		}
	}

	node := raft.NewNode(*id, peers, raft.NewHTTPTransport(addrs))

	mux := http.NewServeMux()
	node.Handler(mux)
	api.Mount(mux, node)

	go node.Run()

	log.Printf("[узел %d] слушаю %s, пиры: %v", *id, *httpAddr, peers)

	srv := &http.Server{
		Addr:              *httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
	}
	log.Fatal(srv.ListenAndServe())
}

// parsePeers разбирает строку вида "0=host:port,1=host:port".
func parsePeers(s string) (map[int]string, error) {
	addrs := map[int]string{}

	for item := range strings.SplitSeq(s, ",") {
		if strings.TrimSpace(item) == "" {
			continue
		}

		idStr, addr, ok := strings.Cut(item, "=")
		if !ok {
			return nil, fmt.Errorf("%w: ожидается id=host:port, получено %q", errBadPeers, item)
		}

		id, err := strconv.Atoi(idStr)
		if err != nil {
			return nil, fmt.Errorf("%w: неверный id в %q", errBadPeers, item)
		}

		addrs[id] = addr
	}

	if len(addrs) == 0 {
		return nil, fmt.Errorf("%w: список пуст", errBadPeers)
	}

	return addrs, nil
}
