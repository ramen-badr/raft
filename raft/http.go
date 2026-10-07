package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"
)

const rpcTimeout = 500 * time.Millisecond

var (
	errUnknownPeer = errors.New("неизвестный узел")
	errBadStatus   = errors.New("неожиданный HTTP-статус")
)

// HTTPTransport реализует Transport поверх HTTP+JSON: /raft/request-vote, /raft/append-entries.
type HTTPTransport struct {
	addrs  map[int]string
	client *http.Client
}

// NewHTTPTransport создает транспорт; addrs — адреса всех узлов кластера по ID.
func NewHTTPTransport(addrs map[int]string) *HTTPTransport {
	return &HTTPTransport{
		addrs:  addrs,
		client: &http.Client{Timeout: rpcTimeout},
	}
}

// RequestVote отправляет RPC RequestVote узлу peer.
func (t *HTTPTransport) RequestVote(peer int, args RequestVoteArgs) (RequestVoteReply, error) {
	var reply RequestVoteReply

	return reply, t.post(peer, "/raft/request-vote", args, &reply)
}

// AppendEntries отправляет RPC AppendEntries узлу peer.
func (t *HTTPTransport) AppendEntries(peer int, args AppendEntriesArgs) (AppendEntriesReply, error) {
	var reply AppendEntriesReply

	return reply, t.post(peer, "/raft/append-entries", args, &reply)
}

func (t *HTTPTransport) post(peer int, path string, args, reply any) error {
	addr, ok := t.addrs[peer]
	if !ok {
		return fmt.Errorf("%w: %d", errUnknownPeer, peer)
	}

	body, err := json.Marshal(args)
	if err != nil {
		return fmt.Errorf("кодирование запроса: %w", err)
	}

	req, err := http.NewRequestWithContext(
		context.Background(), http.MethodPost, "http://"+addr+path, bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("создание запроса: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("узел %d: %w", peer, err)
	}
	defer closeBody(resp)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("узел %d: %w %d", peer, errBadStatus, resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(reply); err != nil {
		return fmt.Errorf("узел %d: декодирование ответа: %w", peer, err)
	}

	return nil
}

func closeBody(resp *http.Response) {
	if err := resp.Body.Close(); err != nil {
		log.Printf("закрытие тела ответа: %v", err)
	}
}

// Handler монтирует HTTP-маршруты Raft на mux.
func (n *Node) Handler(mux *http.ServeMux) {
	mux.HandleFunc("POST /raft/request-vote", handle(n.RequestVote))
	mux.HandleFunc("POST /raft/append-entries", handle(n.AppendEntries))
}

func handle[Args, Reply any](f func(Args) Reply) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var args Args
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		WriteJSON(w, f(args))
	}
}

// WriteJSON отвечает клиенту значением v в формате JSON.
func WriteJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("запись ответа: %v", err)
	}
}
