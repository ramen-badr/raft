package api

import (
	"net/http"

	"raft/raft"
)

// Status — ответ GET /status.
type Status struct {
	ID       int    `json:"id"`
	State    string `json:"state"`
	Term     int    `json:"term"`
	LeaderID int    `json:"leaderId"`
	Peers    []int  `json:"peers"`
	Alive    []int  `json:"alivePeers"`
}

// Mount монтирует клиентские маршруты на mux.
func Mount(mux *http.ServeMux, n *raft.Node) {
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		rep := n.Report()

		raft.WriteJSON(w, Status{
			ID:       rep.ID,
			State:    rep.State,
			Term:     rep.Term,
			LeaderID: rep.LeaderID,
			Peers:    rep.Peers,
			Alive:    rep.AlivePeers,
		})
	})
}
