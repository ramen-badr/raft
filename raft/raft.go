package raft

import (
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"time"
)

const (
	heartbeatInterval  = 100 * time.Millisecond
	minElectionTimeout = 500 * time.Millisecond
	maxElectionTimeout = 1 * time.Second
	tickInterval       = 10 * time.Millisecond
)

// State — роль узла в кластере.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	return [...]string{"Follower", "Candidate", "Leader"}[s]
}

// RequestVoteArgs — аргументы RPC RequestVote.
type RequestVoteArgs struct {
	Term        int `json:"term"`
	CandidateID int `json:"candidateId"`
}

// RequestVoteReply — ответ RPC RequestVote.
type RequestVoteReply struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"voteGranted"`
}

// AppendEntriesArgs — аргументы RPC AppendEntries. Без записей журнала это heartbeat.
type AppendEntriesArgs struct {
	Term     int `json:"term"`
	LeaderID int `json:"leaderId"`
}

// AppendEntriesReply — ответ RPC AppendEntries.
type AppendEntriesReply struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

// Transport — P2P-связь с другими узлами.
type Transport interface {
	RequestVote(peer int, args RequestVoteArgs) (RequestVoteReply, error)
	AppendEntries(peer int, args AppendEntriesArgs) (AppendEntriesReply, error)
}

// Node — один узел кластера Raft.
type Node struct {
	mu    sync.Mutex
	id    int
	peers []int
	tr    Transport

	// Persistent state.
	currentTerm int
	votedFor    int

	// Volatile state.
	state            State
	leaderID         int
	electionDeadline time.Time
	nextHeartbeat    time.Time
	peerAlive        map[int]bool
}

// NewNode создает узел в роли follower.
func NewNode(id int, peers []int, tr Transport) *Node {
	n := &Node{
		id:        id,
		peers:     peers,
		tr:        tr,
		votedFor:  -1,
		leaderID:  -1,
		peerAlive: map[int]bool{},
	}
	n.resetElectionTimer()

	return n
}

// Run запускает цикл таймеров: выборы для follower/candidate, heartbeat для лидера.
func (n *Node) Run() {
	for range time.Tick(tickInterval) {
		n.mu.Lock()

		now := time.Now()
		if n.state == Leader && now.After(n.nextHeartbeat) {
			n.nextHeartbeat = now.Add(heartbeatInterval)
			n.broadcastAppendEntries()
		} else if n.state != Leader && now.After(n.electionDeadline) {
			n.startElection()
		}

		n.mu.Unlock()
	}
}

// RequestVote — обработчик RPC.
func (n *Node) RequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term, -1)
	}

	n.markPeer(args.CandidateID, nil)

	granted := args.Term == n.currentTerm &&
		(n.votedFor == -1 || n.votedFor == args.CandidateID)

	if granted {
		n.votedFor = args.CandidateID
		n.resetElectionTimer()
		n.logf("голосую за узел %d", args.CandidateID)
	}

	return RequestVoteReply{Term: n.currentTerm, VoteGranted: granted}
}

// AppendEntries — обработчик heartbeat от лидера.
func (n *Node) AppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		return AppendEntriesReply{Term: n.currentTerm, Success: false}
	}

	n.markPeer(args.LeaderID, nil)
	n.becomeFollower(args.Term, args.LeaderID)
	n.resetElectionTimer()

	return AppendEntriesReply{Term: n.currentTerm, Success: true}
}

// Report — снимок состояния узла для клиентского контроллера.
type Report struct {
	ID         int
	State      string
	Term       int
	LeaderID   int
	Peers      []int
	AlivePeers []int
}

// Report возвращает снимок текущего состояния узла.
func (n *Node) Report() Report {
	n.mu.Lock()
	defer n.mu.Unlock()

	alive := []int{}

	for _, p := range n.peers {
		if n.peerAlive[p] {
			alive = append(alive, p)
		}
	}

	return Report{
		ID:         n.id,
		State:      n.state.String(),
		Term:       n.currentTerm,
		LeaderID:   n.leaderID,
		Peers:      n.peers,
		AlivePeers: alive,
	}
}

func (n *Node) startElection() {
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = -1
	n.resetElectionTimer()
	n.logf("таймаут выборов → Candidate")

	votes := 1
	if n.isMajority(votes) {
		n.becomeLeader()

		return
	}

	args := RequestVoteArgs{Term: n.currentTerm, CandidateID: n.id}

	for _, peer := range n.peers {
		go func() {
			reply, err := n.tr.RequestVote(peer, args)

			n.mu.Lock()
			defer n.mu.Unlock()

			if !n.markPeer(peer, err) {
				return
			}

			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term, -1)

				return
			}

			if n.state != Candidate || n.currentTerm != args.Term || !reply.VoteGranted {
				return
			}

			votes++
			if n.isMajority(votes) {
				n.becomeLeader()
			}
		}()
	}
}

func (n *Node) becomeLeader() {
	n.state = Leader
	n.leaderID = n.id
	n.nextHeartbeat = time.Now()
	n.logf("стал ЛИДЕРОМ")
}

// becomeFollower не сбрасывает таймер выборов.
func (n *Node) becomeFollower(term, leader int) {
	if n.state == Leader {
		n.resetElectionTimer()
	}

	if term > n.currentTerm {
		n.currentTerm, n.votedFor = term, -1
	}

	if n.state != Follower || n.leaderID != leader {
		n.state, n.leaderID = Follower, leader
		n.logf("→ Follower, лидер=%d", leader)
	}
}

// broadcastAppendEntries рассылает heartbeat (пустой AppendEntries) всем пирам.
func (n *Node) broadcastAppendEntries() {
	args := AppendEntriesArgs{Term: n.currentTerm, LeaderID: n.id}

	for _, peer := range n.peers {
		go func() {
			reply, err := n.tr.AppendEntries(peer, args)

			n.mu.Lock()
			defer n.mu.Unlock()

			if n.markPeer(peer, err) && reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term, -1)
			}
		}()
	}
}

func (n *Node) isMajority(votes int) bool {
	return votes*2 > len(n.peers)+1
}

// resetElectionTimer перевыбирает случайный таймаут заново на каждый цикл ожидания.
func (n *Node) resetElectionTimer() {
	n.electionDeadline = time.Now().Add(minElectionTimeout + rand.N(maxElectionTimeout-minElectionTimeout))
}

// markPeer отмечает доступность пира, логирует подключение/отключение, возвращает true при успехе RPC.
func (n *Node) markPeer(peer int, err error) bool {
	ok := err == nil
	if n.peerAlive[peer] == ok {
		return ok
	}

	n.peerAlive[peer] = ok
	if ok {
		n.logf("пир %d подключен", peer)
	} else {
		n.logf("пир %d недоступен", peer)
	}

	return ok
}

func (n *Node) logf(format string, args ...any) {
	log.Printf("[узел %d][term %d] %s: %s", n.id, n.currentTerm, n.state, fmt.Sprintf(format, args...))
}
