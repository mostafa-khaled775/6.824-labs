package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const heartBeatDelay = 100
const timeoutDelayMin = 110
const timeoutDelayRng = 300
const verbose = false

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type VoteRequest struct {
	term    int
	id      int
	granted chan<- bool
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	term          <-chan int
	setTerm       chan<- int
	termUpdateSub chan<- TermUpdateSub
	killed        chan struct{}
	voteRequests  chan<- VoteRequest
	isLeader      atomic.Bool

	heartBeats chan int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return <-rf.term, rf.isLeader.Load()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term     int
	ServerId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	curTerm := <-rf.term
	if args.Term > curTerm {
		rf.setTerm <- args.Term
	}
	granted := make(chan bool)
	rf.voteRequests <- VoteRequest{args.Term, args.ServerId, granted}
	reply.VoteGranted = <-granted
	reply.Term = <-rf.term
}

type AppendEntriesArgs struct {
	Term int
}
type AppendEntriesReply struct {
	Term int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	term := <-rf.term
	rf.setTerm <- args.Term
	if args.Term >= term {
		rf.heartBeats <- args.Term
	}
	reply.Term = max(args.Term, term)
}

type LogEntry struct {
	term int
}

type TermUpdateSub struct {
	ch        chan<- int
	firstTerm int
}

func termTracker(termUpdateSub <-chan TermUpdateSub, getTerm chan<- int, setTerm <-chan int) {
	term := 0
	subs := []chan<- int{}
	for {
		select {
		case sub := <-termUpdateSub:
			if sub.firstTerm < term {
				sub.ch <- term
				close(sub.ch)
			} else {
				subs = append(subs, sub.ch)
			}
		case getTerm <- term:
		case newTerm := <-setTerm:
			if newTerm <= term {
				continue
			}
			term = newTerm
			capturedSubs := subs
			subs = []chan<- int{}
			go func() {
				for _, s := range capturedSubs {
					s <- newTerm
					close(s)
				}
			}()
		}
	}
}

func (rf *Raft) id() string {
	return fmt.Sprintf("Raft-%d", rf.me)
}

func (rf *Raft) electionTimeout() <-chan time.Time {
	ms := timeoutDelayMin + (rand.Int63() % timeoutDelayRng)
	return time.After(time.Millisecond * time.Duration(ms))
}

func (rf *Raft) log(args ...any) {
	if verbose {
		log.Println(append([]any{rf.id(), ": "}, args)...)
	}
}

func (rf *Raft) follower() {
	select {
	case <-rf.heartBeats:
		go rf.follower()
	case <-rf.killed:
		go rf.halt()
	case <-rf.electionTimeout():
		rf.log("timeout: starting a new term")
		go rf.candidate(<-rf.term + 1)
	}
}

func (rf *Raft) closeOnTermChange(term int, ch chan struct{}) {
	termChanged := make(chan int)
	rf.termUpdateSub <- TermUpdateSub{termChanged, term}
	select {
	case <-termChanged:
		close(ch)
	case <-rf.heartBeats:
		close(ch)
		<-termChanged
	}
}

func (rf *Raft) sendRequestVote(peer *labrpc.ClientEnd, term int, voteGranted chan<- bool, stop <-chan struct{}) {
	args := RequestVoteArgs{term, rf.me}
	reply := RequestVoteReply{}
	ok := peer.Call("Raft.RequestVote", &args, &reply)
	if ok {
		voteGranted <- reply.VoteGranted
		rf.setTerm <- reply.Term
		return
	}
	select {
	case <-stop:
		voteGranted <- false
		return
	case <-rf.killed:
		voteGranted <- false
		return
	case <-time.After(time.Millisecond * 5):
		rf.sendRequestVote(peer, term, voteGranted, stop)
	}
}

func (rf *Raft) voteCollector(term int, votes chan bool, promote chan<- struct{}) {
	votesGranted := 0
	votesReceived := 0
	requestedFromSelf := false
	promoted := false
	for voteGranted := range votes {
		votesReceived++
		if votesReceived == len(rf.peers) {
			close(votes)
		}
		if voteGranted {
			votesGranted++
		}
		if votesGranted == len(rf.peers)/2 && !requestedFromSelf {
			requestedFromSelf = true
			rf.voteRequests <- VoteRequest{term, rf.me, votes}
		}
		if votesGranted > len(rf.peers)/2 && !promoted {
			promoted = true
			close(promote)
		}
	}
	if promoted {
		rf.log("promoted to leader for term ", term, " with ", votesGranted, " votes")
	}
}

func (rf *Raft) candidate(term int) {
	rf.setTerm <- term
	demote := make(chan struct{})
	promote := make(chan struct{})
	votes := make(chan bool)

	go rf.voteCollector(term, votes, promote)

	go rf.closeOnTermChange(term, demote)

	for i, peer := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(peer, term, votes, demote)
	}

	select {
	case <-demote:
		go rf.follower()
	case <-promote:
		go rf.leader(term)
	case <-rf.killed:
		go rf.halt()
	case <-rf.electionTimeout():
		go rf.candidate(term + 1)
	}
}

func (rf *Raft) sendHeartBeats(demoted <-chan struct{}, peer *labrpc.ClientEnd, term int) {
	for {
		select {
		case <-demoted:
			return
		case <-rf.killed:
			return
		case <-time.After(time.Millisecond * heartBeatDelay):
			var reply AppendEntriesReply
			if ok := peer.Call("Raft.AppendEntries", &AppendEntriesArgs{term}, &reply); ok {
				if reply.Term > term {
					rf.setTerm <- reply.Term
				}
			}
		}
	}
}

func (rf *Raft) leader(term int) {
	demote := make(chan struct{})
	rf.isLeader.Store(true)
	defer rf.isLeader.Store(false)
	go rf.closeOnTermChange(term, demote)

	// start heartbeat go-routines
	for i, peer := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendHeartBeats(demote, peer, term)
	}
	select {
	case <-rf.killed:
		go rf.halt()
	case <-demote:
		go rf.follower()
	}
}

func (rf *Raft) halt() {
	time.Sleep(time.Second)
	rf.halt()
}

func voter(term <-chan int, requests <-chan VoteRequest, halt <-chan struct{}) {
	votedFor := -1
	lastTerm := 0
	for {
		select {
		case request := <-requests:
			curTerm := <-term
			if curTerm > lastTerm {
				lastTerm = curTerm
				votedFor = -1
			}
			if request.term < curTerm {
				request.granted <- false
			} else if request.term == curTerm {
				if votedFor == -1 {
					votedFor = request.id
					request.granted <- true
				} else if votedFor == request.id {
					log.Panicln("internal error")
				} else {
					request.granted <- false
				}
			} else if request.term > curTerm {
				log.Panicln("internal error!")
			}
		case <-halt:
			return
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	close(rf.killed)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	term := make(chan int)
	rf.term = term
	termUpdateSub := make(chan TermUpdateSub)
	rf.termUpdateSub = termUpdateSub
	voteRequests := make(chan VoteRequest)
	rf.voteRequests = voteRequests
	setTerm := make(chan int)
	rf.setTerm = setTerm
	rf.killed = make(chan struct{})
	rf.heartBeats = make(chan int)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go voter(rf.term, voteRequests, rf.killed)
	go termTracker(termUpdateSub, term, setTerm)
	go rf.follower()
	return rf
}
