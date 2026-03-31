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
	"context"
	"fmt"
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

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	term          <-chan int
	setTerm       chan<- int
	termUpdateSub chan<- TermUpdateSub
	voteRequests  chan<- VoteRequest
	kill          context.CancelFunc
	isLeader      atomic.Bool

	heartBeats chan int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return <-rf.term, rf.isLeader.Load()
}

func (rf *Raft) goForEachPeer(f func(*labrpc.ClientEnd)) {
	for i, peer := range rf.peers {
		if i != rf.me {
			go f(peer)
		}
	}
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

func (rf *Raft) CallRequestVote(peer *labrpc.ClientEnd, args RequestVoteArgs) bool {
	var reply RequestVoteReply
	if ok := peer.Call("Raft.RequestVote", &args, &reply); ok {
		if reply.Term > args.Term {
			rf.setTerm <- reply.Term
		}
	}
	return reply.VoteGranted
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

func (rf *Raft) CallAppendEntries(peer *labrpc.ClientEnd, args AppendEntriesArgs) {
	var reply AppendEntriesReply
	if ok := peer.Call("Raft.AppendEntries", &args, &reply); ok {
		if reply.Term > args.Term {
			rf.setTerm <- reply.Term
		}
	}
}

type LogEntry struct {
	term int
}

type TermUpdateSub struct {
	ch        chan<- int
	firstTerm int
}

func termTracker(ctx context.Context, termUpdateSub <-chan TermUpdateSub, getTerm chan<- int, setTerm <-chan int) {
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
		case <-ctx.Done():
			close(getTerm)
			return
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

type state func(context.Context) state

func (rf *Raft) onTermChange(ctx context.Context, term int) (context.Context, context.CancelFunc) {
	c, cancel := context.WithCancel(ctx)
	go func() {
		termChanged := make(chan int)
		rf.termUpdateSub <- TermUpdateSub{termChanged, term}
		<-termChanged
		cancel()
	}()
	return c, cancel
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
	rf.kill()
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
	rf.heartBeats = make(chan int)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	ctx, cancel := context.WithCancel(context.Background())
	rf.kill = cancel
	go voter(ctx, rf.term, voteRequests)
	go termTracker(ctx, termUpdateSub, term, setTerm)
	go func() {
		state := rf.follower
		for {
			select {
			case <-ctx.Done():
				break
			default:
				state = state(ctx)
			}
		}
	}()
	return rf
}
