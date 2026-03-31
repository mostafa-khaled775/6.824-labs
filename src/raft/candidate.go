package raft

import (
	"context"

	"6.5840/labrpc"
)

func (rf *Raft) sendRequestVote(_ context.Context, term int, voteGranted chan<- bool) func(peer *labrpc.ClientEnd) {
	return func(peer *labrpc.ClientEnd) {
		args := RequestVoteArgs{term, rf.me}
		voteGranted <- rf.CallRequestVote(peer, args)
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
}

func (rf *Raft) candidate(ctx context.Context, term int) state {

	rf.setTerm <- term
	promote := make(chan struct{})
	votes := make(chan bool)

	go rf.voteCollector(term, votes, promote)
	childCtx, _ := rf.onTermChange(ctx, term)
	rf.goForEachPeer(rf.sendRequestVote(childCtx, term, votes))

	for {
		select {
		case <-promote:
			return func(ctx context.Context) state { return rf.leader(ctx, term) }
		case <-childCtx.Done():
			return rf.follower
		case <-rf.electionTimeout():
			return func(ctx context.Context) state { return rf.candidate(ctx, term+1) }
		case hbTerm := <-rf.heartBeats:
			if hbTerm >= term {
				return rf.follower
			}
		}
	}
}
