package raft

import "context"

func (rf *Raft) follower(ctx context.Context) state {
	select {
	case <-rf.heartBeats:
		return rf.follower
	case <-ctx.Done():
		return nil
	case <-rf.electionTimeout():
		return func(ctx context.Context) state { return rf.candidate(ctx, <-rf.term+1) }
	}
}
