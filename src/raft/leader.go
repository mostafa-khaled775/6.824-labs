package raft

import (
	"context"
	"time"

	"6.5840/labrpc"
)

func (rf *Raft) sendPeriodicHeartBeats(ctx context.Context, term int) func(peer *labrpc.ClientEnd) {
	return func(peer *labrpc.ClientEnd) {
		go rf.CallAppendEntries(peer, AppendEntriesArgs{term})
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Millisecond * heartBeatDelay):
				go rf.CallAppendEntries(peer, AppendEntriesArgs{term})
			}
		}
	}
}

func (rf *Raft) leader(ctx context.Context, term int) state {
	rf.isLeader.Store(true)
	defer rf.isLeader.Store(false)
	childCtx, _ := rf.onTermChange(ctx, term)
	rf.goForEachPeer(rf.sendPeriodicHeartBeats(childCtx, term))
	<-childCtx.Done()
	return rf.follower
}
