package raft

import (
	"context"
	"log"
)

func voter(ctx context.Context, term <-chan int, requests <-chan VoteRequest) {
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
		case <-ctx.Done():
			return
		}
	}
}
