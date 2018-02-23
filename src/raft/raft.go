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
	"fmt"
	"labrpc"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Index   int
	Term    int
}

const (
	RAFT_STATE_LEADER    = 0
	RAFT_STATE_CANDIDATE = 1
	RAFT_STATE_FOLLOWER  = 2
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       int
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	isDelaySelectLeader bool
	isNeedExit          bool
	applyCh             chan ApplyMsg

	wg sync.WaitGroup
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == RAFT_STATE_LEADER
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, replay *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.currentTerm {
		// DPrintf("%d: append from %d, term %d->%d", rf.me, args.LeaderId, rf.currentTerm, args.Term)
		rf.votedFor = args.LeaderId
		rf.currentTerm = args.Term
		rf.state = RAFT_STATE_FOLLOWER
		rf.isDelaySelectLeader = true

		if args.Entries != nil {
			i := 0
			for ; i < len(rf.log); i++ {
				if rf.log[i].Term == args.PrevLogTerm && rf.log[i].Index == args.PrevLogIndex {
					break
				}
			}
			if i == len(rf.log) {
				DPrintf("%d: can not find start index", rf.me)
				replay.Term = rf.currentTerm
				replay.Success = false
				return
			}
			i++
			j := 0
			for j < len(args.Entries) && i < len(rf.log) {
				if rf.log[i].Term == args.Entries[j].Term && rf.log[i].Term == args.Entries[j].Index {
					i++
					j++
				} else {
					break
				}
			}
			if i != len(rf.log) {
				panic(fmt.Sprintf("%d: log not consistent", rf.me))
			}
			rf.log = append(rf.log, args.Entries[j:]...)
		}

		if args.LeaderCommit > rf.commitIndex && rf.commitIndex < len(rf.log)-1 {
			for i := rf.commitIndex + 1; i < len(rf.log); i++ {
				DPrintf("%d: follower apply msg, %d, %d", rf.me, i, len(rf.log))
				rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i}
			}
			rf.commitIndex = len(rf.log) - 1
		}

		replay.Success = true
	} else {
		replay.Term = rf.currentTerm
		replay.Success = false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		// DPrintf("%d: vote %d, term %d->%d", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.isDelaySelectLeader = true
		rf.state = RAFT_STATE_FOLLOWER
		reply.VoteGranted = true
	} else {
		// DPrintf("%d: reject %d, term %d <= %d", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	term, isLeader = rf.currentTerm, rf.state == RAFT_STATE_LEADER
	if isLeader {
		index = len(rf.log)
		rf.log = append(rf.log, LogEntry{command, len(rf.log), rf.currentTerm})
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.isNeedExit = true
	rf.wg.Wait()
	DPrintf("%d: be killed, term %d", rf.me, rf.currentTerm)
}

func (rf *Raft) selectLeader() {
	rf.mu.Lock()
	isNeedSelectLeader := false
	if rf.state == RAFT_STATE_FOLLOWER || rf.state == RAFT_STATE_CANDIDATE {
		rf.state = RAFT_STATE_CANDIDATE
		rf.currentTerm++
		rf.votedFor = rf.me
		isNeedSelectLeader = true
	}
	term := rf.currentTerm
	rf.mu.Unlock()

	if isNeedSelectLeader {
		DPrintf("%d: select leader, term %d", rf.me, term)

		args := &RequestVoteArgs{term, rf.me, 0, 0}

		var voteCount int32 = 1

		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}

			go func(i int) {
				reply := &RequestVoteReply{}
				// DPrintf("%d: send get vote to %d", rf.me, i)
				if rf.sendRequestVote(i, args, reply) {
					if reply.VoteGranted {
						DPrintf("%d: get vote from %d, term %d", rf.me, i, term)
						atomic.AddInt32(&voteCount, 1)
					} else {
						// DPrintf("%d: %d reject vote, term %d >= %d", rf.me, i, reply.Term, term)
					}
				} else {
					DPrintf("%d: get vote from %d failed, term %d", rf.me, i, term)
				}
			}(i)
		}

		time.Sleep(200 * time.Millisecond)

		if voteCount > int32(len(rf.peers)/2) {
			rf.mu.Lock()
			if term == rf.currentTerm {
				rf.state = RAFT_STATE_LEADER
				rf.nextIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.log)
				}
				DPrintf("%d: become leader, term %d", rf.me, rf.currentTerm)
			}
			rf.mu.Unlock()
			return
		}
	}
}

func (rf *Raft) keepLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == RAFT_STATE_LEADER {
		DPrintf("%d: keep leader, term %d", rf.me, rf.currentTerm)

		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: 0,
				PrevLogTerm: 0, Entries: nil, LeaderCommit: rf.commitIndex}
			if rf.nextIndex[i] < len(rf.log) {
				args.PrevLogIndex = rf.log[rf.nextIndex[i]-1].Index
				args.PrevLogTerm = rf.log[rf.nextIndex[i]-1].Term
				args.Entries = rf.log[rf.nextIndex[i]:]
			}
			reply := &AppendEntriesReply{}

			go func(i int) {
				ok := rf.sendAppendEntries(i, args, reply)

				if ok {
					if reply.Success {
						rf.mu.Lock()
						if args.Term == rf.currentTerm && args.Entries != nil {
							rf.nextIndex[i] += len(args.Entries)

							// update commmit index
							minIndex, maxIndex := math.MaxInt64, math.MinInt32
							for i, _ := range rf.peers {
								if rf.me == i {
									continue
								}
								if minIndex > rf.nextIndex[i] {
									minIndex = rf.nextIndex[i]
								}
								if maxIndex < rf.nextIndex[i] {
									maxIndex = rf.nextIndex[i]
								}
							}
							minIndex--
							maxIndex--
							commitIndex := minIndex
							for ; minIndex <= maxIndex; minIndex++ {
								confirmCount := 0
								if minIndex < len(rf.log) {
									confirmCount++
								} else {
									break
								}
								for i, _ := range rf.peers {
									if rf.me == i {
										continue
									}
									if rf.nextIndex[i] >= minIndex {
										confirmCount++
									}
								}
								if confirmCount > len(rf.peers)/2 {
									commitIndex = minIndex
								} else {
									break
								}
							}
							for i := rf.commitIndex + 1; i <= commitIndex; i++ {
								DPrintf("%d: leader apply msg %d %d", rf.me, i, len(rf.log))
								rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i}
							}
							rf.commitIndex = commitIndex
						}
						rf.mu.Unlock()
					} else {
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							DPrintf("%d: find grate term %d from %d, term %d", rf.me, reply.Term, i, rf.currentTerm)
							rf.state = RAFT_STATE_FOLLOWER
						} else {
							rf.nextIndex[i] /= 2
						}
						rf.mu.Unlock()
					}
				} else {
					DPrintf("%d: keep leader to %d failed, term %d", rf.me, i, args.Term)
				}
			}(i)
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.state = RAFT_STATE_FOLLOWER
	rf.applyCh = applyCh
	rf.log = make([]LogEntry, 1)

	rf.wg.Add(2)

	go func(rf *Raft) {
		for {
			rf.mu.Lock()
			rf.isDelaySelectLeader = false
			rf.mu.Unlock()

			time.Sleep(time.Duration(300+rand.Intn(300)) * time.Millisecond)

			if rf.isNeedExit {
				break
			}

			if rf.isDelaySelectLeader {
				continue
			}

			rf.selectLeader()

			if rf.isNeedExit {
				break
			}
		}
		rf.wg.Done()
	}(rf)

	go func(rf *Raft) {
		for {
			time.Sleep(300 * time.Millisecond)

			if rf.isNeedExit {
				break
			}

			rf.keepLeader()

			if rf.isNeedExit {
				break
			}
		}
		rf.wg.Done()
	}(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
