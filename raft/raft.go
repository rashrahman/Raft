package raft

//Collaborators: Afzal Khan, Mussie Abraham

//
// This is an outline of the API that raft must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"bytes"
	"cs350/labgob"
	"cs350/labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// log entry struct
type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // This peer's index into peers[]
	dead      int32               // Set by Kill()

	// Your data here (4A, 4B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//4A states for leader election
	//persistent state on all servers
	CurrentTerm int        //latest term server has seen
	VotedFor    int        //candidate id that received vote in current term (null if none)
	Log         []LogEntry //log entries; each entry contains command for state machine, and term when entry was received by leader

	// //volatile state on all servers
	CommitIndex int //index of highest log entry known to be committed
	LastApplied int //index of highest log entry applied to state machine

	//volatile state on leaders
	NextIndex  []int //for each server, index of the next log entry to send to that server
	MatchIndex []int //for each server, index of highest log entry known to be replicated on server

	State         int         //0 for follower, 1 for candidate, 2 for leader
	ResetHearbeat *time.Timer //timer for heartbeat
	ElectionTimer *time.Timer
	applyCh       chan ApplyMsg
}

// Return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (4A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = false
	term = rf.CurrentTerm
	if rf.State == 2 {
		isleader = true
	}

	return term, isleader

}

// Save Raft's persistent state to stable storage, where it
// can later be retrieved after a crash and restart. See paper's
// Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (4B).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (4B).
	// Example:
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var term int
	var logEntries []LogEntry
	var votedFor int

	// Attempt to decode the data
	errTerm := decoder.Decode(&term)
	errVotedFor := decoder.Decode(&votedFor)
	errLog := decoder.Decode(&logEntries)

	if errTerm != nil || errVotedFor != nil || errLog != nil {
		panic("error in readPersist()")
	} else {
		rf.CurrentTerm = term
		rf.VotedFor = votedFor
		rf.Log = logEntries
	}

}

// Example RequestVote RPC arguments structure.
// Field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (4A, 4B).
	//4A - leader election
	Term         int //candidate's term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate's last log entry
	LastLogTerm  int //term of candidate's last log entry
}

// Example RequestVote RPC reply structure.
// Field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (4A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}
type AppendEntriesArgs struct {
	Term         int        //leader's term
	LeaderId     int        //so follower can redirect clients
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevlogindex entry
	Entries      []LogEntry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        //leader's commit index
}

type AppendEntriesReply struct {
	Term    int  //current term for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLongTerm
}

// Example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (4A, 4B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1                                                             // Reset VotedFor in case of a new term
		rf.State = 0                                                                 // Convert to follower if higher term
		rf.ResetHearbeat.Stop()                                                      //stopping the timer
		rf.ElectionTimer.Reset(time.Duration(rand.Intn(100)+400) * time.Millisecond) //resetting
	}

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return

	}

	currentLogTerm := rf.Log[len(rf.Log)-1].Term
	logUpToDate := args.LastLogTerm > currentLogTerm || (args.LastLogTerm == currentLogTerm && args.LastLogIndex >= len(rf.Log)-1)

	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && args.Term >= rf.CurrentTerm && logUpToDate {
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateId
		rf.ElectionTimer.Reset(time.Duration(rand.Intn(100)+400) * time.Millisecond)
	} else {
		reply.VoteGranted = false
	}
	rf.persist()
	reply.Term = rf.CurrentTerm

}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Println("AppendEntries called with Term:", args.Term)
	//Receiver implementation #1:
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.State = 0
		rf.VotedFor = -1
		rf.CurrentTerm = args.Term
		rf.ResetHearbeat.Stop()

	}
	rf.ElectionTimer.Reset(time.Duration(rand.Intn(100)+400) * time.Millisecond) //reset election timer

	//Receiever implementation #2
	lastLogIndex := len(rf.Log) - 1
	if lastLogIndex < args.PrevLogIndex || rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// fmt.Println("Log not up-to-date with the leader.")
		reply.Success = false
		reply.Term = rf.CurrentTerm

		return
	}

	// receiver implementations #3 & #4
	indexMismatch := -1
	for index, entry := range args.Entries {
		if args.PrevLogIndex+2+index > len(rf.Log) || rf.Log[args.PrevLogIndex+1+index].Term != entry.Term {
			// fmt.Printf("Log mismatch at index %d\n", indexMismatch)
			indexMismatch = index
			break
		}
	}

	if indexMismatch != -1 {
		endIndex := args.PrevLogIndex + 1 + indexMismatch
		rf.Log = append(rf.Log[:endIndex], args.Entries[indexMismatch:]...)
	}

	// receiver implmenation 5
	newCommitIndex := min(args.LeaderCommit, lastLogIndex)
	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = newCommitIndex
		rf.applyLog()
	}
	reply.Success = true
	reply.Term = rf.CurrentTerm
	// fmt.Println("AppendEntries successful. Persisting state.")
	rf.persist()
}

// min function
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Example code to send a RequestVote RPC to a server.
// Server is the index of the target server in rf.peers[].
// Expects RPC arguments in args. Fills in *reply with RPC reply,
// so caller should pass &reply.
//
// The types of the args and reply passed to Call() must be
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
// Look at the comments in ../labrpc/labrpc.go for more details.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. The third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1

	term := rf.CurrentTerm
	isLeader := rf.State == 2

	if isLeader {
		index = len(rf.Log)
		rf.Log = append(rf.Log, LogEntry{Term: term, Command: command})
		rf.MatchIndex[rf.me] = index

		return index, term, isLeader

	}
	rf.persist()

	return index, term, isLeader

}

// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.ElectionTimer.C:
			rf.mu.Lock()
			if rf.State == 0 || rf.State == 1 {
				rf.State = 1
				rf.CurrentTerm++
				rf.VotedFor = rf.me
				voteReceived := 1
				peers := rf.peers
				rf.ElectionTimer.Reset(time.Duration(rand.Intn(100)+400) * time.Millisecond)
				// fmt.Println("Election timer reset after becoming Candidate.")
				rf.mu.Unlock()
				for peer := range peers {
					if peer != rf.me {
						go func(peer int) {
							rf.mu.Lock()
							if rf.State != 1 || rf.killed() {
								// fmt.Println("Not in Candidate state anymore or server killed, aborting vote request.")
								rf.mu.Unlock()
								return
							}

							args := &RequestVoteArgs{
								Term:         rf.CurrentTerm,
								CandidateId:  rf.me,
								LastLogIndex: len(rf.Log) - 1,
								LastLogTerm:  rf.Log[len(rf.Log)-1].Term,
							}
							rf.mu.Unlock()
							reply := &RequestVoteReply{}
							ok := rf.sendRequestVote(peer, args, reply)
							if ok {
								rf.mu.Lock()
								if reply.Term < rf.CurrentTerm || rf.State != 1 || args.Term != rf.CurrentTerm {
									rf.mu.Unlock()
									return
								}
								if reply.Term > rf.CurrentTerm {
									// fmt.Printf("Received higher term from %d during vote request, converting to Follower.\n", peer)
									rf.State = 0
									rf.VotedFor = -1
									rf.CurrentTerm = reply.Term
									rf.ResetHearbeat.Stop()
									rf.ElectionTimer.Reset(time.Duration(rand.Intn(100)+400) * time.Millisecond)
								}

								if reply.VoteGranted {

									// fmt.Printf("Vote granted by %d, total votes received now: %d\n", peer, voteReceived)
									voteReceived++
									if voteReceived > len(peers)/2 {
										if rf.State == 2 {
											rf.mu.Unlock()
											return
										}
										rf.State = 2
										for i := range rf.NextIndex {
											rf.NextIndex[i] = len(rf.Log)
										}
										for i := range rf.MatchIndex {
											rf.MatchIndex[i] = 0
										}

										rf.mu.Unlock()
										rf.ElectionTimer.Stop()

										for peer := range rf.peers {
											if peer != rf.me {
												go func(peer int) {

													rf.mu.Lock()
													if rf.State != 2 {
														rf.mu.Unlock()
														return
													}

													args := &AppendEntriesArgs{
														Term:         rf.CurrentTerm,
														LeaderId:     rf.me,
														PrevLogIndex: rf.NextIndex[peer] - 1,
														PrevLogTerm:  rf.Log[rf.NextIndex[peer]-1].Term,
														Entries:      make([]LogEntry, len(rf.Log[(rf.NextIndex[peer]):])),
														LeaderCommit: rf.CommitIndex,
													}
													rf.mu.Unlock()

													reply := &AppendEntriesReply{}
													ok := rf.sendAppendEntries(peer, args, reply)
													rf.mu.Lock()
													if ok {
														if rf.State != 2 || rf.CurrentTerm != args.Term {
															rf.mu.Unlock()
															return
														}
													}
													rf.mu.Unlock()

													if ok && reply.Success {
														rf.mu.Lock()
														rf.MatchIndex[peer] = args.PrevLogIndex + len(args.Entries)
														rf.NextIndex[peer] = rf.MatchIndex[peer] + 1
														N := len(args.Entries) + args.PrevLogIndex
														count := 0
														if rf.Log[N].Term == rf.CurrentTerm && N > rf.CommitIndex {
															for peer := range rf.peers {
																if rf.MatchIndex[peer] >= N {
																	count++
																}
															}
															if count > len(rf.peers)/2 {
																rf.CommitIndex = N
																rf.mu.Unlock()
																rf.applyLog()
																rf.mu.Lock()
															}
														}
														rf.mu.Unlock()
													} else {
														rf.mu.Lock()
														defer rf.mu.Unlock()
														if reply.Term > rf.CurrentTerm {
															rf.CurrentTerm = reply.Term
															rf.State = 0
															rf.ResetHearbeat.Stop()
															rf.ElectionTimer.Reset(time.Duration(rand.Intn(100)+400) * time.Millisecond)
															rf.VotedFor = -1
															rf.persist()

														} else {
															rf.NextIndex[peer] = 1
														}
													}
												}(peer)
											}
											time.Sleep(10 * time.Millisecond)
										}
										rf.mu.Lock()
										rf.ResetHearbeat.Reset(100 * time.Millisecond)
									}

								}
								rf.mu.Unlock()
							}

						}(peer)

					}
				}
				rf.mu.Lock()
			}
			rf.mu.Unlock()

		case <-rf.ResetHearbeat.C:
			rf.mu.Lock()
			if rf.State == 2 {
				rf.mu.Unlock()
				for peer := range rf.peers {
					if peer != rf.me {
						go func(peer int) {
							rf.mu.Lock()

							args := &AppendEntriesArgs{
								Term:         rf.CurrentTerm,
								LeaderId:     rf.me,
								PrevLogIndex: rf.NextIndex[peer] - 1,
								PrevLogTerm:  rf.Log[rf.NextIndex[peer]-1].Term,
								Entries:      append([]LogEntry(nil), rf.Log[rf.NextIndex[peer]:]...),
								LeaderCommit: rf.CommitIndex,
							}
							// fmt.Printf("Sending AppendEntries to peer %d.\n", peer)
							reply := &AppendEntriesReply{}
							ok := rf.sendAppendEntries(peer, args, reply)
							rf.mu.Unlock()

							if ok && reply.Success {
								rf.mu.Lock()
								rf.MatchIndex[peer] = args.PrevLogIndex + len(args.Entries)
								rf.NextIndex[peer] = rf.MatchIndex[peer] + 1

								N := len(args.Entries) + args.PrevLogIndex
								ct := 0
								if rf.Log[N].Term == rf.CurrentTerm && N > rf.CommitIndex {
									for peer := range rf.peers {
										if rf.MatchIndex[peer] >= N {
											ct++
										}
									}
									if ct > len(rf.peers)/2 {
										rf.CommitIndex = N
										rf.mu.Unlock()
										rf.applyLog()
										rf.mu.Lock()
									}
								}
								rf.mu.Unlock()

							} else {
								rf.mu.Lock()
								defer rf.mu.Unlock()
								if reply.Term > rf.CurrentTerm {
									rf.CurrentTerm = reply.Term
									rf.State = 0
									rf.ResetHearbeat.Stop()
									rf.ElectionTimer.Reset(time.Duration(rand.Intn(100)+400) * time.Millisecond)
									rf.VotedFor = -1
									rf.persist()
								} else {
									rf.NextIndex[peer] = 1

								}
							}
						}(peer)
					}
					time.Sleep(10 * time.Millisecond)
				}
				rf.mu.Lock()
				rf.ResetHearbeat.Reset(100 * time.Millisecond)
			}
			rf.mu.Unlock()
		}

		time.Sleep(100 * time.Millisecond)

	}
}

func (rf *Raft) applyLog() {
	if rf.LastApplied < rf.CommitIndex {
		logEntries := rf.Log[rf.LastApplied+1 : rf.CommitIndex+1]

		go func(startIndex int, entries []LogEntry) {
			for i, entry := range entries {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: startIndex + i,
				}
				rf.applyCh <- msg

				rf.mu.Lock()
				if rf.LastApplied < msg.CommandIndex {
					rf.LastApplied = msg.CommandIndex
				}
				rf.mu.Unlock()
			}
		}(rf.LastApplied+1, logEntries)
	}
}

// sendAppendEntries is the method to send the actual AppendEntries RPC to a peer
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// The service or tester wants to create a Raft server. The ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
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
	// Your initialization code here (4A, 4B).
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Log = make([]LogEntry, 1)
	rf.State = 0
	rf.MatchIndex, rf.NextIndex = make([]int, len(rf.peers)), make([]int, len(rf.peers))
	rf.applyCh = applyCh
	applyCh <- ApplyMsg{
		CommandValid: true,
		Command:      0,
		CommandIndex: 0,
	}
	rf.ResetHearbeat = time.NewTimer(100 * time.Millisecond)
	rf.ElectionTimer = time.NewTimer(time.Duration(rand.Intn(100)+400) * time.Millisecond)
	rf.readPersist(persister.ReadRaftState())
	go rf.ticker()
	return rf
}
