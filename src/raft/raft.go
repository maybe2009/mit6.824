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

import "sync"
import (
	"labrpc"
	"log"
	"math/rand"
	"time"
)

// import "bytes"
// import "encoding/gob"

const (
	HEARTBEAT_TICKER_DURATION time.Duration = time.Millisecond * 5
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

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
	hasLeader       bool
	leader          int
	currentTerm     int
	votedTerms      map[int]int
	electionTimer   *time.Timer
	electionTimeout time.Duration

	stopHeartbeatCh chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	term = rf.currentTerm
	if rf.leader == rf.me {
		isleader = true
	} else {
		isleader = false
	}

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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Me   int
	Term int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Agree bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//log.Println("Raft ", rf.me, "term ", rf.currentTerm, " receive vote request from Raft ", args.Me, " term ", args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term == rf.currentTerm {
		if args.Me != rf.leader {
			reply.Agree = false
		} else {
			// heartbeat, reset timer
			reply.Agree = true
			rf.electionTimer.Reset(rf.electionTimeout)
			//log.Println("rf ", args.Me, " -> rf ", rf.me)
		}

	} else if args.Term > rf.currentTerm {
		// vote request
		_, voted := rf.votedTerms[args.Term]

		if voted {
			reply.Agree = false

		} else {
			rf.stopElectionTimer()

			// new term start
			if rf.leader == rf.me {
				rf.dethrone()
			}

			reply.Agree = true
			rf.leader = args.Me
			rf.votedTerms[args.Term] = args.Me
			rf.currentTerm = args.Term
			rf.electionTimer.Reset(rf.electionTimeout)
			log.Println("Raft ", rf.me, " vote Raft ", args.Me, " as leader in term ",
				args.Term)
		}

	} else {
		reply.Agree = false
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
	//log.Println("Rf ", rf.me, " term ", rf.currentTerm, " call Rf ", server, " term ", args.Term)
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
	rf.leader = -1
	rf.hasLeader = false
	// Your initialization code here (2A, 2B, 2C).
	rf.votedTerms = map[int]int{}

	// init heartbeat channel
	rf.stopHeartbeatCh = make(chan bool)

	// start election clock
	go rf.startElectionTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) startElectionTimer() {
	// pick an random election timeout
	rand.Seed(int64(rf.me))
	rf.electionTimeout = time.Duration(rand.Uint32()%100+150) * time.Millisecond
	rf.electionTimer = time.NewTimer(rf.electionTimeout)
	log.Println("server ", rf.me, " timeout ", rf.electionTimeout.Seconds())

	for _ = range rf.electionTimer.C {
		rf.hasLeader = false
		rf.triggerElection()
	}

}

func (rf *Raft) stopElectionTimer() {
	rf.electionTimer.Stop()
}

func (rf *Raft) acquireVote() int {
	agreeCount := 0
	ch := make(chan bool)
	timeout := time.NewTimer(time.Millisecond * 50)

	// send vote request to all raft nodes
	for server, _ := range rf.peers {
		if server == rf.me {
			agreeCount++
			continue
		}

		go func(me int, server int, term int) {
			var req RequestVoteArgs
			var rsp RequestVoteReply

			req.Me = me
			req.Term = term

			//log.Println("Raft ", me, " send vote request to ", server, " term ", term)
			rf.sendRequestVote(server, &req, &rsp)

			if rsp.Agree {
				ch <- true
			} else {
				ch <- false
			}
		}(rf.me, server, rf.currentTerm)
	}

	// wait for vote result
	count := len(rf.peers)
OuterLoop:
	for ; count > 0; count-- {
		select {
		case agree := <-ch:
			if agree {
				agreeCount++
			}
		case <-timeout.C:
			break OuterLoop
		}
	}

	return agreeCount
}

func (rf *Raft) triggerElection() {
	rf.currentTerm = rf.currentTerm + 1

	log.Println("server ", rf.me, " trigger election in term ", rf.currentTerm)

	agreeCount := rf.acquireVote()
	if agreeCount > (len(rf.peers) / 2) {
		//we win the election
		//turn into leader
		rf.enthrone()

	} else {
		log.Println("server ", rf.me, " failed to win the election in term ",
			rf.currentTerm)
		rf.currentTerm = rf.currentTerm - 1
	}

	rf.electionTimer.Reset(rf.electionTimeout)
}

func (rf *Raft) enthrone() {
	log.Println("server ", rf.me, " win the election in term ", rf.currentTerm)

	rf.hasLeader = true
	rf.leader = rf.me

	go rf.sendHeartbeat()
}

func (rf *Raft) dethrone() {
	defer log.Println("Raft ", rf.me, " dethrone")
	rf.stopHeartbeat()
}

func (rf *Raft) sendHeartbeat() {
	ticker := time.NewTicker(HEARTBEAT_TICKER_DURATION)

	for {
		select {
		case <-ticker.C:
			rf.mu.Lock()
			term := rf.currentTerm
			rf.mu.Unlock()

			//log.Println("send heartbeat to ", len(rf.peers), " peers: ", rf.peers)
			for server, _ := range rf.peers {
				//log.Println("Rf ", rf.me, " -> Rf ", server)
				go func(server int) {
					var req RequestVoteArgs
					req.Me = rf.me
					req.Term = term

					//log.Println("send to ", server)
					err, _ := rf.syncSendRequest(server, &req, time.Millisecond*10)
					if err != nil {
						//log.Println("Raft ", rf.me, "send heartbeat to ",
						//	server, " failed: ", err)
					}
				}(server)
			}

		case stop := <-rf.stopHeartbeatCh:
			if stop {
				log.Println("raft ", rf.me, " stop sending heartbeat")
				return
			}
		}
	}
}

func (rf *Raft) stopHeartbeat() {
	rf.stopHeartbeatCh <- true
}

func (rf *Raft) syncSendRequest(
	server int,
	req *RequestVoteArgs,
	timeout time.Duration) (error, *RequestVoteReply) {
	var rsp RequestVoteReply
	var err error

	t := time.NewTimer(timeout)
	ch := make(chan bool)
	go func() {
		if rf.sendRequestVote(server, req, &rsp) {
			ch <- true
		} else {
			ch <- false
		}
	}()

	select {
	case ok := <-ch:
		if !ok {
			err = &RfError{"request failed"}
		}

	case <-t.C:
		err = &RfError{"request timeout"}
	}
	return err, &rsp
}

type RfError struct {
	err string
}

func (rf *RfError) Error() string {
	return rf.err
}
