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
"sync"
"labrpc"
"math/rand"
"time"
) 
// import "bytes"
// import "labgob"

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

// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
    //整个集群的所有节点，当集群固定之后，每个节点的集群集合的下标都是一样的，
    //换句话说，在节点1上访问peers(1)得到第1个节点，在节点2上访问peers(1)也拿到的是同一个节点。
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[] 当前节点在peers列表的下标
    //这里me就是ID

	currentTerm int
	votedFor    int //当前任期，把票投给谁了,一个任期只可以投票一次
	leaderId    int
	state    int

    electionTimer  *time.Timer
    heartbeatTimer  *time.Timer
    resetElection chan interface{}
    done chan interface{}
    heartbeatTimerInterval time.Duration
    electionTimeOutDuration time.Duration
    // shutdownElection chan struct{}
    // shutdownHeartbeat chan struct{}
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}


// const(
// 	Candidate RaftState = "Candidate"
// 	Leader RaftState = "Leader"
// 	Follower RaftState = "Follower"
// )

const(
    Candidate = iota
    Leader
    Follower
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
    rf.mu.Lock()
    defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == Leader
    DPrintf("[%d-%s]: isleader return: %t at term %d \n", rf.me, rf, isleader, rf.currentTerm)
	return term, isleader
}

func (rf *Raft) String() string {
    // rf.mu.Lock()
    // defer rf.mu.Unlock()
    //s死锁
    switch rf.state {
    case Leader:
        return "Leader"
    case Candidate:
        return "Candiate"
    case Follower:
        return "Follower"
    default:
        return ""
    }
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
	Term        int //候选人的任期号
	CandidateId int //请求投票的候选人 id
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int   //leader 的任期数
	LeaderId int   //leader id
	Entries  []int //日志项，为空则做心跳
}

type AppendEntriesReply struct {
	Term    int //节点的currentTerm
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply){
	// Your code here (2A, 2B).
	//接受者需要实现：
	//如果term < currentTerm返回 false（5.1 节）
	//如果votedFor为空或者与candidateId相同，并且候选人的日志和自己的日志一样新，则给该候选人投票（5.2 节 和 5.4 节）
    
    //1.如果term < currentTerm返回 false 
    //2.如果votedFor为空或者与candidateId相同，则给该候选人投票
    //返回投票者自己的当前任期号，是否投票

    //如何实现一个任期内只投票一次
    //一个任期开始后先把VotedFor重置为空
    //这里任期相等时不可以重置，因为可能是别的候选者发来的请求，
    //此时任期已经被第一个候选者的请求重置过了
    rf.mu.Lock()
    defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {

        reply.VoteGranted = false
        reply.Term = rf.currentTerm
    }else {
        //why does this 
        //比如当leader 过期后重新连接时，因为集群开始选举，任期号过期，
        //所以要被重置为follower
        //所以heartbeat Daemon 中要重新判断状态，多发的一个心跳因为任期小会被拒绝
        if args.Term > rf.currentTerm {
            DPrintf("[%d-%s] Candidate's Term is  %d.  rf's currentTerm is %d  \n",rf.me, rf, args.Term, rf.currentTerm)
            rf.state = Follower
            rf.currentTerm = args.Term
            rf.votedFor = -1
        }

         //如果没给人投过票或者是自己发来的请求就投票
        if  (rf.votedFor == -1)  ||  (rf.votedFor == args.CandidateId) {

            rf.votedFor = args.CandidateId
            rf.state = Follower
            //选举过程中要重置其他follower的选举过期定时器
            rf.resetElection <-struct{}{}

            reply.VoteGranted = true
            reply.Term = rf.currentTerm
            
            DPrintf("[%d-%s]: votedFor peer[%d] RPC call at term %d \n", rf.me, rf, args.CandidateId, rf.currentTerm)
        }
    }
    //此时任期已经被第一个候选者的请求重置过了
}


//AppendEntries 可以用来做心跳服务，表示心跳时entries为空
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){

	//如果 entries 为空，视为心跳重置选举过期时间
	//leader的任期必须大于跟随者
    //收到AppendEntries ，当entries为空就重置 election timeout

    rf.mu.Lock()
    defer rf.mu.Unlock()

    if args.Term < rf.currentTerm{
        reply.Success = false
        reply.Term = rf.currentTerm
        return
    }
    DPrintf("[%d-%s]:  receive heartbeat from peer[%d] at term %d", rf.me, rf, args.LeaderId, rf.currentTerm)
    //这里如果把state 判断放在前面就会被错误的请求给重置身份
    //所以放在过滤错误请求后
    //why does this 
    //比如当leader 过期后重新连接时，进群选举完毕，收到心跳
    //所以要被重置为follower

    //BUGFIX 这里因为每次发送心跳时没有过滤自己，导致发送给了自己
    //导致自己被重置为Follower,
    //发送心跳前会手动重置Leader 的 election reset
    if rf.state == Leader {
        rf.turnToFollower()
    }

    //当收到leaderTerm大于currentTerm时说明
    //集群刚发生了选举，需要重置VotedFor
    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.votedFor = -1
    }

    reply.Success = true
    reply.Term = rf.currentTerm    

    //当集群易主的时候需要更新 leaderId
    if args.LeaderId != rf.leaderId{
        rf.leaderId = args.LeaderId
    }

    DPrintf("[%d-%s]:  send reset ElectionTimer signal at heartbeat at term %d", rf.me, rf, rf.currentTerm)
    rf.resetElection <- struct{}{}

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




func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
// the service or tester wants to create a Raft server.
// the ports of all the Raft servers (including this one) are in peers[].
// this server's port is peers[me].
// all the servers' peers[] arrays have the same order.
// persister is a place for this server to save its persistent state,
// and also initially holds the most recent saved state, if any.
// applyCh is a channel on which the tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

//10 heartbeat persecond adn make election timeout more large than 300, but not too large
//because u need elected a leader in 5 second

func (rf *Raft) sendHeartbeat(id int) {
    //发送心跳前重置了Leader 自己的选举超时定时器
    //所以在 sendHeartbeat 发送心跳中要在peers中排除leader自己
    // rf.resetElection <- struct{}{} 放入了heartbeatDaemon中

    // reply := &AppendEntriesReply{}
    // args := &AppendEntriesArgs{}
    // args.Term = rf.currentTerm
    // args.LeaderId = rf.me
    // args.Entries = []int{}

    //返回了term用来更新自己?
    //如果接收到的 RPC 请求或响应中，任期号 T > currentTerm ，那么就令 currentTerm 等于 T，并切换状态为跟随者

    rf.mu.Lock()
    defer rf.mu.Unlock()
    DPrintf("[%d-%s] send Heartbeat to  peers[%d] at term %d", rf.me, rf, id, rf.currentTerm)

    if rf.state != Leader{
        DPrintf("[%d-%s]: peer think he is not leader then return  %d",  rf.me, rf, rf.currentTerm)
        return
    }
    reply := &AppendEntriesReply{}
    args := &AppendEntriesArgs{}
    args.Term = rf.currentTerm
    args.LeaderId = rf.me
    args.Entries = []int{}
    ok := rf.sendAppendEntries(id, args, reply)
    if!ok{
        DPrintf("[%d-%s] failed to send Heartbeat to  peers[%d] at term %d", rf.me, rf, id, rf.currentTerm)
    }else{
        //finder new leader and turned to follower
        if !reply.Success{
            DPrintf("[%d-%s] send Heartbeat to  peers[%d] at term failed %d, finder new leader and turned to follower", rf.me, rf, id, rf.currentTerm)
            rf.turnToFollower()
            rf.resetElection <- struct{}{}
            return
        }
    }

}


func (rf *Raft)turnToCandidate() {
    //变成candidate后
    //currentTerm + 1
    //改变状态为 Candidate
    //先给自己投票
    rf.mu.Lock()
    defer rf.mu.Unlock()

    rf.currentTerm +=1
    rf.state = Candidate
    rf.votedFor = rf.me
    DPrintf("[%d-%s]: electionTimer time out ,election triggered, ellection at term %d \n", rf.me, rf , rf.currentTerm)
    DPrintf("[%d-%s] turned to  Candiate at term %d", rf.me, rf, rf.currentTerm)
}

func (rf * Raft)turnToFollower(){
    rf.mu.Lock()
    defer rf.mu.Unlock()
    rf.state = Follower
    rf.votedFor = -1
}


func (rf * Raft)canvassVotes() {
    rf.turnToCandidate()

    var receivedVotesCnt int  = 1
    peersLen := len(rf.peers)
    rqVoteargs := &RequestVoteArgs{}

    rf.fillRequestVoteArgs(rqVoteargs)

    replyHandler := func(reply *RequestVoteReply){
        //那么就令 currentTerm 等于 T，并切换状态为跟随者
        rf.mu.Lock()
        defer rf.mu.Unlock()
        DPrintf("[%d-%s]: peer %d replyHandler. at term %d \n", rf.me, rf, rf.me, rf.currentTerm)

        if rf.state == Candidate{
            if reply.Term > rf.currentTerm{
                //变为follower
                rf.turnToFollower()
                rf.resetElection <- struct{}{}
                return
            }

            if reply.VoteGranted {
                if receivedVotesCnt > len(rf.peers)/2{
                //选举成功，变为leader，推出循环激活心跳go 程
                    //这里成为leader 后，因为Leader中发出心跳要等到 HeartBeat定时器到期才会第一发心跳，这时候会耽误自己，
                    //导致自己的选举定时器过期。同时导致其他节点没有收到心跳，过期，任期变大开始新的一轮选举，导致本次选举失效
                    //这里应该在 heartbeatDaemon 中第一时间重置electiontimer 并且发心跳
                    rf.state = Leader
                    go rf.heartbeatDaemon()
                    DPrintf("[%d-%s]: peer %d become new leader. at term %d \n", rf.me, rf, rf.me, rf.currentTerm)
                    return
                }
                receivedVotesCnt += 1
            }
        }
    }

    //这里不能用rf.peers 去得到所有peers的地址，因为for循环加锁
    //后面的 replyHandler 中也去申请锁会死锁
    for idx:=0; idx < peersLen; idx++ {
        if idx != rf.me{
            go func( i int) {
                reply := &RequestVoteReply{}
                //RPC 请求超时的情况
                if rf.sendRequestVote(i, rqVoteargs, reply){
                    replyHandler(reply)
                }else{
                    rf.mu.Lock()
                    DPrintf("[%d-%s]  failed to send sendRequestVote RPC to peer[%d] at term %d", rf.me, rf, i, rf.currentTerm)
                    rf.mu.Unlock()
                }
            }(idx)
        }
    }
}


func (rf *Raft)fillRequestVoteArgs(args * RequestVoteArgs){
    rf.mu.Lock()
    defer rf.mu.Unlock()
    args.Term = rf.currentTerm
    args.CandidateId = rf.me
}

func (rf *Raft)heartbeatDaemon(){
    //这里因为heartbeatDaemon 是守护进程，不可以 defer 释放锁，因为锁会一直被hold住
    // rf.mu.Lock()
    // defer rf.mu.Unlock()

    DPrintf("[%d-%s]: peer start sent  heartbeat at term %d",  rf.me, rf, rf.currentTerm)
    rf.heartbeatTimer = time.NewTimer(1*time.Nanosecond)
    for{
        if _, isLeader := rf.GetState();!isLeader{
            DPrintf("[%d-%s]: peer think he is not leader then return  %d",  rf.me, rf, rf.currentTerm)
            return
        }
        select{
        case  <- rf.done:
            return
        case <- rf.heartbeatTimer.C:
            rf.heartbeatTimer.Reset(rf.heartbeatTimerInterval)
            //leader发送heartbeat前先重置自己的选举超时
            rf.resetElection <- struct{}{}
            for i:=0; i<len(rf.peers); i++{
                if i!= rf.me{
                    go rf.sendHeartbeat(i)
                }
                
            }
            
        }
    }
}

func (rf *Raft)electionDaemon() {
   //rf.electionTimer = time.NewTimer(rf.electionTimeOutDuration)
    //放在外面函数返回后容易造成变量丢失
    go func(){
        DPrintf("[%d-%s]: peer start at term %d",  rf.me, rf, rf.currentTerm)
        for{
            select{
            case <- rf.done:
                return
            case <-rf.resetElection:
                // Reset should be invoked only on stopped or expired timers with drained channels
                if !rf.electionTimer.Stop(){
                    <-rf.electionTimer.C
                }
                rf.electionTimer.Reset(rf.electionTimeOutDuration)
                rf.mu.Lock()
                DPrintf("[%d-%s]: peer reset electionTimer at term %d",  rf.me, rf, rf.currentTerm)
                rf.mu.Unlock()
            case <- rf.electionTimer.C:
                //electionTimer 过期，触发选举
                go rf.canvassVotes()
                rf.electionTimer.Reset(rf.electionTimeOutDuration)
            }
        }
    }()
    
}


//第一步启动初始化为follower，
//判断身份，如果是follower就启动election timeout 机制

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = Follower
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
    rf.resetElection = make(chan interface{})
    // rf.shutdownElection = make(chan struct{})
    // rf.shutdownHeartbeat = make)chan struct{})
    rf.votedFor = -1
    rf.done = make(chan interface{})
    rf.heartbeatTimerInterval = time.Millisecond*100
    rf.electionTimeOutDuration = time.Duration(randTimeOut())*time.Millisecond
    rf.electionTimer = time.NewTimer(rf.electionTimeOutDuration)
	//receive heart beat and reset timer
	//election trigger on
    rf.electionDaemon()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func randTimeOut() int {
    //rand的seed必须不同
    rand.Seed(time.Now().UnixNano())
    randTimeOut := (450 + rand.Intn(150))
    return randTimeOut
}
