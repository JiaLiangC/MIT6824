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
"sort"
"bytes"
"labgob"
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

    commitCond *sync.Cond

    /*******Log Replication Split**********/
    logs[] LogEntry         //
    nextIndex []int         //维护的所有peer的，是leader要发送给其他peer的下一个日志序号。
    matchIndex []int        //是leader收到的其他peer已经确认一致的日志序号。

    commitIndex int     //自己已经提交的日志的索引
    lastApplied int      //

    applyCh chan ApplyMsg


    lastIncludedIndex int 
    lastIncludedTerm int
    // shutdownElection chan struct{}
    // shutdownHeartbeat chan struct{}
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

type LogEntry struct{
    Term int
    Command interface{}
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
    // Your data here (2A, 2B).
    Term        int //候选人的任期号
    CandidateId int //请求投票的候选人 id

    /*----------Replication LOG split------*/
    LastLogIndex int //候选人的最后⽇志条⽬的索引值
    LastLogTerm int //候选人的最后⽇志条⽬的任期
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
    Entries  []LogEntry //日志项，为空则做心跳

    /*----------Replication LOG split------*/
    PrevLogIndex int //要同步的日志的前一个日志的索引，如果没有数据同步，就是最后一个日志索引
    PrevLogTerm int   //要同步的日志的前一个日志的任期
    LeaderCommit int   //领导人已经提交的⽇日志的索引值
}

type AppendEntriesReply struct {
    Term    int //节点的currentTerm
    Success bool
    FirstIndex int
    ConflictTerm int
}


type InstallSnapshotsArgs struct{
    Term    int        //leader 任期号
    LeaderId    int     //leader id,方便follower重定向请求
    LastIncludedIndex    int // 快照中包含的最后的条目的日志索引
    LastIncludedTerm    int //快照中包含的最后的条目的日志任期
    //Offset              int //快照的偏移量
    Data              []byte //数据
    //Done                bool //是否发送完毕
}


type InstallSnapshotsReply struct{
    Term     int        //接受者自己的当前任期号
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    var term int
    var isleader bool
	term = rf.currentTerm
	isleader = rf.state == Leader
    //DPrintf("[%d-%s]: isleader return: %t at term %d \n", rf.me, rf, isleader, rf.currentTerm)
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




func (rf *Raft)subIdx(i int) int{
    return    i - rf.lastIncludedIndex
}


func (rf *Raft)lastIdx() int{
    return rf.lastIncludedIndex+len(rf.logs)-1
}

func (rf *Raft)logLength() int{
    return rf.lastIdx()+1
}

func (rf *Raft)lastTerm() int{
    return rf.logs[len(rf.logs)-1].Term
}



//这个实现中最重要的两个RPC，请求投票RPC 和 心跳RPC。


//返回本节点的最后日志任期和索引
func(rf * Raft) LastLogIndexAndTerm()(int, int){
    //index := len(rf.logs)-1
    //term := rf.logs[index].Term
    index := rf.lastIdx()
    term := rf.lastTerm()
    return index, term
}



//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
    // Your code here (2C).

    // Example:
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)

    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
    e.Encode(rf.logs)
    e.Encode(rf.lastIncludedIndex)
    e.Encode(rf.lastIncludedTerm)
    
    data := w.Bytes()
    rf.persister.SaveRaftState(data)
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
    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)

    d.Decode(& rf.currentTerm)
    d.Decode(& rf.votedFor)
    d.Decode(& rf.logs)

}


func (rf *Raft)InstallSnapshots(args *InstallSnapshotsArgs, reply *InstallSnapshotsReply){
    DPrintf("InstallSnapshots: [%d-%s] receive InstallSnapshots RPC from leader-server[%d-%d]  currentTerm is %d\n",rf.me,rf,args.LeaderId, args.Term, rf.currentTerm)

    select{
    case <- rf.done:
        return
    default:
    }

    rf.mu.Lock()
    defer rf.mu.Unlock()
    DPrintf("InstallSnapshots: [%d-%s] receive InstallSnapshots1 RPC from leader-server[%d-%d]  currentTerm is %d\n",rf.me,rf,args.LeaderId, args.Term, rf.currentTerm)
    reply.Term = rf.currentTerm

    //not leader
    if args.Term < rf.currentTerm{
        DPrintf("InstallSnapshots: [%d-%s] receive RPC  InstallSnapshots,  args.Term <= rf.currentTerm(%d < %d) \n",rf.me,rf,args.Term,rf.currentTerm)
        return
    }


    rf.resetElection <- struct{}{}

    //过滤旧的快照请求，这一步很重要
    if args.LastIncludedIndex <= rf.lastIncludedIndex{
        DPrintf("InstallSnapshots: [%d-%s] receive RPC  InstallSnapshots,  args.LastIncludedIndex <= rf.snapshotIndex(%d < %d) \n",rf.me,rf,args.LastIncludedIndex,rf.lastIncludedIndex)
        return
    }

    // Index       int
    // Command     interface{}
    // UseSnapshot bool   // ignore for lab2; only used in lab3
    // Snapshot    []byte

    //apply log
    //如果现存的⽇志条⽬与快照中最后包含的日志条⽬具有相同的索引值和任期号，则保留其后的日志条⽬并进行回复
    if args.LastIncludedIndex < rf.lastIdx(){
        rf.logs = rf.logs[args.LastIncludedIndex-rf.lastIncludedIndex:]
    }else{
        rf.logs = []LogEntry{{Term: args.LastIncludedTerm, Command: nil}}
    }

    rf.lastIncludedIndex = args.LastIncludedIndex
    rf.lastIncludedTerm = args.LastIncludedTerm

    rf.commitIndex = args.LastIncludedIndex
    rf.lastApplied = args.LastIncludedIndex


    DPrintf("[%d-%s]:InstallSnapshots args.LastIncludedIndex : rf.snapshotIndex(%d : %d) \n",rf.me,rf,args.LastIncludedIndex,rf.lastIncludedIndex)

    rf.persistStateAndSnapshot(args.Data)

    rf.applyCh <- ApplyMsg{
        CommandValid: false,//not command but a kvSnapshot data
        CommandIndex: rf.lastIncludedIndex,
        Command: args.Data, // kv 
    }

}

func max(a int, b int) int{
    if a > b{
        return a
    }
    return b
}

func (rf *Raft)persistStateAndSnapshot(kvSnapshotData []byte){
    rfSnapShotData := rf.NewSnapshot()
    rf.persister.SaveStateAndSnapshot(rfSnapShotData, kvSnapshotData)
    DPrintf("[%d-%s]:persistStateAndSnapshot finished.  rf.logs size:%d  rf.lastIncludedIndex: %d rf.lastIncludedTerm: %d , rf.RaftStateSize:%d \n",rf.me,rf,len(rf.logs),rf.lastIncludedIndex,rf.lastIncludedTerm, rf.persister.RaftStateSize())
}


//Raft 收到KV Server生成快照的请求后更新完自己的快照信息就立刻返回，不要阻塞，快照同步交给心跳去处理：
func (rf *Raft)TakeSnapshot(index int, kvSnapshotData []byte){
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.commitIndex < index || index <= rf.lastIncludedIndex{
        panic("TakeSnapshot: new snapshots index <= old snapshots index")
    }

    //过滤老的snapshot请求
    // if index <= rf.lastIncludedIndex{
    //     return
    // }
    DPrintf("[%d-%s]:TakeSnapshot start at index: %d  rf.logs size:%d  rf.lastIncludedIndex: %d rf.lastIncludedTerm: %d , rf.RaftStateSize:%d, rf.lastIndex:%v \n",rf.me,rf, index, len(rf.logs), rf.lastIncludedIndex, rf.lastIncludedTerm, rf.persister.RaftStateSize(), rf.logs[len(rf.logs)-1])

    rf.logs = rf.logs[rf.subIdx(index):]
    rf.lastIncludedIndex = index
    rf.lastIncludedTerm = rf.logs[0].Term

    rf.persistStateAndSnapshot(kvSnapshotData)

    DPrintf("[%d-%s]:TakeSnapshot finished.  rf.logs size:%d  rf.lastIncludedIndex: %d rf.lastIncludedTerm: %d , rf.RaftStateSize:%d rf.logs is: %v \n",rf.me,rf,len(rf.logs),rf.lastIncludedIndex,rf.lastIncludedTerm, rf.persister.RaftStateSize(),rf.logs)

}


func (rf *Raft)NewSnapshot() []byte{
    //删除旧的日志
    //index-rf.lastIncludedIndex
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
    e.Encode(rf.logs)
    e.Encode(rf.lastIncludedIndex)
    e.Encode(rf.lastIncludedTerm)
    data := w.Bytes()

    DPrintf("[%d-%s]:NewSnapshot finished.  rf.logs size:%d, data size:%d,  rf.lastIncludedIndex: %d rf.lastIncludedTerm: %d , rf.RaftStateSize:%d \n",rf.me,rf,len(rf.logs),len(data),rf.lastIncludedIndex,rf.lastIncludedTerm, rf.persister.RaftStateSize())

    return data
}


func (rf * Raft)sendSnapshot(server int){
    //有问题
    DPrintf("sendSnapshot: [%d-%s] start  send snapshots to server[%d]  currentTerm is %d\n",rf.me,rf,server,rf.currentTerm)
    installSnapshotArgs := &InstallSnapshotsArgs{
        Term:    rf.currentTerm,  //leader 任期号
        LeaderId:    rf.me,     //leader id,方便follower重定向请求
        LastIncludedIndex:    rf.lastIncludedIndex, // 快照中包含的最后的条目的日志索引
        LastIncludedTerm:    rf.lastIncludedTerm, //快照中包含的最后的条目的日志任期
        Data:              rf.persister.ReadSnapshot(), //数据
    }

    replyHandler := func(server int, reply *InstallSnapshotsReply){
        DPrintf("sendSnapshot: [%d-%s] receive InstallSnapshotsRPC response from server[%d]  currentTerm is %d\n",rf.me,rf,server,rf.currentTerm)
        rf.mu.Lock()
        defer rf.mu.Unlock()

        if rf.state == Leader{
            if reply.Term > rf.currentTerm{
                rf.currentTerm = reply.Term
                rf.turnToFollower()
                DPrintf("sendSnapshot: [%d-%s]  InstallSnapshotsRPC_Response  from server[%d],  reply.Term > rf.currentTerm (%d > %d) \n",rf.me,rf,server,reply.Term ,rf.currentTerm)
                return
            }

            rf.matchIndex[server] = rf.lastIncludedIndex
            rf.nextIndex[server] = rf.lastIncludedIndex+1

            DPrintf("sendSnapshot: [%d-%s]  InstallSnapshotsRPC_Response  from server[%d],  rf.nextIndex[%d]:%d   , \n",rf.me,rf,server,server,rf.nextIndex[server])
        }
    }

    go func() {
        var reply InstallSnapshotsReply
        if rf.sendInstallSnapshots(server,installSnapshotArgs,&reply){
            replyHandler(server, &reply)
        }
    }()

}




func (rf * Raft)synSnapshot(serverId int){
    DPrintf("synSnapshot: [%d-%s] start  sync to server[%d]  currentTerm is %d\n",rf.me,rf,serverId,rf.currentTerm)
    installSnapshotArgs := InstallSnapshotsArgs{
        Term:    rf.currentTerm,  //leader 任期号
        LeaderId:    rf.me,     //leader id,方便follower重定向请求
        LastIncludedIndex:    rf.lastIncludedIndex, // 快照中包含的最后的条目的日志索引
        LastIncludedTerm:    rf.lastIncludedTerm, //快照中包含的最后的条目的日志任期
        //Offset:              0, //快照的偏移量
        Data:              rf.persister.ReadSnapshot(), //数据
        //Done:                true, //是否发送完毕
    }
    installSnapshotsReply := InstallSnapshotsReply{}

    installSnapshotReplyHandler := func(serverId int, reply *InstallSnapshotsReply){
        rf.mu.Lock()
        defer rf.mu.Unlock()

         //TODO 这里这个判断是否有必要 reply.Term < rf.currentTerm 
        if rf.state!=Leader || reply.Term < rf.currentTerm {
            return
        }
        //term expired loose leader
        if reply.Term > rf.currentTerm{
            rf.currentTerm = reply.Term

            rf.turnToFollower()
            DPrintf("synSnapshot: [%d-%s]:   reply term: %d bigger than  rf's term: %d  turns to follower\n", rf.me, rf,  reply.Term, rf.currentTerm)
            return
        }

        rf.matchIndex[serverId] = rf.lastIncludedIndex
        rf.nextIndex[serverId] = rf.lastIncludedIndex+1
    }

    go func() {
        respChan :=  make(chan struct{})
        go func() {
            if rf.sendInstallSnapshots(serverId, &installSnapshotArgs, &installSnapshotsReply) {
                respChan <- struct{}{}
            }
        }()

        select{
            case <-time.After(300*time.Millisecond):
                return
            case <-respChan:
                close(respChan)
        }
        installSnapshotReplyHandler(serverId, &installSnapshotsReply)
        
    }()
}

// example RequestVote RPC handler.
//请求投票RPC的实现函数,收到请求投票RPC后，就执行这个函数
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply){
    select{
    case <-rf.done:
        return
    default:
    }
	// Your code here (2A, 2B).

	//接受者需要实现：
    
    //1.用VotedFor作为投票标志位，实现一个任期只投票一次

	//2.如果候选人的 term < currentTerm 就返回false 拒绝投票

	//如果votedFor为空或者与candidateId相同，并且候选人的日志和自己的日志一样新，则给该候选人投票（5.2 节 和 5.4 节）
    

    //返回投票者自己的当前任期号，是否投票

    //一个任期开始后先把VotedFor重置为空
    //这里任期相等时不可以重置，因为可能是别的候选者发来的请求，
    //此时任期已经被第一个候选者的请求重置过了


    rf.mu.Lock()
    defer rf.mu.Unlock()
    lastLogIndex, lastLogTerm := rf.LastLogIndexAndTerm()

    //请求者任期小于自己的任期，拒绝投票
	if args.Term < rf.currentTerm {
        reply.VoteGranted = false
        reply.Term = rf.currentTerm
    }else {
        //如果任期比自己大就给投票，并且身份转变为follower,然后重置投票信息，voteFor.(因为candidate 发起投票前任期都会自加1,所以不用担心任期相同的情况)

        if args.Term > rf.currentTerm {
            DPrintf("candidate.Term > rf.currentTerm: [%d-%s] Candidate's Term is  %d.  rf's currentTerm is %d  \n",rf.me, rf, args.Term, rf.currentTerm)
            rf.state = Follower
            //这一步更新任期，避免了给任期比自己大的候选者重复投票
            rf.currentTerm = args.Term
            rf.votedFor = -1
            DPrintf("[%d-%s] turned into follower  \n",rf.me,rf)
        }

        //这里通过前面更新任期，以及设置votedFor 避免重复投票， 但是考虑到有两个时间段，A1,A2 A1>A2 ,造成网络隔离导致两台机器成为candidate并且任期递增，一旦恢复网络
        //那么还是会出现重复投票的现象，但是因为任期都比自己大，每次投票时更新了任期，还是做到了一个任期只投票一次的原则。
        if  (rf.votedFor == -1) {
            //只给最后最后一条日志任期比我大的 或者 任期至少和我相同，并且日志索引大于等于我的 candidate 投票
            if (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) ||  args.LastLogTerm > lastLogTerm {
                //发信号重置选举超时的定时器
                rf.resetElection <-struct{}{}
                rf.votedFor = args.CandidateId
                rf.state = Follower

                reply.VoteGranted = true
                DPrintf("[%d-%s]: votedFor peer[%d] RPC call at term %d \n", rf.me, rf, args.CandidateId, rf.currentTerm)
            }else{
                DPrintf("votedRefused [%d-%s]:  lastLogIndex: %d | lastLogTerm: %d ||  CandidateId: %d |  args.LastLogTerm:%d |----args.LastLogIndex:%d | ",rf.me, rf, lastLogIndex, lastLogTerm, args.CandidateId, args.LastLogTerm, args.LastLogIndex)
            }
        }
    }

    //这里为啥必须要persist
    rf.persist()
}




//全项目最重要的一个RPC，包括心跳，以及日志同步的实现
//AppendEntries 可以用来做心跳服务，表示心跳时entries为空
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){

	//如果 entries 为空，视为心跳重置选举过期时间
	//leader的任期必须大于跟随者
    //收到AppendEntries ，当entries为空就重置 election timeout

    //查询关闭信号,一旦 rf.done 有值，并且收到一个心跳就会关闭

    DPrintf("[%d-%s]:AppendEntries  received AppendEntries,args.Term:%d turns to follower\n", rf.me, rf,  args.Term)

    select{
        case <-rf.done:
            return
        default:
    }

    rf.mu.Lock()
    defer rf.mu.Unlock()

    //如果对方任期比自己小，就拒绝
    if args.Term < rf.currentTerm{
        reply.Success = false
        reply.Term = rf.currentTerm
        DPrintf("[%d-%s]:AppendEntries refuesed! args.Term < rf.currentTerm(%d < %d)  \n", rf.me, rf,  args.Term,rf.currentTerm)
        return
    }

    //DPrintf("[%d-%s]:  receive heartbeat from peer[%d] at term %d.  Entries: %+v", rf.me, rf, args.LeaderId, rf.currentTerm, args.Entries)

    //这里如果把state 判断放在前面就会被错误的请求给重置身份
    //所以放在过滤错误请求后
    //why does this 

    //BUGFIX 这里因为每次发送心跳时没有过滤自己，导致发送给了自己
    //导致自己被重置为Follower,
    //发送心跳前会手动重置Leader 的 election reset


    //收到的任期大于自己就更新自己任期
    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
    }

    if  rf.state == Leader  {
        DPrintf("[%d-%s]:AppendEntries leader received AppendEntries,args.Term:%d turns to follower\n", rf.me, rf,  args.Term)
        rf.turnToFollower()
    }

    //这里leader 不会发送心跳给自己，除非是其他的成员给自己发送心跳，并且任期比自己大，就转变为 Follower
    //候选者收到任期大于等于自己leader的心跳，就承认，并且转为follower

    //重置过期的leader
  

    //如果投票标记不是当前的leader，就改为leader_id
    if rf.votedFor != args.LeaderId {
        rf.votedFor = args.LeaderId
    }

    //重置自己的选举超时
    rf.resetElection <- struct{}{}


    if args.PrevLogIndex < rf.lastIncludedIndex{
        reply.Success = false
        reply.FirstIndex = rf.lastIncludedIndex 
        reply.ConflictTerm = rf.lastIncludedTerm
        DPrintf("[%d-%s]:AppendEntries refuesed! args.PrevLogIndex < rf.lastIncludedIndex(%d < %d)  \n", rf.me, rf,  args.PrevLogIndex,rf.lastIncludedIndex)
        return
    }

    //接受者最后一个log的索引和任期
    // rfpreLogidx, rfpreLogTerm := 0, 0
    // 如果leader的索引小于等于自己就绝对服从，取最后匹配的索引和Term
    //leadr会一致减小 prevLogIndex重试，直到索引比对方小，且任期匹配

    //日志处理的逻辑，leader日志比自己短，并且一致性对的上(leader同步的日志的前一个任期和我对应leader该任期索引出处的任期一致)，就截取自己并放leader的发来的日志。
    //如果leader短，并且一致性对不上，那就从leader 发来的prelog的索引处开始在自己的日志中向前回退搜索，一直到找到leader的prelogIndex处自己的任期的第一个索引


    //如果leader 的 日志比自己的短， 这里为啥不是 len(rf.logs -1)还有待学习
    //1<=1，第一次日志的时候 logLength
    //if args.PrevLogIndex < len(rf.logs){
    if args.PrevLogIndex < rf.logLength(){
        DPrintf("[%d-%s]:AppendEntries receive log at term %d，args.PrevLogIndex： %d, args.PrevLogTerm: %d, %d", rf.me, rf, rf.currentTerm, args.PrevLogIndex,args.PrevLogTerm, rf.logs[rf.subIdx(args.PrevLogIndex)].Term)

        //，args.PrevLogIndex 3  args.PrevLogTerm 10, rf.logs[args.PrevLogIndex].Term 10 8


        //如果leader的日志短，并且leader的最后一条日志的索引和任期都和己方对的上，那就按照leader的日志删去多余的自己的日志
        //if args.PrevLogTerm == rf.logs[args.PrevLogIndex].Term{
        if args.PrevLogTerm == rf.logs[rf.subIdx(args.PrevLogIndex)].Term{    

            reply.Success = true
            //go 切片规则是不包含最后一个，所以要+1 
            //s := arr[startIndex:endIndex]， 将arr中从下标startIndex到endIndex-1 下的元素创建为一个新的切片

            //rf.logs = rf.logs[:args.PrevLogIndex+1]
            rf.logs = rf.logs[:rf.subIdx(args.PrevLogIndex)+1]
            rf.logs = append(rf.logs, args.Entries...)
            //DPrintf("[%d-%s]: rf.logs is  %+v",rf.me, rf, rf.logs)

            //如果领导人的commitIndex(最后一个提交的日志的索引)大于自己的，那么说明日志更新正常，然后更新自己的索引
            //更新为 leader的  args.LeaderCommit 和 follower的最新的索引中的比较小的那一个
            if  args.LeaderCommit > rf.commitIndex{
                //rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
                rf.commitIndex = min(args.LeaderCommit, rf.lastIdx())
                go func(){ rf.commitCond.Broadcast()}()
                //通知应用Apply
            }

            //reply.ConflictTerm = rf.logs[len(rf.logs)-1].Term
            //reply.FirstIndex = len(rf.logs)-1
            reply.ConflictTerm = rf.lastTerm()
            reply.FirstIndex = rf.lastIdx()

            DPrintf("[%d-%s]:AppendEntries:success receive log at term %d，args.PrevLogIndex： %d, args.PrevLogTerm: %d, %d, reply.ConflictTerm:%d,  reply.FirstIndex:%d", rf.me, rf, rf.currentTerm, args.PrevLogIndex,args.PrevLogTerm, rf.logs[rf.subIdx(args.PrevLogIndex)].Term,reply.ConflictTerm , reply.FirstIndex)

            rf.persist()

        }else{
            reply.Success = false
            //找到第一个冲突的条目,就是找到 leader的索引 所在的follower 的索引的的任期中最小的索引，然后返回这个索引和任期
            //lastConsistTerm := rf.logs[args.PrevLogIndex].Term
            lastConsistTerm := rf.logs[rf.subIdx(args.PrevLogIndex)].Term
            lastConsistIdx := 1

            //for i := args.PrevLogIndex-1; i>0; i-- {
            for i := args.PrevLogIndex-1; i>rf.lastIncludedIndex; i-- {
                //if  rf.logs[i].Term != lastConsistTerm{
                if  rf.logs[rf.subIdx(i)].Term != lastConsistTerm{
                //如果发现任期不同，就说明到头了，迭代到了上一个任期，然后返回上个索引
                    lastConsistIdx = i+1
                    break
                }
            }
            reply.FirstIndex = lastConsistIdx 
            reply.ConflictTerm = lastConsistTerm
        }
    }else{

       //如果leader的日志比我长，那我直接发送最后一个日志索引和任期，让leader去找最后日志一致的地方，leader会找到最后一致的任期，更新next_index 然后从那个地方重新开始同步日志过来。
        reply.Success = false

        //reply.FirstIndex = len(rf.logs)
        reply.FirstIndex = rf.lastIdx()
        //reply.ConflictTerm = rf.logs[len(rf.logs)-1].Term
        reply.ConflictTerm = rf.lastTerm()
    }
    
    DPrintf("[%d-%s]:  send reset ElectionTimer signal at heartbeat at term %d", rf.me, rf, rf.currentTerm)
}




//这个方法就是最终命令达成一致后应用到状态机
//1. 如果 commitIndex > lastApplied ，那么就 lastApplied 加一，并把 log[lastApplied] 应⽤用到状态机中
func (rf *Raft)ApplyLogEntryDaemon(){
    // commitIndex int 
    // lastApplied int
    for{
        var sendLogs []LogEntry

        rf.mu.Lock()
        for rf.lastApplied == rf.commitIndex{
            rf.commitCond.Wait()
            select{
            case <- rf.done:
                rf.mu.Unlock()
                close(rf.applyCh)
                return
            default:
            }
        }
        DPrintf("[%d-%s]:  in  ApplyLogEntryDaemon", rf.me, rf)

    //lastApplied < commitIndex , 更新 lastApplied = commitIndex  然后组装 ApplyMsg 信息，发送如 ApplyCh 通道，
    //因为applyMsg 的command 是单个命令，所以要根据日志长度发送多次，这里具体发送几次为 commitIndex-lastApplied 次数 


        //得到发送长度，拿到要发送到日志，更新lastApplied
        sendLen:=0
        lastIdx := rf.lastApplied
        if rf.lastApplied < rf.commitIndex{
            sendLen = rf.commitIndex - rf.lastApplied
            rf.lastApplied = rf.commitIndex
            sendLogs = make([]LogEntry, sendLen)

            //copy(sendLogs, rf.logs[lastIdx+1 : rf.commitIndex+1])
            copy(sendLogs, rf.logs[rf.subIdx(lastIdx+1) : rf.subIdx(rf.commitIndex+1)])
            //DPrintf("[%d-%s]:  in  ApplyLogEntryDaemon sendLogs is %+v ", rf.me, rf, sendLogs)
        }
        rf.mu.Unlock()

        for i:=0; i< sendLen;i++{
            applyMsg := ApplyMsg{
                CommandValid: true,
                Command: sendLogs[i].Command,
                CommandIndex: i + lastIdx + 1,
            }
            rf.applyCh <- applyMsg
            DPrintf("[%d-%s]:  send applyMsg to  applyCh  at term %d, applyMsg: %+v", rf.me, rf, rf.currentTerm, applyMsg)
            //rf.persist()
        }
    }

}

func (rf *Raft)checkConsistency(args *AppendEntriesArgs){
    //  prevLogIndex int //要同步的日志的前一个日志的索引，如果没有数据同步，就是最后一个日志索引
    // prevLogTerm int   //要同步的日志的前一个日志的任期
    // LeaderCommit int   //领导人已经提交的⽇日志的索引值
    // if args.prevLogIndex
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


func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}




func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshots(server int, args *InstallSnapshotsArgs, reply *InstallSnapshotsReply) bool{
    ok := rf.peers[server].Call("Raft.InstallSnapshots", args, reply)
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


//leader 节点接受命令放入自己日志，并更新自己的nextIndex，和matchIndex
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := 0
	isLeader := false

	// Your code here (2B).
    select{
    case <- rf.done:
        return index, term, isLeader
    default:
        rf.mu.Lock()
        defer rf.mu.Unlock()
        if rf.state == Leader{
            log := LogEntry{rf.currentTerm, command}
            rf.logs = append(rf.logs,log)

            index = len(rf.logs)-1
            term = rf.currentTerm
            isLeader = true
            rf.nextIndex[rf.me] = index+1
            rf.matchIndex[rf.me] = index
            
            //这里是否有rf.persist的必要性，persist 是否能够保证正确的恢复日志，保证日志一致性
            rf.persist()
        }

    }
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
    rf.done <- struct{}{}
    //Kill all
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
    DPrintf("[%d-%s]  start  send Heartbeat to  peers[%d] at term %d", rf.me, rf, id, rf.currentTerm)


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

    if rf.state != Leader{
        DPrintf("sendHeartbeat:[%d-%s]: peer think he is not leader then return  %d",  rf.me, rf, rf.currentTerm)
        return
    }
    
    //DPrintf("sendHeartbeat: [%d-%s] send Heartbeat to  peers[%d] at term %d rf.nextIndex[id] is : %d",rf.me, rf, id, rf.currentTerm, rf.nextIndex[id])
   

    //BUG  rf.nextIndex[id] = 0, 所以 rf.nextIndex[id]-1 会出现0-1的错误，这里的日志长度莫名的变为0 很奇怪

    //因为nextIndex[serverId] 存的下一条要发的日志的索引，既当前日志加1, 所以个快照比时要减一
    pre := rf.nextIndex[id]-1

    //当发现快照比 next
    if pre < rf.lastIncludedIndex {
        //rf.synSnapshot(id)
        DPrintf("sendHeartbeat:[%d-%s]:will sendSnapshot, cause pre < rf.lastIncludedIndex(%d < %d)  %d",  rf.me, rf,pre, rf.lastIncludedIndex)
        rf.sendSnapshot(id)
    }else{
        var args = AppendEntriesArgs{
            Term:         rf.currentTerm,
            LeaderId:     rf.me,
            PrevLogIndex: pre,
            PrevLogTerm:  rf.logs[rf.subIdx(pre)].Term,
            Entries:      nil,
            LeaderCommit: rf.commitIndex,
        }
        //这里send 发送心跳应该用go 程去做，不然网络卡顿，会引起自己这边的超时
        //ok := rf.sendAppendEntries(id, args, reply)

        //BUG fix
        //if rf.nextIndex[id] < len(rf.logs)
        if rf.nextIndex[id] < len(rf.logs)+rf.lastIncludedIndex{
            DPrintf("[%d-%s]send Heartbeat to peers[%d] at term %d rf.nextIndex[id]<rf.lastIdx()+1:%d < %d", rf.me, rf, id, rf.currentTerm, rf.nextIndex[id], len(rf.logs)+rf.lastIncludedIndex)
            args.Entries = append(args.Entries, rf.logs[rf.subIdx(rf.nextIndex[id]):]...)
        }

        go func() {
            reply := &AppendEntriesReply{}
            if rf.sendAppendEntries(id, &args, reply){
                DPrintf("[%d-%s]    send Heartbeat to  peers[%d] at term %d success", rf.me, rf, id, rf.currentTerm)
                rf.checkConsistencyByReply(id,reply)
            }else{
                DPrintf("[%d-%s] failed to send Heartbeat to  peers[%d] at term %d", rf.me, rf, id, rf.currentTerm)
            }
        }()
    }
}


// 如果存在一个满⾜N > commitIndex 的 N，并且⼤多数的 matchIndex[i] ≥ N 成⽴，
// 并且 log[N].term == currentTerm 成立，那么令 commitIndex 等于这 个 N (5.3 和 5.4 节)

// matchIndex集合中的多数值都大于N —— 意味着，多数follower都已经收到了logs[0,N]条目并应用到了本地状态机中；
// 并且，logs[N].term等于当前节点（leader）的currentTerm —— 意味着，这条日志是当前任期产生而不是其他leader的任期同步过来的；
// 那么，可以将leader节点的commitIndex设置为N
func(rf *Raft)updateCommitIndex(){
    //DPrintf("updateCommitIndex:[%d-%s]: leader started update commit index  \n",)
    copyMacthIndex := make([]int, len(rf.matchIndex))
    copy(copyMacthIndex, rf.matchIndex)
    //默认都是从小到大排序  把所有server matchIndex 从小到大排序，比如 1 3 5 6 7，如果 matchIndex 多数值都大于中位数5
    //说明多数follower都收到了 logs[0-5]的日志并且应用到了本地状态机中
    sort.Ints(copyMacthIndex)

    //因为是从小到大排序，所以只要大于中位值，就相当于大于一半的节点的machIndex
    target := copyMacthIndex[len(rf.peers)/2]
    
    DPrintf("updateCommitIndex:[%d-%s]: leader start updated commitedIndex: %d \n", rf.me, rf, target)

    //if rf.commitIndex < target  && target>0{
    if rf.commitIndex < target  && target>rf.lastIncludedIndex{

        if rf.logs[target].Term == rf.currentTerm{
            rf.commitIndex = target
            DPrintf("updateCommitIndex:[%d-%s]: leader success updated commitedIndex: %d \n", rf.me, rf, target)
            go func() { rf.commitCond.Broadcast() }()
        }else{
            DPrintf("[%d-%s]: leader %d update commit index %d failed (log term %d != current Term %d)\n",
                    rf.me, rf, rf.me, rf.commitIndex, rf.logs[target], rf.currentTerm)
        }
    }

}


// 发出心跳请求 AppendEntry后，检查相应信息，检查日志是否同步
func (rf *Raft)checkConsistencyByReply(id int,reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.state != Leader{
        return
    }

    if reply.Success{
        // RPC and consistency check successful
         //DPrintf("[%d-%s] receive Heartbeat reply update matchIndex, reply.FirstIndex %d",rf.me, rf,  reply.FirstIndex)
        //日志发送成功，那么leader 更新已经同步的list matchIndex 中对应的值为，follower返回的其最新的日志的index
        //更新对应的nextIndex 为follower最新的日志索引值+1

        rf.matchIndex[id] = reply.FirstIndex
        rf.nextIndex[id] = rf.matchIndex[id] +1
        DPrintf("[%d-%s]:checkConsistencyByReply for server[%d] rf.nextIndex[%d]:%d, rf.matchIndex[id]:%d \n", rf.me, rf, id, id,  rf.nextIndex[id], rf.matchIndex[id] )
        rf.updateCommitIndex()
    }else{
       
        //如果自己还是leader，但是任期小于对方的任期，那就转变为follower
        if rf.state==Leader && reply.Term > rf.currentTerm{
            DPrintf("[%d-%s] send Heartbeat to  peers[%d] at term failed %d, find new leader and turned to follower", rf.me, rf, id, rf.currentTerm)
            rf.turnToFollower()
            rf.resetElection <- struct{}{}
            rf.persist()
            return
        }

        //这里可以分为2种情况，
        //第一种对方的任期比leader大，日志比leader多，那么在成员接收端验证日志一致性后，直接截取掉多余日志，更新成功
        //如果验证失败，说明中间有任期不对的地方，就发回自己的最后一条日志的索引和任期，leader接受到后如果可以找到就更新 该成员的next_index,然后下次心跳leader从此处开始发送日志
        //如果成员匹配上就会自动截取掉多余日志，如果匹配不上，重复这个过程，此时next_index会一直减小，直到为0，如果第一条日志都不同的话，那就会删除成员的全部日志

        //第二种对方的日志比leader少，那么leader需要找到他们两个最后一个日志一致的点开始发送日志。此时对方返回其最后一条日志的索引和任期
        //然后leader开始哭哈哈的寻找他们最后一次任期对的上的点，然后从那个点的自己的索引和对方的索引找出一个较小值，从那个索引开始发送日志
        //重复这个过程直到找到一致的日志为止

        rf.nextIndex[id] = reply.FirstIndex

        if reply.ConflictTerm !=0 {
            for i := len(rf.logs)-1; i>0; i--{
                if rf.logs[i].Term == reply.ConflictTerm{
                    rf.nextIndex[id] = min(reply.FirstIndex, i)
                    break
                }
            }
        }

        if rf.lastIncludedIndex!=0 && rf.nextIndex[id] <= rf.lastIncludedIndex{
            DPrintf("[%d-%s]: peer[%d] needs snapshots, rf.nextIndex<=rf.lastIncludedIndex( %d < %d) \n", rf.me, rf , id, rf.nextIndex[id], rf.lastIncludedIndex)
            //rf.synSnapshot(id)
            rf.sendSnapshot(id)
        }else{
            rf.nextIndex[id] = min(max(rf.nextIndex[id],1+rf.lastIncludedIndex), len(rf.logs)+rf.lastIncludedIndex)
            DPrintf("[%d-%s]: nextIndex for  peer[%d]: rf.nextIndex:%d rf.lastIncludedIndex( %d ) \n", rf.me, rf , id, rf.nextIndex[id], rf.lastIncludedIndex)

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
    DPrintf("[%d-%s]: electionTimer time out ,election triggered, start canvass, ellection at term %d \n", rf.me, rf , rf.currentTerm)
}


//调用它的函数中有rf的锁，所以这里为了避免死锁就去掉
//调用这个函数的时候保证范围内有锁
func (rf * Raft)turnToFollower(){
    //rf.mu.Lock()
    //defer rf.mu.Unlock()
    rf.state = Follower
    rf.votedFor = -1
}



//选举进程
func (rf * Raft)canvassVotes() {

    DPrintf("[%d-%s]:canvassVotes  currentTerm:%d  \n", rf.me, rf , rf.currentTerm)

    //转变状态为候选者，任期+1，给自己投票
    rf.turnToCandidate()

    var receivedVotesCnt int  = 1
    peersLen := len(rf.peers)
    
    rqVoteargs := &RequestVoteArgs{}

    //填充请求投票的参数
    rf.fillRequestVoteArgs(rqVoteargs)

    //处理投票RPC 返回的结果
    replyHandler := func(reply *RequestVoteReply){
        //那么就令 currentTerm 等于 T，并切换状态为跟随者
        rf.mu.Lock()
        defer rf.mu.Unlock()

        if rf.state == Candidate{
            if reply.Term > rqVoteargs.Term{
                //选举时如果对方的任期比自己大就返回
                rf.currentTerm = reply.Term
            // if reply.Term > rf.currentTerm{
                //如果收到的请求投票RPC 对方返回的Term大于自己的Term ，说明自己过期了，转变为follower

                //如果在turnToFollower里再加锁，这里会变为死锁，因为上面已经加锁了
                rf.turnToFollower()
                rf.persist()

                //重置选举时间，返回函数
                rf.resetElection <- struct{}{}
                DPrintf("canvassVotes: [%d-%s]:   reply term: %d bigger then  rf's term: %d  turns to follower\n", rf.me, rf,  reply.Term, rf.currentTerm)
                return
            }

            //如果对方返回的任期小于等于自己
            if reply.VoteGranted {
                //if receivedVotesCnt > len(rf.peers)/2{
                //这里原来写的是大于，如果只有3个节点，会出现获得1票无法胜出
                if receivedVotesCnt == len(rf.peers)/2{
                //选举成功，变为leader，退出循环激活心跳go程

                    //这里成为leader 后，因为Leader中发出心跳要等到 HeartBeat定时器到期才会发第一次心跳，这时候会耽误自己，
                    //导致自己的选举定时器过期。同时导致其他节点没有收到心跳，过期。任期变大开始新的一轮选举，导致本次选举失效。
                    //所以这里应该在 heartbeatDaemon 中第一时间重置electiontimer 并且发心跳
                    rf.state = Leader

                    //选举成功后，设置nextIndex 和 matchedIndex
                    rf.resetAfterElectionSuccess()
                    DPrintf("[%d-%s]: peer %d become new leader. at term %d \n", rf.me, rf, rf.me, rf.currentTerm)
                    
                    //开启leader的心跳守护进程，开始给follower 发送心跳
                    go rf.heartbeatDaemon()
                    return
                }
                receivedVotesCnt += 1
            }
        }
    }


    //给集群的所有角色开始发送请求投票

    //重复加锁BUG，这里不能用rf.peers 去得到所有peers的地址，因为for循环加锁
    //后面的 replyHandler中也去申请锁会死锁
    for idx:=0; idx < peersLen; idx++ {
        if idx != rf.me{
            
            //启动一个go 线程专门发送请求投票RPC
            go func( i int) {
                reply := &RequestVoteReply{}
                //RPC 请求超时的情况
                if rf.sendRequestVote(i, rqVoteargs, reply){
                    DPrintf("[%d-%s]   send sendRequestVote RPC to peer[%d] at term %d successfully", rf.me, rf, i, rf.currentTerm)
                    //处理投票RPC返回的结果
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


func (rf *Raft)resetAfterElectionSuccess(){
    if rf.state == Leader{
        for i:=0; i< len(rf.peers); i++ {
            //rf.nextIndex[i] = len(rf.logs)
            rf.nextIndex[i] = rf.logLength()
            rf.matchIndex[i] = 0
            if i == rf.me{
                //完全认可Leader的日志
                //rf.matchIndex[i] = len(rf.logs)-1
                rf.matchIndex[i] = rf.lastIdx()
            }
        }
        
    }

}

func (rf *Raft)fillRequestVoteArgs(args * RequestVoteArgs){
    rf.mu.Lock()
    defer rf.mu.Unlock()
    args.Term = rf.currentTerm
    args.CandidateId = rf.me
    args.LastLogIndex , args.LastLogTerm =  rf.LastLogIndexAndTerm()
}

func (rf *Raft)heartbeatDaemon(){
    //这里因为heartbeatDaemon 是守护进程，不可以 defer 释放锁，因为锁会一直被hold住

    for{
        //每次开始前检测一下自己还是不是leader，因为网络等原因可能重新触发选举，导致自己的任期过期，在选举守护进程中被重置为follower
        if _, isLeader := rf.GetState();!isLeader{
            DPrintf("[%d-%s]: peer think he is not leader then return  %d",  rf.me, rf, rf.currentTerm)
            return
        }

        rf.resetElection <- struct{}{}

        select{
        case  <- rf.done:
            return
        default:

            //开始挨个给follower发送心跳，这里必须用 go 程，因为是同步的RPC请求，
            //此时某一个follower可能因为网络原因返回慢，导致后续的follower不能及时收到心跳，导致选举定时器过期
            for i:=0; i<len(rf.peers) ;i++{
                if i!= rf.me{
                    go rf.sendHeartbeat(i)
                }
                
            }
            
        }
        time.Sleep(rf.heartbeatTimerInterval)
    }
}



//选举守护进程，当选举定时器到期后，开始选举，收到心跳信号后，重置选举定时器
func (rf *Raft)electionDaemon() {
   //rf.electionTimer = time.NewTimer(rf.electionTimeOutDuration)
    //放在外面函数返回后容易造成变量丢失
    rf.mu.Lock()
    DPrintf("[%d-%s]: peer start at term %d",  rf.me, rf, rf.currentTerm)
    rf.mu.Unlock()
    for{
        select{
        case <- rf.done:
            return
        case <- rf.resetElection:
            //收到信号重置选举
            // Reset should be invoked only on stopped or expired timers with drained channels
            if !rf.electionTimer.Stop(){
                <-rf.electionTimer.C
            }
            rf.electionTimer.Reset(rf.electionTimeOutDuration)
            //DPrintf("[%d-%s]: peer reset electionTimer at term %d",  rf.me, rf, rf.currentTerm)
        case <- rf.electionTimer.C:
            //electionTimer 过期，触发选举
            DPrintf("[%d-%s]election timeout, issue election @ term %d\n",
            rf.me, rf, rf.currentTerm)
            //开始选举
            go rf.canvassVotes()
            //开始选举后，重置选举定时器
            rf.electionTimer.Reset(rf.electionTimeOutDuration)
        }
    }
    
}



//第一步启动初始化为follower，
//判断身份，如果是follower就启动election timeout 机制

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {


	rf := &Raft{}

	rf.peers = peers
	rf.persister = persister
	rf.me = me
    //所有角色都默认初始化为Follower
	rf.state = Follower
    rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).

    rf.votedFor = -1
	rf.currentTerm = 0
    rf.logs = make([]LogEntry, 1)
    rf.logs[0] = LogEntry{Term: 0, Command: nil}
    //用来重置选举定时器的chan
    rf.resetElection = make(chan interface{})
    //是一个集合，针对每个peer都有一个值，表示下一个需要 发送给跟随者的日志条目的索引地址  
    //当一个领导⼈刚获得权力的时候，他初始化所有的 nextIndex 值为⾃己的最后 一条日志的index加1
    rf.nextIndex = make([]int , len(rf.peers))

    // 是一个集合，针对每个peer都有一个值，是leader收到的其他peer已经确认一致的日志序号。
    rf.matchIndex = make([]int , len(rf.peers))

    rf.commitCond = sync.NewCond(&rf.mu)
    
    rf.done = make(chan interface{})

    //发送心跳的定时器间隔
    rf.heartbeatTimerInterval = time.Millisecond*50

    //初始化选举定时器的间隔
    rf.electionTimeOutDuration = time.Duration(randTimeOut())*time.Millisecond

    //初始化选举定时器
    rf.electionTimer = time.NewTimer(rf.electionTimeOutDuration)
	//receive heart beat and reset timer
	//election trigger on

    //已经提交的日志，怎么算是提交呢，半数的机器复制了日志，返回了True
    rf.commitIndex=rf.lastIncludedIndex

    //最后被应用到状态机的日志项索引值
    rf.lastApplied=rf.lastIncludedIndex

    //启动选举守护线程
    go rf.electionDaemon()

    go rf.ApplyLogEntryDaemon()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

    DPrintf("Make: [%d-%s]: peer start at term %d",  rf.me, rf, rf.currentTerm)

	return rf
}

func randTimeOut() int {
    //rand的seed必须不同
    rand.Seed(time.Now().UnixNano())
    randTimeOut := (400 + rand.Intn(150)*3)
    return randTimeOut
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}
