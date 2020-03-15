package shardkv


// import "ShardKV"
import "labrpc"
import "raft"
import "sync"
import "labgob"
import "shardmaster"
import "time"
import "bytes"
import "strconv"
import "fmt"
import "log"



//之前 服务器重启后, 日志没有回放完, 新的请求就进来了, 如下所示
//if !ok || op.SeqNum >= latestSeq {
//此时 新请求的 序列大于没有回放的日志的序列，所以一旦回放完成前，新请求被接受，
//后续老的日志如果都是该client put append的数据，那么后续的日志回放都被拒绝。
//这里去掉了 这个判断，因为在shard kv get中 已经做了去重请求。




type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType   OpT
	Args interface{}
	// Key 	string  //键
	// Value 	string  //值
	ClientId 	int  //客户端ID，为了去重
	SeqNum 	 int    //请求序列号，去重使用
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	doneCh chan struct{}
	dbPool    map[int]map[string]string
	historyRequest map[int]int
	agreementNotifyCh map[int] chan struct{}
	snapshotIndex int
	persister *raft.Persister
	sm       *shardmaster.Clerk
	config   shardmaster.Config

	logCommitedIndex int
}

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func (s *OpT) toString() string {
    var strs string
    if *s == 0{
        strs = "Get"
    }
    if *s == 1{
        strs = "Put"
    }
    if *s == 2{
        strs="Append"
    }
    return   strs
}


func( s  *ShardKV) String() string{
	strs := fmt.Sprintf("%+v",s.config)
    return "{me:"+strconv.Itoa(s.me)+
    " || "+"gid:"+strconv.Itoa(s.gid)+
    " || "+"logCommitedIndex:"+strconv.Itoa(s.logCommitedIndex)+
    " || "+"maxraftstate:"+strconv.Itoa(s.maxraftstate)+
    " || "+"snapshotIndex:"+strconv.Itoa(s.snapshotIndex)+
    " || "+"config:"+strs+"}"
}



/*设计说明
*1.每个server可能服务多个shard 或者0个shard。为了方便shard数据迁移，所以db设置为map[shard][]string的结构
*2.Config改变时(shard 分配变动),此时replica Groups 之间 应该传递shard，并且保证客户端请求的一致性。
*3.配置变更期间的get put应该得到正确处理
*4.请求到了该group，但是shard 还没完全在该group达到一致时的处理
*5.用资源的复制，尤其是rpc请求使用对应server的信息时，如果直接引用会因为server总是被锁住得不锁
*rpc中发送的信息都用copy，不要直接饮用原来的资源，不然会race
*6.shard 交换期间，不处理该shard的请求，同时维护该shard的状态。
*6.1 需要的group去拉取这个shard，拉取后删除该shard即可。拉取完成该shard后，该shard状态即变为可服务
*master 在更新完配置后，对比上一个config,划分出需要被pull的shards,这些shards状态化为不可用，
*等待对应的group达到一致后(leader)，发送请求给master标记该shard可用
*6.2 wrong Group 要从出发和返回两个阶段都判断，因为这之间可能发生过配置变更
*/


// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.doneCh <- struct{}{}
}



//验证group是否正确
//group 迁移期间返回wrongGroup
func (kv *ShardKV) GroupValid(cmd Op) bool{
	//DPrintf("ShardKV[%d][%d]:GroupValid  cmd:%+v \n", kv.gid, kv.me, cmd)
	var key string
	if cmd.OpType==Get{
		args := cmd.Args.(GetArgs)
		key = args.Key
	}else{
		args := cmd.Args.(PutAppendArgs)
		key= args.Key
	}
	kv.mu.Lock()
	kv.config = kv.sm.Query(-1)
	kv.mu.Unlock()
	shard := key2shard(key)
	gid := kv.config.Shards[shard]

	res := kv.gid == gid

	//DPrintf("ShardKV[%d][%d]:GroupValid kv:%+v , cmd:%+v ,gid: %d, shard: %d \n", kv.gid, kv.me, kv,cmd, gid, shard)

	return res
}


func (kv *ShardKV) waitForAgree(cmd Op,  fillReply func(err Err)){

	if !kv.GroupValid(cmd){
		DPrintf("ShardKV[%d][%d]:3.waitForAgree false,WrongGroup kv:%+v \n", kv.gid, kv.me, kv)
		fillReply(ErrWrongGroup)
		return
	}

	if _, isLeader := kv.rf.GetState();!isLeader{
		DPrintf("ShardKV[%d][%d]:3. kv.rf.GetState, waitForAgree false,WrongLeader \n", kv.gid, kv.me)
		fillReply(ErrWrongLeader)
		return 
	}


	//如果是leader就等待日志回放完成，如果不是leader，一开始 commitIndex 为1 >0 ，
	// if commitIndex, isLeader := kv.rf.GetCommitIndex(); !isLeader || commitIndex > kv.logCommitedIndex+1  {
	// 	DPrintf("ShardKV[%d][%d]:3. kv.rf.GetState, waitForAgree false,WrongLeader or in log replay process. kvdb:%+v, commitIndex(%d) >kv.logCommitedIndex(%d) kv.historyRequest: %+v \n", kv.gid, kv.me,kv.dbPool,commitIndex,kv.logCommitedIndex,kv.historyRequest)
	// 	fillReply(ErrWrongLeader)
	// 	return 
	// }

	kv.mu.Lock()
	latestSeq, ok := kv.historyRequest[cmd.ClientId]

	if ok{
		//过期请求就返回 WrongLeader
		if  cmd.SeqNum <= latestSeq  {
			fillReply(ErrWrongLeader)
			DPrintf("ShardKV[%d][%d]:3.waitForAgree false,cmd.SeqNum <= latestSeq(%d<=%d) \n",kv.gid,  kv.me,cmd.SeqNum,latestSeq)
			kv.mu.Unlock()
			return
		}else{
			DPrintf("ShardKV[%d][%d]:3.waitForAgree ,cmd.SeqNum(%d) > latestSeq(%d) kv:%+v kv.historyRequest:%+v \n",kv.gid,  kv.me,cmd.SeqNum,latestSeq,kv,kv.historyRequest)
			kv.historyRequest[cmd.ClientId] = cmd.SeqNum
		}

	}else{
		DPrintf("ShardKV[%d][%d]:3.waitForAgree ,not exist cmd.Seqnum. create: cmd.SeqNum:%d  kv:%+v  kv.historyRequest:%+v \n",kv.gid,  kv.me,cmd.SeqNum, kv,kv.historyRequest)
		kv.historyRequest[cmd.ClientId] = cmd.SeqNum
	}

	CommandIndex, term, _ := kv.rf.Start(cmd)
	notifyCh := make(chan struct{})
	//这里chan设计请求不能并发
	kv.agreementNotifyCh[CommandIndex] = notifyCh
	kv.mu.Unlock()
	//这里设计一个超时

	select{
	case <-kv.doneCh:
		return
	case <- time.After(300*time.Millisecond):
		DPrintf("ShardKV[%d][%d]:3.waitForAgree false, timeout , \n", kv.gid, kv.me)
		fillReply(ErrWrongLeader)
		return
	case <-notifyCh:
		if currentTerm, isLeader := kv.rf.GetState();!isLeader || term!=currentTerm {
			DPrintf("ShardKV[%d][%d]:3. after notifyCh, waitForAgree false,WrongLeader \n", kv.gid, kv.me)
			fillReply(ErrWrongLeader)
			return 
		}
		DPrintf("ShardKV[%d][%d]:3.waitForAgree  reached agreement, \n",  kv.gid,kv.me)
		fillReply(OK)
	}
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//1.过滤重复请求 2.等待applyChDaemon 的通知 3.过滤非leader 请求
	DPrintf("ShardKV[%d][%d]:2.Get  receive Query RPC Request, args:%+v ", kv.gid, kv.me, args)
	command := Op{OpType: Get, Args: *args, ClientId: args.ClientId, SeqNum: args.SeqNum}

	kv.waitForAgree(command,func(err Err){
		DPrintf("ShardKV[%d][%d]:Get  receive Get RPC Request, command:%+v ,err:%v \n", kv.gid, kv.me, command, err)
		if err==OK{
			reply.WrongLeader = false
			shard := key2shard(args.Key)
			if value, ok := kv.dbPool[shard][args.Key]; ok{
				reply.Value = value
				reply.Err = OK
				DPrintf("ShardKV:[%d][%d]:4.server received GET RPC  aggreement Signal,args:%+v , kv:%+v reply:%+v \n", kv.gid, kv.me, args, kv, reply)
			}else{
				reply.Err = ErrNoKey
			}
			DPrintf("ShardKV:[%d][%d]:4.server received GET RPC  aggreement Signal , args:%+v , kv:%+v reply:%+v shard: %d  \n", kv.gid, kv.me, args, kv, reply, shard)
		}else{
			reply.Err = err
			reply.WrongLeader = true
		}
	})
	DPrintf("ShardKV:[%d][%d] Get finished kv:%+v, args:%+v, reply:%+v", kv.gid, kv.me,kv, args, reply)
}


//上线的 group
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//1.过滤重复请求 2.等待applyChDaemon 的通知 3.过滤非leader 请求
	DPrintf("ShardKV[%d][%d]:2.PutAppend  receive PutAppend RPC Request, args:%+v ", kv.gid, kv.me, args)
	command := Op{OpType: args.OpType, Args: *args, ClientId: args.ClientId, SeqNum: args.SeqNum}

	kv.waitForAgree(command,func(err Err){
		DPrintf("ShardKV[%d][%d]:PutAppend  receive PutAppend RPC Request, command:%+v ,err:%+v \n",kv.gid, kv.me, command, err)
		if err==OK{
			reply.WrongLeader = false
			reply.Err = OK
			DPrintf("ShardKV:[%d][%d]: server received PutAppend RPC  aggreement Signal,args:%+v , kv:%+v \n", kv.gid, kv.me, args, kv)
		}else{
			reply.Err = err
			reply.WrongLeader = true
		}
	})
}


//把日志应用到自己的状态机中
//去重,验证请求是否合法然后根据请求类型作出不同的动作
func (kv *ShardKV) ApplyChDaemon(){
	go func () {
		for{
			select{
			case <- kv.doneCh:
				return
			case msg:= <-kv.applyCh:

					if !msg.CommandValid{
						kv.mu.Lock()
						//kv.restoreSnapshot(msg.Command.([]byte))
						kv.readSnapshot(msg.Command.([]byte))
						kvSnapshotData := kv.NewSnapshot(msg.CommandIndex)
						kv.persister.SaveStateAndSnapshot(kv.persister.ReadRaftState(), kvSnapshotData)
						kv.mu.Unlock()
						continue
					}

					if  msg.Command!=nil && msg.CommandIndex > kv.snapshotIndex {
						op := msg.Command.(Op)
						DPrintf("ShardKV[%d][%d]:ApplyChDaemon, msg:%v  op:%+v\n", kv.gid, kv.me, msg,op.Args)
						kv.mu.Lock()
						latestSeq,ok := kv.historyRequest[op.ClientId]
						kv.mu.Unlock()

						//if !ok || op.SeqNum >= latestSeq {
						if !ok || ok {
							DPrintf("ShardKV[%d][%d]:ApplyChDaemon apply start, msg:%+v \n", kv.gid, kv.me, msg)
			
							// if msg.CommandIndex > kv.logCommitedIndex + 1{
							// 	DPrintf("ShardKV:[%d][%d]:ApplyChDaemon Request,log replyed, op is %+v kvdb is : %+v, msg.CommandIndex(%d) > kv.logCommitedIndex(%d)", kv.gid, kv.me, op, kv.dbPool, msg.CommandIndex, kv.logCommitedIndex)
							// }

							kv.updateKv(op.OpType, op.Args)

							kv.handleSnapshot(msg.CommandIndex)

							kv.mu.Lock()

							// //记录日志位置，当停机重启时需要回放完所有日志后才提供服务
							// if msg.CommandIndex > kv.logCommitedIndex {
							// 	kv.logCommitedIndex = msg.CommandIndex
							// 	DPrintf("ShardKV:[%d][%d]:ApplyChDaemon Request,update kv.logCommitedIndex, op is %+v kvdb is : %+v, msg.CommandIndex(%d) > kv.logCommitedIndex(%d)", kv.gid, kv.me, op, kv.dbPool, msg.CommandIndex, kv.logCommitedIndex)
							// }
							
							// 如果是请求就回应请求，如果是回放日志就读不到通道，不回应请求。
							if ch, ok := kv.agreementNotifyCh[msg.CommandIndex];ok && ch!=nil{
								close(ch)
								delete(kv.agreementNotifyCh, msg.CommandIndex)
							}

							
							//DPrintf("ShardKV:[%d][%d]:ApplyChDaemon server received PutAppend RPC  aggreement Signal,args:%+v , kv:%+v \n", kv.gid, kv.me, args, kv)
							DPrintf("ShardKV:[%d][%d]:ApplyChDaemon Request got log agreement,and apply success to slef state machine and finished,op is %+v kvdb is : %+v",kv.gid, kv.me, op, kv.dbPool)
							kv.mu.Unlock()
						}else{
							DPrintf("ShardKV:[%d][%d]:ApplyChDaemon Request got log agreement,op is %+v kvdb is : %+v,op.SeqNum(%d) >= latestSeq(%d)",kv.gid, kv.me, op, kv.dbPool,op.SeqNum,latestSeq)
						}
					}
			}
		}
	}()
}

func (kv *ShardKV)updateKv(t OpT, args interface{}) {
	DPrintf("ShardKV[%d][%d]:updateConfig  start, OpT:%+v args:%+v \n", kv.gid, kv.me, t, args)
	kv.mu.Lock()
	switch t{
		case Put:
			cmd := args.(PutAppendArgs)
			shard := key2shard(cmd.Key)

			if _,ok := kv.dbPool[shard];!ok{
				kv.dbPool[shard] = make(map[string]string)
			}
			kv.dbPool[shard][cmd.Key] = cmd.Value
			DPrintf("ShardKV[%d][%d]:updateConfig  finished, OpT:%+v args:%+v , kv db:%+v\n", kv.gid, kv.me, t, args,kv.dbPool)
		case Append:
			cmd := args.(PutAppendArgs)
			shard := key2shard(cmd.Key)
			if _,ok := kv.dbPool[shard];!ok{
				kv.dbPool[shard] = make(map[string]string)
			}
			kv.dbPool[shard][cmd.Key] += cmd.Value
			DPrintf("ShardKV[%d][%d]:updateConfig  finished, OpT:%+v args:%+v ,kv db:%+v \n", kv.gid, kv.me, t, args,kv.dbPool)
		case Get:
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) handleSnapshot(index int){
	if kv.maxraftstate < 0{
		return
	}

	if kv.persister.RaftStateSize() < kv.maxraftstate*10/9 {
		return 
	}

	DPrintf("ShardKV:[%d][%d]:handleSnapshot start snapshot kv.persister.RaftStateSize() > kv.maxraftstate*10/9(%d > %d)",kv.gid, kv.me, kv.persister.RaftStateSize(),  kv.maxraftstate*10/9)

	kvSnapshotData := kv.NewSnapshot(index)
	kv.rf.TakeSnapshot(index, kvSnapshotData)
}

func (kv *ShardKV)restoreSnapshot(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if data==nil || len(data)<1{
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	kv.dbPool = make(map[int]map[string]string)

	kv.historyRequest = make(map[int]int) 

    d.Decode(& kv.dbPool)
    d.Decode(& kv.historyRequest)
    d.Decode(& kv.snapshotIndex)
	
}


func (kv *ShardKV)readSnapshot(data []byte) {
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	if data==nil || len(data)<1{
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	kv.dbPool = make(map[int]map[string]string)

	kv.historyRequest = make(map[int]int) 

    d.Decode(& kv.dbPool)
    d.Decode(& kv.historyRequest)
    d.Decode(& kv.snapshotIndex)
	
}


func (kv *ShardKV)NewSnapshot(index int) []byte{
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	kv.snapshotIndex = index

	e.Encode(kv.dbPool)
	e.Encode(kv.historyRequest)
    e.Encode(kv.snapshotIndex)
    
	data := w.Bytes()
	return data
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the ShardKV.
//
// pass masters[] to ShardKV.MakeClerk() so you can send
// RPCs to the ShardKV.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.

	labgob.Register(Op{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	// Use something like this to talk to the ShardKV:
	// kv.mck = ShardKV.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister
	kv.doneCh = make(chan struct{})
	kv.dbPool = make(map[int]map[string]string)
	kv.historyRequest = make(map[int]int)
	kv.agreementNotifyCh = make(map[int] chan struct{})
	kv.readSnapshot(kv.persister.ReadSnapshot())
	kv.agreementNotifyCh = make(map[int]chan struct{})

	kv.logCommitedIndex=0

	kv.sm = shardmaster.MakeClerk(masters)


	go kv.ApplyChDaemon()
	DPrintf("ShardKV:[%d][%d]: server  Started------------ \n", kv.gid, kv.me)

	return kv
}
