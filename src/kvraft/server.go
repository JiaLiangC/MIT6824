package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op 	string //操作类型
	Key 	string  //键
	Value 	string  //值
	ClientId 	int  //客户端ID，为了去重
	SeqNo 	 int    //请求序列号，去重使用
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	doneCh chan struct{}

	dbPool    map[string]string

	historyRequest map[int]*LatestReply

	agreementNotifyCh map[int] chan struct{}



	// Your definitions here.
}


type LatestReply struct{
	SeqNo 	 int
	Reply  GetReply
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	if _, isleader := kv.rf.GetState(); !isleader {
		reply.WrongLeader = true
		reply.Err = "WrongLeader"
		DPrintf("KVServer:[%d]: server %d receive GET RPC Request, WrongLeader:True \n", kv.me, kv.me)
		return
	}

	kv.mu.Lock()
	if  latestReply, ok := kv.historyRequest[args.ClientId]; ok{
		if args.SeqNo <= latestReply.SeqNo{
			kv.mu.Unlock()
			reply.WrongLeader = false
			reply.Err = OK
			reply.Value =latestReply.Reply.Value
			DPrintf("KVServer:[%d]: received Duplicate GET RPC Request, WrongLeader:false   key is: %s. value is:%s  \n", kv.me, args.Key, latestReply.Reply.Value)
			return
		}
	}

	op := Op{Op: "Get",Key: args.Key, ClientId: args.ClientId, SeqNo: args.SeqNo}
	//commandIndex, term, isLeader := kv.rf.Start(op)
	commandIndex, term, _ := kv.rf.Start(op)

	ch := make(chan struct{})
	kv.agreementNotifyCh[commandIndex] = ch

	//在rf.Start 这里提前判断leader 身份没用，因为start是才开始，需要等待集群同步，中间会变动身份，所以放到后面返回后检查
	reply.WrongLeader = false
	reply.Err = OK

	kv.mu.Unlock()

	select{
		case <- kv.doneCh:
			return
		case <-ch:
			DPrintf("KVServer:[%d]: server  received aggreement GET RPC Request \n", kv.me)
			currentTerm, isleader := kv.rf.GetState();
			if  !isleader || currentTerm!=term {
				DPrintf("KVServer:[%d]: server received GET RPC  aggreement Signal, WrongLeader:True \n", kv.me)
				reply.WrongLeader = true
				reply.Err = "WrongLeader"
				return
			}

			kv.mu.Lock()
			if value, ok := kv.dbPool[args.Key]; ok{
				reply.Value = value
				DPrintf("KVServer:[%d]: server received GET RPC  aggreement Signal, WrongLeader:false key is : %s. value is %s \n", kv.me, args.Key, value)
			}else{
				reply.Err = ErrNoKey
			}
			kv.mu.Unlock()
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, isleader := kv.rf.GetState(); !isleader {
		reply.WrongLeader = true
		reply.Err = "WrongLeader"
		DPrintf("KVServer:[%d]: server %d received PutAppend RPC Request, WrongLeader:True \n", kv.me, kv.me)
		return
	}

	kv.mu.Lock()

	if  latestReply, ok := kv.historyRequest[args.ClientId]; ok{
		if args.SeqNo <= latestReply.SeqNo {
			kv.mu.Unlock()
			reply.WrongLeader = false
			reply.Err = OK
			DPrintf("KVServer:[%d]: server %d received Duplicate PutAppend RPC Request, WrongLeader:false \n", kv.me, kv.me)
			return
		}
	}

	op := Op{Op: args.Op,Key: args.Key,Value: args.Value, ClientId: args.ClientId, SeqNo: args.SeqNo}
	//commandIndex, term, isLeader := kv.rf.Start(op)
	commandIndex, term, _ := kv.rf.Start(op)

	ch := make(chan struct{})
	kv.agreementNotifyCh[commandIndex] = ch

	kv.mu.Unlock()

	//在rf.Start 这里提前判断leader 身份没用，因为start是才开始，需要等待集群同步，中间会变动身份，所以放到后面返回后检查

	reply.WrongLeader = false
	reply.Err = OK

	select{
		case <- kv.doneCh:
			return
		case <- ch:
			currentTerm, isLeader := kv.rf.GetState()
			DPrintf("KVServer:[%d]: received aggreement PutAppend Signal , WrongLeader:%v \n", kv.me, isLeader)
			//lose leadership
			if !isLeader || term != currentTerm{
			// if _, isleader := kv.rf.GetState(); !isleader{
				DPrintf("KVServer:[%d]: received success aggreement PutAppend Signal , WrongLeader:%v \n", kv.me,isLeader)
				reply.WrongLeader = true
				reply.Err = ""
				return
			}
	}
	DPrintf("KVServer:[%d]:PutAppend received success aggreement PutAppend Signal  , finished\n", kv.me)

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}




//把日志应用到自己的状态机中
func (kv *KVServer) ApplyChDaemon() {

	for{
		select{
			case <- kv.doneCh:
				return
			case msg,ok := <- kv.applyCh:
				DPrintf("KVServer[%d]:ApplyChDaemon:-------------------",kv.me)
				if ok {
					if msg.Command !=nil && msg.CommandIndex > 0{
						cmd := msg.Command.(Op)

						kv.mu.Lock()
						//过滤来自客户端的重复的请求
						//这里 CMD 的数据结构和OP相同
						if latestReply, ok := kv.historyRequest[cmd.ClientId]; !ok || cmd.SeqNo > latestReply.SeqNo{
							//"Get" "Put" or "Append"
							switch cmd.Op {
								case "Get":
									kv.historyRequest[cmd.ClientId] = &LatestReply{SeqNo: cmd.SeqNo,Reply: GetReply{Value: kv.dbPool[cmd.Key]}}
									DPrintf("KVServer:[%d]:ApplyChDaemon server receive cmd: %v \n", kv.me, cmd)
								case "Put":
									kv.dbPool[cmd.Key] = cmd.Value
									kv.historyRequest[cmd.ClientId] = &LatestReply{SeqNo: cmd.SeqNo}
									DPrintf("KVServer:[%d]:ApplyChDaemon server receive cmd: %v \n", kv.me, cmd)
								case "Append":
									kv.dbPool[cmd.Key] += cmd.Value
									kv.historyRequest[cmd.ClientId] = &LatestReply{SeqNo: cmd.SeqNo}
									DPrintf("KVServer:[%d]:ApplyChDaemon server receive cmd: %v \n", kv.me, cmd)
								default:
									DPrintf("KVServer:[%d]:ApplyChDaemon server %d receive invalid cmd: %v\n", kv.me, kv.me, cmd)
									panic("invalid command operation")
							}
						}

						if ch, ok:= kv.agreementNotifyCh[msg.CommandIndex]; ok && ch!=nil{
							//关闭通道，删除map中的地址
							close(ch)
							delete(kv.agreementNotifyCh, msg.CommandIndex)
							DPrintf("KVServer:[%d]: Request[%d] log agreement,and apply success to slef state machine",kv.me, cmd.SeqNo)
						}
						DPrintf("KVServer:[%d]: Request[%d] log agreement,and apply success to slef state machine and finished, kvdb is : %v",kv.me, cmd.SeqNo, kv.dbPool)
						kv.mu.Unlock()
					}

				}

		}
	}

}



//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.doneCh = make(chan struct{})

	kv.dbPool = make(map[string]string)

	kv.historyRequest = make(map[int]*LatestReply) 

	kv.agreementNotifyCh = make(map[int] chan struct{})

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	

	// You may need initialization code here.

	go kv.ApplyChDaemon()
	DPrintf("KVServer:[%d]: server %d Started------------ \n", kv.me, kv.me)

	return kv
}
