package shardmaster


import "raft"
import "labrpc"
import "sync"
import "labgob"
import "consistenthash"
import "time"
import "log"

const Debug = 1

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config // indexed by config num

	//snapshotIndex  int
	consistentHashMap *consistenthash.Map
	persister *raft.Persister
	historyRequest map[int]int
	notifyChs map[int]chan struct{}
	doneCh chan struct{}
	
}


//这里和KV server 相似，Config 是需要持久化保存的东西。相当于DB
//这里保存Op, 根据传来的Op重放configs
//针对不同的Op ,apply 做不同的操作
//同时去重复，保证操作的线性.

type Op struct {
	// Your data here.
	OpType   OpT      //Join  Leave  Move Query
	Args interface{}
	ClientId 	int  //客户端ID，为了去重
	SeqNum 	 int    //请求序列号，去重使用
}

type OpT int
const(
	Join OpT = iota
	Leave
	Move
	Query
)


func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


func (sm *ShardMaster) waitForAgree(cmd Op,  fillReply func(success bool)){
	if _, isLeader := sm.rf.GetState();!isLeader{
		DPrintf("ShardMaster[%d]:3.waitForAgree false,WrongLeader \n", sm.me)
		fillReply(false)
		return 
	}

	sm.mu.Lock()
	latestSeq, ok := sm.historyRequest[cmd.ClientId]

	if ok{
		//过期请求就返回 WrongLeader
		if  cmd.SeqNum <= latestSeq  {
			fillReply(false)
			DPrintf("ShardMaster[%d]:3.waitForAgree false,cmd.SeqNum <= latestSeq(%d<=%d) \n", sm.me,cmd.SeqNum,latestSeq)
			sm.mu.Unlock()
			return
		}

		if  cmd.SeqNum > latestSeq{
			//fillReply(true)
			sm.historyRequest[cmd.ClientId] = cmd.SeqNum
			//DPrintf("ShardMaster[%d]:3.waitForAgree false,cmd.SeqNum <= latestSeq(%d<=%d) \n", sm.me,cmd.SeqNum,latestSeq)
			// sm.mu.Unlock()
			// return
		}

	}else{
		sm.historyRequest[cmd.ClientId] = cmd.SeqNum
	}

	CommandIndex, term, _ := sm.rf.Start(cmd)
	notifyCh := make(chan struct{})
	//这里chan设计请求不能并发
	sm.notifyChs[CommandIndex] = notifyCh
	sm.mu.Unlock()
	//这里设计一个超时

	select{
	case <-sm.doneCh:
		return
	case <- time.After(300*time.Millisecond):
		DPrintf("ShardMaster[%d]:3.waitForAgree false, timeout , \n", sm.me)
		fillReply(false)
		return
	case <-notifyCh:
		if currentTerm, isLeader := sm.rf.GetState();!isLeader || term!=currentTerm {
			DPrintf("ShardMaster[%d]:3.waitForAgree false,WrongLeader \n", sm.me)
			fillReply(false)
			return 
		}
		DPrintf("ShardMaster[%d]:3.waitForAgree  reached agreement, \n", sm.me)
		fillReply(true)
	}
}


//上线的 group
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	//1.过滤重复请求 2.等待applyChDaemon 的通知 3.过滤非leader 请求
	DPrintf("ShardMaster[%d]:2.Join  receive Query RPC Request, command:%v ", sm.me, args)
	command := Op{OpType: Join, Args: *args, ClientId: args.ClientId, SeqNum: args.SeqNum}

	sm.waitForAgree(command,func(success bool){
		DPrintf("ShardMaster[%d]:Join  receive Join RPC Request, command:%v ,success:%v \n", sm.me, command, success)
		if success{
			reply.WrongLeader = false
			reply.Err = OK
		}else{
			reply.WrongLeader = true
		}
	})


}



func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	command := Op{OpType: Leave, Args: *args, ClientId: args.ClientId, SeqNum: args.SeqNum}
	sm.waitForAgree(command, func(success bool){
		DPrintf("ShardMaster[%d]:Leave  receive Leave RPC Request, command:%v ,success:%v \n", sm.me, command, success)
		if success{
			reply.WrongLeader = false
			reply.Err = OK
		}else{
			reply.WrongLeader = true
		}
		
	})
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	command := Op{OpType: Move, Args: *args, ClientId: args.ClientId, SeqNum: args.SeqNum}
	sm.waitForAgree(command, func(success bool){
		DPrintf("ShardMaster[%d]:Leave  receive Move RPC Request, command:%v ,success:%v \n", sm.me, command, success)
		if success{
			reply.WrongLeader = false
			reply.Err = OK
		}else{
			reply.WrongLeader = true
		}
		
	})
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	DPrintf("ShardMaster[%d]:2.Query  receive Query RPC Request, command:%v ", sm.me, args)
	command := Op{OpType: Query, Args: *args, ClientId: args.ClientId, SeqNum: args.SeqNum}
	sm.waitForAgree(command,func(success bool){
		DPrintf("ShardMaster[%d]:2.Query  receive Query RPC Request, command:%v ,success:%v \n", sm.me, args, success)
		if success{
			reply.WrongLeader=false
			reply.Config = Config{}
			sm.mu.Lock()
			sm.copyConfig(args.Num, &reply.Config)
			sm.mu.Unlock()
			reply.Err = OK
		}else{
			reply.WrongLeader=true
		}
		
	})
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	sm.doneCh <- struct{}{}
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}


//去重,验证请求是否合法然后根据请求类型作出不同的动作
func (sm *ShardMaster) ApplyChDaemon(){
	go func () {
		for{
			select{
			case <- sm.doneCh:
				return
			case msg:= <-sm.applyCh:
					if msg.CommandValid && msg.Command!=nil{
						op := msg.Command.(Op)
						DPrintf("ShardMaster[%d]:ApplyChDaemon, msg:%v  op:%v\n", sm.me, msg,op.Args)
						sm.mu.Lock()
						latestSeq,ok := sm.historyRequest[op.ClientId]
						sm.mu.Unlock()

						if !ok || op.SeqNum >= latestSeq{
							DPrintf("ShardMaster[%d]:ApplyChDaemon apply success, msg:%v \n", sm.me, msg)
							sm.updateConfig(op.OpType, op.Args)

							sm.mu.Lock()
							if ch, ok := sm.notifyChs[msg.CommandIndex];ok && ch!=nil{
								close(ch)
								delete(sm.notifyChs, msg.CommandIndex)
							}
							sm.mu.Unlock()
						}
					}
			}
		}
	}()
}

func (sm *ShardMaster)updateConfig(t OpT, args interface{}) {
	DPrintf("ShardMaster[%d]:updateConfig  start, OpT:%v args:%v \n", sm.me, t, args)
	switch t{
		case Join:
			sm.doJoin(args.(JoinArgs))
		case Leave:
			sm.doLeave(args.(LeaveArgs))
		case Move:
			sm.doMove(args.(MoveArgs))
		case Query:
	}
}


func (sm *ShardMaster) doJoin(args JoinArgs){
	//	configs []Config // indexed by config num
	//算出总的shards,求出目前每个group 该分配的shard 数，然后挨个group判断，多了就移出去
	//shards /all groups 
	DPrintf("ShardMaster[%d]:doJoin  start, JoinArgs:%v \n", sm.me, args)
	newConfig := &Config{} 
	sm.mu.Lock()
	//拷贝最新的config到新join的config
	sm.copyConfig(-1, newConfig)
	sm.mu.Unlock()
	newConfig.Num+=1

	keys := make([]int,0)
	for k,servers := range args.Servers{
		DPrintf("ShardMaster[%d]:doJoin keys: %v ,k:%v, servers:%v \n", sm.me, keys, k, servers)
		tmp := make([]string,len(servers))
		copy(tmp, servers)
		newConfig.Groups[k]=tmp
		keys = append(keys,k)
	}

	//keys 是 group_id， 全部加入hash 环
	sm.mu.Lock()
	sm.consistentHashMap.Add(keys...)
	var new_shards [NShards]int
	//在加入了新的hash key 后，重新计算算出所有的Shards 的hash,找到落在哪个group上
	for shard_id := range newConfig.Shards{
		group_id := sm.consistentHashMap.Get(shard_id)
		DPrintf("ShardMaster[%d]:doJoin , shard_id:%d  ,group_id:%d \n", sm.me, shard_id, group_id)
		new_shards[shard_id] = group_id
	}
	
	newConfig.Shards = new_shards
	//因为hash不是绝对均衡的分配，会有小小的偏差，这里再次做一个均衡
	sm.rebalance(newConfig)
	sm.configs = append(sm.configs,*newConfig)
	DPrintf("ShardMaster[%d]:doJoin finished, newConfig:%v  ,sm.configs:%v \n", sm.me, newConfig, sm.configs)
	sm.mu.Unlock()

}


func (sm *ShardMaster) doLeave(args LeaveArgs){
	//	configs []Config // indexed by config num
	//算出总的shards,求出目前每个group 该分配的shard 数，然后挨个group判断，多了就移出去
	//shards /all groups 
	//考虑第一分配没数据时的默认config处理和第二次分配，第三次分配
	DPrintf("ShardMaster[%d]:doLeave  start, JoinArgs:%v \n", sm.me, args)

	newConfig := &Config{}
	sm.mu.Lock()
	sm.copyConfig(-1, newConfig)
	sm.mu.Unlock()
	newConfig.Num += 1

	for _,k := range args.GIDs{
		delete(newConfig.Groups, k)
	}

	//keys 是 group_id， 全部删除hash 环
	sm.mu.Lock()
	sm.consistentHashMap.Remove(args.GIDs...)

	var new_shards [NShards] int
	//在加入了新的hash key 后，重新计算算出所有的Shards 的hash,找到落在哪个group上
	for shard_id := range newConfig.Shards{
		group_id := sm.consistentHashMap.Get(shard_id)
		DPrintf("ShardMaster[%d]:doLeave , shard_id:%d  ,group_id:%d \n", sm.me, shard_id, group_id)
		new_shards[shard_id] = group_id
	}
	newConfig.Shards = new_shards

	//因为hash不是绝对均衡的分配，会有小小的偏差，这里再次做一个均衡
	sm.rebalance(newConfig)
	sm.configs = append(sm.configs,*newConfig)
	DPrintf("ShardMaster[%d]:doLeave finished, newConfig:%v  ,sm.configs:%v \n", sm.me, newConfig, sm.configs)
	sm.mu.Unlock()

}

func (sm *ShardMaster) doMove(args MoveArgs){
	//	configs []Config // indexed by config num
	//算出总的shards,求出目前每个group 该分配的shard 数，然后挨个group判断，多了就移出去
	//shards /all groups 
	//考虑第一分配没数据时的默认config处理和第二次分配，第三次分配

	newConfig := &Config{} 
	sm.mu.Lock()
	sm.copyConfig(-1, newConfig)
	
	newConfig.Num += 1
	newConfig.Shards[args.Shard] = args.GID
	sm.configs = append(sm.configs,*newConfig)
	sm.mu.Unlock()
}


//计算重新排列shards
func  (sm *ShardMaster)rebalance(config *Config){
	//找到最大的然后匀给最小的
	DPrintf("ShardMaster[%d]:rebalance start, config:%v, sm.configs:%v \n", sm.me,config, sm.configs)
	groups_count := len(config.Groups)
	
	if groups_count==0{
		return
	}


	info := make(map[string]int)

	group_shards := make(map[int][]int)

	for shard, gid := range config.Shards{
		group_shards[gid]=append(group_shards[gid], shard)
	}



	//max_shard_count := 0
	info["min_shard_count"] = NShards
	var maxGid = -1
	var minGid = -1
	var max_shard_count=0
	var min_shard_count=NShards

	//找到最大最小shard数目，然后开始均衡
	for gid,_ := range config.Groups{
		shard_count :=  shards_count(config.Shards, gid)
		//if shard_count > info["max_shard_count"]{
		if shard_count > max_shard_count{
			maxGid = gid
			max_shard_count = shard_count
			// info["max_shard_count"] = shard_count
			// info["max_shard_gid"] = gid
		}
		//if shard_count < info["min_shard_count"]{
		if shard_count < min_shard_count{
			minGid = gid
			min_shard_count = shard_count
			//info["min_shard_count"] = shard_count
			//info["min_shard_gid"] = gid
		}
	}

	DPrintf("ShardMaster[%d]:rebalance info[max_shard_count]:%d, info[min_shard_count]:%d info[max_shard_gid]:%d, info[min_shard_gid]:%d\n",
	 sm.me,max_shard_count, min_shard_count,maxGid,minGid)

	// DPrintf("ShardMaster[%d]:rebalance info[max_shard_count]:%d, info[min_shard_count]:%d info[max_shard_gid]:%d, info[min_shard_gid]:%d\n",
	//  sm.me,info["max_shard_count"], info["min_shard_count"],info["max_shard_gid"],info["min_shard_gid"])


	//if info["max_shard_count"] > (info["min_shard_count"]+1){
	if max_shard_count > (min_shard_count+1){
		shard := group_shards[maxGid][0]
		config.Shards[shard] = minGid
		sm.rebalance(config)
	}
	DPrintf("ShardMaster[%d]:rebalance finished, config:%v, sm.configs:%v \n", sm.me,config, sm.configs)

}


//注意 map 释放内存问题 map=nil
//拷贝ShardMaster configs 中索引为index的config到目标config
func (sm *ShardMaster)copyConfig(index int, config *Config) {
	if index ==-1 || index >= len(sm.configs){
		index = len(sm.configs)-1
	}
	config.Num = sm.configs[index].Num
	config.Shards = sm.configs[index].Shards
	new_groups := make(map[int][]string)
	copyMap(new_groups, sm.configs[index].Groups)
	config.Groups = new_groups
	DPrintf("ShardMaster[%d]:copyConfig, config:%v, sm.configs:%v \n", sm.me,config, sm.configs)

}


//全部混在一起根据value 排序，得出一组key,

//return [shard idx]
func get_shards_by_gid(shards [NShards]int, gid int) []int {

	shard_ids := make([]int,1)
	for i:=0;i<len(shards);i++{
		if shards[i] == gid{
			shard_ids=append(shard_ids,i)
		}
	}
	return shard_ids
}

func shards_count(shards [NShards]int, gid int ) int{
	var s_count int;
	for i:=0;i<len(shards);i++{
		if shards[i] == gid{
			s_count+=1;
		}
		
	}
	return s_count;
}



func copyMap(source map[int][]string, target map[int][]string){
	for k,v := range target{
		tmp :=make([]string,len(v))
		copy(tmp, v)
		source[k] = tmp
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.consistentHashMap = consistenthash.New(100, nil)
	sm.persister = persister
	sm.historyRequest = make(map[int]int)
	sm.notifyChs = make(map[int]chan struct{})
	sm.doneCh = make(chan struct{})

	go sm.ApplyChDaemon()

	return sm
}
