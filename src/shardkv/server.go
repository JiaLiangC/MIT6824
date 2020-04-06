package shardkv


// import "ShardKV"
import (
	"labrpc"
	"sort"
)
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

//TODO 考虑 slice 操作效率问题

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

type GC struct {
	ConfigNum   int
	Shard int
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
	dbPool    map[int]map[string]string 		//{shardID:{key: value}}
	historyRequest map[int]int
	agreementNotifyCh map[int] chan struct{}
	snapshotIndex int
	persister *raft.Persister
	sm       *shardmaster.Clerk

	config   shardmaster.Config

	inShards map[int]map[int]bool 		//{ConfigNum: [shards ]}
	//outShards map[int]map[int]int 			//{configNum:{shard: gid}}

	//其实只用存shard id数组即可，但是go 中slice 不好删除，所以用了map
	garbages map[int]map[int]bool       //{configNum:[shard]}

}



type  ShardMigrationArgs struct{
	Shard int
	ConfigNum int
}

type  ShardMigrationReply struct{
	Err string
	ConfigNum int
	Shard int
	ShardData map[string]string
	Seqnum int

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

	var key string
	if cmd.OpType==Get{
		args := cmd.Args.(GetArgs)
		key = args.Key
	}else{
		args := cmd.Args.(PutAppendArgs)
		key = args.Key
	}
	shard := key2shard(key)
	kv.mu.Lock()
	res := kv.config.Shards[shard]  == kv.gid
	kv.mu.Unlock()
	DPrintf("ShardKV[%d][%d]:GroupValid kv:%+v , cmd:%+v ,gid: %d, shard: %d \n", kv.gid, kv.me, kv, cmd, kv.config.Shards[shard] , shard)
	return res
}


//TODO 判断shard 是否处于迁移中
//TODO 判断当前config 的上一任中是否有该 shard
func (kv *ShardKV) isShardPending(cmd Op) bool{

	var key string
	if cmd.OpType==Get{
		args := cmd.Args.(GetArgs)
		key = args.Key
	}else{
		args := cmd.Args.(PutAppendArgs)
		key = args.Key
	}
	shard := key2shard(key)
	kv.mu.Lock()
	currentNum := kv.config.Num
	_, ok := kv.inShards[currentNum-1][shard]
	kv.mu.Unlock()
	DPrintf("ShardKV[%d][%d]:isShardPending kv:%+v , cmd:%+v ,gid: %d, shard: %d, kv.inShards:%+v \n", kv.gid, kv.me, kv, cmd, kv.config.Shards[shard] , shard, kv.inShards)
	return ok
}



//key 处于移除列表，则 wrong group, 如果是 移入列表 则wrong leader .

func (kv *ShardKV) waitForAgree(cmd Op,  fillReply func(err Err)){

	if _, isLeader := kv.rf.GetState(); !isLeader{
		DPrintf("ShardKV[%d][%d]:3. kv.rf.GetState, waitForAgree false,WrongLeader \n", kv.gid, kv.me)
		fillReply(ErrWrongLeader)
		return
	}

	if !kv.GroupValid(cmd){
		DPrintf("ShardKV[%d][%d]:3.waitForAgree false, WrongGroup kv:%+v \n", kv.gid, kv.me, kv)
		fillReply(ErrWrongGroup)
		return
	}

	//TODO 判断是请求的 shard 是否处于Migration中
	if kv.isShardPending(cmd){
		DPrintf("ShardKV[%d][%d]:3.isMigrationDone false,WrongGroup kv:%+v \n", kv.gid, kv.me, kv)
		fillReply(ErrWrongLeader)
		return
	}


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

	kv.waitForAgree(command, func(err Err){
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
						DPrintf("ShardKV[%d][%d]:ApplyChDaemon, msg:%v \n", kv.gid, kv.me, msg)
						switch op := msg.Command.(type) {
						case shardmaster.Config:
							DPrintf("ShardKV[%d][%d]:UpdateShardMigrationInfo, msg:%v  op:%+v\n", kv.gid, kv.me, msg,op)
							kv.UpdateShardMigrationInfo(op)
						case ShardMigrationReply:
							kv.applyMigration(op)
						case GC:
							kv.doGarbageCollection(op)
						case Op:
							DPrintf("ShardKV[%d][%d]:ApplyChDaemon, msg:%v  op:%+v\n", kv.gid, kv.me, msg,op.Args)
							kv.mu.Lock()
							latestSeq,ok := kv.historyRequest[op.ClientId]
							kv.mu.Unlock()

							//if !ok || op.SeqNum >= latestSeq {
							if !ok || ok {
								DPrintf("ShardKV[%d][%d]:ApplyChDaemon apply start, msg:%+v \n", kv.gid, kv.me, msg)
								kv.updateKv(op.OpType, op.Args)
								kv.handleSnapshot(msg.CommandIndex)
								kv.mu.Lock()

								// 如果是请求就回应请求，如果是回放日志就读不到通道，不回应请求。
								if ch, ok := kv.agreementNotifyCh[msg.CommandIndex];ok && ch!=nil{
									close(ch)
									delete(kv.agreementNotifyCh, msg.CommandIndex)
								}
								
								DPrintf("ShardKV:[%d][%d]:ApplyChDaemon Request got log agreement,and apply success to slef state machine and finished,op is %+v kvdb is : %+v",kv.gid, kv.me, op, kv.dbPool)
								kv.mu.Unlock()
							}else{
								DPrintf("ShardKV:[%d][%d]:ApplyChDaemon Request got log agreement,op is %+v kvdb is : %+v,op.SeqNum(%d) >= latestSeq(%d)",kv.gid, kv.me, op, kv.dbPool,op.SeqNum,latestSeq)
							}
						}

					}
			}
		}
}

func (kv *ShardKV)doGarbageCollection(arg GC){
	//kv.dbPool    map[int]map[string]string
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.dbPool, arg.Shard)
	
	// if _,ok := kv.outShards[arg.ConfigNum];ok{
	// 	delete(kv.outShards[arg.ConfigNum], arg.Shard)
		
	// }
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

// [1,3,4]
// [4,3,1]
// [3,4]
func get_shards_by_gid(shards [shardmaster.NShards]int, gid int) []int {
	DPrintf("get_shards_by_gid shards:%+v gid:%d \n", shards, gid)
	shardIds := make([]int,0)
	for i:=0; i<len(shards); i++{
		if shards[i] == gid{
			shardIds = append(shardIds, i)
		}
	}
	DPrintf("get_shards_by_gid finished shards:%+v gid:%d shard_ids:%+v \n", shards, gid, shardIds)
	return shardIds
}

func contains(s []int, e int) bool {
    for _, a := range s {
        if a == e {
            return true
        }
    }
    return false
}



//计算出 扇入扇出的shard 和对应的gid, 作为工作组
//{configNum:{shard: gid}}
//这里连续更新两个任期后
//TODO 存在一个问题，当config连续变化，此时Shard kV 的 config 会保持最新，旧的没完成的都会被覆盖
//所以有必要按照任期来保管 in_shards
func (kv *ShardKV)UpdateShardMigrationInfo(config shardmaster.Config) {
	//算出出去的 configNum: shard  shard gid  
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("ShardKV:[%d][%d]:UpdateShardMigrationInfo start. config:%+v",kv.gid, kv.me, config)

	if config.Num <= kv.config.Num{
		DPrintf("ShardKV:[%d][%d]:UpdateShardMigrationInfo return. config.Num(%d) <= kv.config.Num(%d) config:%+v, kv.config:%+v",kv.gid, kv.me, config.Num, kv.config.Num, config, kv.config)
		return
	}

	oldConfig := kv.config
	kv.config = config

	if oldConfig.Num == 0{
		return
	}

	oldShards := get_shards_by_gid(oldConfig.Shards, kv.gid)
	newShards := get_shards_by_gid(config.Shards, kv.gid)

	//old shard 在new_shard 中不存在的就是 out的 new shard 中有，old 中没有的就是进来的
	/*for idx, old_shard := range old_shards{
		if _,ok := kv.outShards[config.num];!ok{
			 kv.outShards[config.num] = make(map[int]int)
		}
		if !contains(new_shards, old_shard){
			kv.outShards[config.Num][old_shard] = old_config.Num
		}
	}*/


	DPrintf("ShardKV:[%d][%d]:UpdateShardMigrationInfo start. config:%+v, oldShards:%+v newShards:%+v ",kv.gid, kv.me, config, oldShards, newShards)

	for _, newShard := range newShards {
		if !contains(oldShards, newShard){
			//kv.inShards[newShard] = oldConfig.Num
			if _,ok := kv.inShards[oldConfig.Num];!ok{
				kv.inShards[oldConfig.Num] = make(map[int]bool)
			}
			kv.inShards[oldConfig.Num][newShard] = true
		}
	}
	DPrintf("ShardKV:[%d][%d]:UpdateShardMigrationInfo finished.  config:%+v, kv.config:%+v, kv.inShards:%+v, kv:%+v",kv.gid, kv.me, config, kv.config,kv.inShards,kv)
}



// 当config变化后尝试拉取Shard，如果得到reply就写入日志，然后所有节点执行数据操纵
//周期性的 pull shard,所以不用担心 对方group 重选导致的第一轮请求不到数据问题

//给Key 排序
//TODO 考虑重复写入日志请求的情况
//考虑连续进入10个config的情况
func  (kv *ShardKV)PullShard(){
	kv.mu.Lock()
	if _, isLeader := kv.rf.GetState(); !isLeader || len(kv.inShards)==0{
		kv.mu.Unlock()
		return
	}

	DPrintf("ShardKV:[%d][%d]:PullShard in kv:%+v ",kv.gid, kv.me, kv)

	//keys := make([]int,0)
	var keys []int
	for k,_ := range kv.inShards{
		keys = append(keys, k)
	}
	sort.Ints(keys)

	DPrintf("ShardKV:[%d][%d]:PullShard in1 kv:%+v inShards:%+v, keys:%+v",kv.gid, kv.me, kv,kv.inShards,keys)

	//按照config num 的顺序写入日志
	//
	for _,configNum := range keys{
		config := kv.sm.Query(configNum)
		DPrintf("ShardKV:[%d][%d]:PullShard in2 kv:%+v inShards:%+v, keys:%+v,configNum:%d",kv.gid, kv.me, kv,kv.inShards,keys,configNum)
		for shard,_ := range kv.inShards[configNum]{
			DPrintf("ShardKV:[%d][%d]:PullShard in2 kv:%+v inShards:%+v, keys:%+v,shard:%d",kv.gid, kv.me, kv,kv.inShards,keys,shard)
			go func(shard int,  cfg shardmaster.Config){
				args := ShardMigrationArgs{Shard: shard,ConfigNum: cfg.Num}
				gid := cfg.Shards[shard]
				for _,server := range cfg.Groups[gid]{
					srv := kv.make_end(server)
					reply := ShardMigrationReply{}
					DPrintf("ShardKV:[%d][%d]:PullShard start kv:%+v args:%+v,kv.inShards:%+v",kv.gid, kv.me, kv, args,kv.inShards)

					if ok := srv.Call("ShardKV.ShardMigration", &args, &reply); ok && reply.Err == OK{
						DPrintf("ShardKV:[%d][%d]:PullShard  finished successfully kv:%+v args:%+v, reply:%+v",kv.gid, kv.me, kv, args,reply)
						//因为shard 数据需要保持一致,所以写入日志，应用到所有节点上
						//reply 中做leader 和 confignum
						kv.rf.Start(reply)
					}else {
						DPrintf("ShardKV:[%d][%d]:PullShard  failed  kv:%+v args:%+v, reply:%+v  ok:%+v",kv.gid, kv.me, kv, args,reply,ok)
					}
				}
			}(shard, config)
		}

	}


	/*for shard, configNum  := range kv.inShards{
		config := kv.sm.Query(configNum)

		go func(cfg shardmaster.Config){
			args := ShardMigrationArgs{Shard: shard,ConfigNum: cfg.Num}
			gid := cfg.Shards[shard]
			for _,server := range cfg.Groups[gid]{
				srv := kv.make_end(server)
				reply := ShardMigrationReply{}
				DPrintf("ShardKV:[%d][%d]:PullShard start kv:%+v args:%+v",kv.gid, kv.me, kv, args)

				if ok := srv.Call("ShardKV.ShardMigration", &args, &reply); ok && reply.Err == OK{
					DPrintf("ShardKV:[%d][%d]:PullShard  finished successfully kv:%+v args:%+v, reply:%+v",kv.gid, kv.me, kv, args,reply)
					//因为shard 数据需要保持一致,所以写入日志，应用到所有节点上
					//reply 中做leader 和 confignum 
					kv.rf.Start(reply)
				}
			}
		}(config)
	}*/
	kv.mu.Unlock()

}


//ShardMigration RPC  
//收到RPC 后发送对应的数据给请求者
func  (kv *ShardKV) ShardMigration(args *ShardMigrationArgs, reply *ShardMigrationReply){
	DPrintf("ShardKV:[%d][%d]:ShardMigration in, kv:%+v args:%+v",kv.gid, kv.me, kv, args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.ConfigNum = args.ConfigNum
	reply.Shard = args.Shard

	if _, isLeader := kv.rf.GetState();!isLeader{
		reply.Err = ErrWrongLeader
		return
	}

	if args.ConfigNum>=kv.config.Num{
		reply.Err = ErrWrongGroup
		return
	}

	DPrintf("ShardKV:[%d][%d]:ShardMigration start, kv:%+v args:%+v",kv.gid, kv.me, kv, args)

	shardData := make(map[string]string)
	for k,v := range kv.dbPool[args.Shard]{
		shardData[k] = v
	}

	reply.Err = OK
	reply.ShardData = shardData
}

// 收到来自 ShardMigration RPC 请求的leader 把reply 写入日志，
//apply Daemon 收到ShardMigrationReply类型的日志后，执行实际的DB变动操作。
//然后设置GC 信息，等待GC

//这里有一个重要的h过滤，就是过滤 config 日期比自己小的请求，
//当 config 连续变化时，checkConfig 可以快速更新 group 成员的config到最新的 Config
//此时server 就不会拉取到旧的数据了
func (kv *ShardKV)applyMigration(replyData ShardMigrationReply){
//all server

	DPrintf("ShardKV:[%d][%d]:applyMigration start, kv:%+v replyData:%+v",kv.gid, kv.me, kv, replyData)

	if replyData.ConfigNum >= kv.config.Num{
		return
	}

	delete(kv.inShards[replyData.ConfigNum], replyData.Shard)
	if len(kv.inShards[replyData.ConfigNum])==0{
		delete(kv.inShards,replyData.ConfigNum)
	}

	if _ ,ok := kv.dbPool[replyData.Shard]; !ok{
		kv.dbPool[replyData.Shard] = make(map[string] string)

		//apply to DB
		for k,v := range replyData.ShardData{
			kv.dbPool[replyData.Shard][k] = v
		}

		//TODO set garbage collection info
		/*if _,ok := kv.garbages[replyData.ConfigNum];!ok{
			kv.garbages[replyData.ConfigNum] = make(map[int]bool)
		}
		kv.garbages[replyData.ConfigNum][replyData.Shard]=true*/
	}

	DPrintf("ShardKV:[%d][%d]:applyMigration finished, kv:%+v replyData:%+v, kv.inShards:%+v",kv.gid, kv.me, kv, replyData,kv.inShards)
}


//GarbageCollecttion RPC
//进行垃圾回收，干掉已经应用了的 shard data, leader收到GC 请求后后写入日志,然后等待apply后真正执行GC操作
func (kv *ShardKV)GarbageCollecttion(args *ShardMigrationArgs, reply *ShardMigrationReply){
	reply = &ShardMigrationReply{ConfigNum: args.ConfigNum, Shard: args.Shard}
	if _, isLeader := kv.rf.GetState();!isLeader{
		reply.Err = ErrWrongLeader
		return
	}

	//TODO config num 判断 不能大于当前的config,reply 日志到达一直性判断

	//判断 out shards 是否包含
	reply.Err = ErrWrongGroup
	//if _,ok := kv.outShards[args.ConfigNum]; !ok{return	}
	//if _,ok := kv.outShards[args.ConfigNum][args.Shard]; !ok{return}

	cmd := GC{ConfigNum:args.ConfigNum, Shard: args.Shard}

	kv.rf.Start(cmd)
	//CommandIndex, term, isLeader := kv.rf.Start(cmd)
	reply.Err = OK

}



// try GC 
//clear remote shard that already pulled
func (kv *ShardKV)tryGarbageCollecttion(){
	if _, isLeader := kv.rf.GetState();!isLeader || len(kv.garbages)==0{
		return
	}

	for configNum, shards := range kv.garbages{
		config := kv.sm.Query(configNum)

		for shard,_ := range shards{

			go func(cfg shardmaster.Config){
				args :=ShardMigrationArgs{Shard: shard, ConfigNum: cfg.Num}
				gid := cfg.Shards[shard]
				for _,server := range kv.config.Groups[gid]{
					srv := kv.make_end(server)
					reply := ShardMigrationReply{}
					if ok := srv.Call("ShardKV.GarbageCollecttion", &args, &reply); ok && reply.Err == OK{
						delete(kv.garbages[configNum],shard)
						if len(kv.garbages[configNum])==0{
							delete(kv.garbages, configNum)
						}
					}
				}
			}(config)
		}
	}

}


func (kv *ShardKV)isMigrationDone() bool {
	len := len(kv.inShards)
	res := len == 0
	return res
}

//每次间隔300ms 去拉取一次 master端的config信息,拉取到以后写入日志
func (kv *ShardKV)CheckConfig() {

	kv.mu.Lock()
	if _, isLeader := kv.rf.GetState();!isLeader || len(kv.inShards)>0{
		kv.mu.Unlock()
		return
	}
	//DPrintf("ShardKV:[%d][%d]:CheckConfig start. config:%+v",kv.gid, kv.me, kv.config)
	config := kv.config
	kv.mu.Unlock()

	currentConfigNum := kv.config.Num

	new_config := kv.sm.Query(currentConfigNum+1)

	//DPrintf("ShardKV:[%d][%d]:CheckConfig  . config: %+v, new_config:%+v",kv.gid, kv.me, kv.config,new_config)
	if config.Num +1 == new_config.Num{
		DPrintf("ShardKV:[%d][%d]:CheckConfig CONFIG CHANGED. config:%+v new_config:%+v",kv.gid, kv.me, kv.config, new_config)
		kv.rf.Start(new_config)
	}
}


func (kv *ShardKV)Daemon(task func(), terminalMs int) {
	for{
		select{
		case <-kv.doneCh:
			return
		case <- time.After(time.Duration(terminalMs) * time.Millisecond):
			task()
		}

	}
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
	labgob.Register(shardmaster.Config{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetArgs{})
	labgob.Register(ShardMigrationArgs{})
	labgob.Register(ShardMigrationReply{})
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
	kv.config = shardmaster.Config{}

	kv.inShards = make(map[int]map[int]bool)
	kv.garbages = make(map[int]map[int]bool)


	kv.sm = shardmaster.MakeClerk(masters)


	go kv.ApplyChDaemon()
	go kv.Daemon(kv.CheckConfig,100)
	go kv.Daemon(kv.PullShard,200)

	DPrintf("ShardKV:[%d][%d]: server  Started------------ \n", kv.gid, kv.me)

	return kv
}

/*
* leader: CheckConfig(周期性检查配置变化)  启动时，启动一个线程专门定时拉取config, confignum == currentConfignum+1 && isMigrationDone,成功后写入日志
* all: UpdateShardMigrationInfo(收到raft 的applyCh 后处理config信息)   applyDaemon中 如果日志类型为config, 调用配置变更处理函数 UpdateShardMigrationInfo 处理消息。
* all: UpdateShardMigrationInfo 负责根据日志更新自己的config 和需要移动的shard列表
* leader: PullShard  周期性的，负责根据shard迁移列表中的信息请求对应的 group 拿到对应的shard ，然后写入日志
* all   applyMigration applyDaemon 中根据 ShardMigrationReply 类型的日志,应用shard 到本地，然后设置GC信息， 设置已经应用的shard 到GC列表
* leader  tryGarbageCollecttion 查看CG 列表上是否有待回收的shard，然后发起 GarbageCollecttion RPC 请求，服务端收到后，把GC 信息写入日志，收到回应后清楚对应的GC信息
* all doGarbageCollection   applyDaemon 中收到GC 类型日志后，删除DB中对应的Shard ,和 outShard中对应的shard
* 
*
*GroupValid    snapshot 改造
*/




