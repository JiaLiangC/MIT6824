package shardkv

import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	_ "net/http/pprof"
	"raft"
	"shardmaster"
	"strconv"
	"sync"
	"time"
)

//重启后的，日志回放 如何保证日志回放完成之前不处理数据请求，
//只要保证 新的请求追加在 raft 日志末尾即可，raft 日志有持久化，重启后读取的也是新的权量的raft日志 ,因为是有序的，等所有前面的日志按个回放结束，新的请求才会被处理
//TODO 考虑 slice 操作效率问题
//TODO 网络不稳定时的丢包问题，丢包后seqnum 被加1，导致后续的重试无法通过去重操作
//日志回放结束后 又做了更新config的操作导致重复拉取的错误的旧的 shard 数据  // 在configNum 3的时候就处理了操作，日志没回放出来

//锁和channel 导致的死锁

type Op struct {
	OpType   OpT
	Args     interface{}
	ClientId int64 //客户端ID，为了去重
	SeqNum   int   //请求序列号，去重使用
}

type GC struct {
	ConfigNum int
	Shard     int
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
	doneCh            chan struct{}
	dbPool            map[int]map[string]string //{shardID:{key: value}}
	historyRequest    map[int64]int
	agreementNotifyCh map[int]chan struct{}
	snapshotIndex     int
	persister         *raft.Persister
	sm                *shardmaster.Clerk

	config shardmaster.Config

	inShards map[int]map[int]bool //{ConfigNum: [shards ]}
	//outShards map[int]map[int]int 			//{configNum:{shard: gid}}

	//其实只用存shard id数组即可，但是go 中slice 不好删除，所以用了map
	garbages              map[int]map[int]bool //{configNum:[shard]}
	migrationApplyHistory map[int]map[int]bool //config:Num{shard: bool}
}

type ShardMigrationArgs struct {
	Shard     int
	ConfigNum int
}

type ShardMigrationReply struct {
	Err            string
	ConfigNum      int
	Shard          int
	ShardData      map[string]string
	HistoryRequest map[int64]int
	Seqnum         int
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
	if *s == 0 {
		strs = "Get"
	}
	if *s == 1 {
		strs = "Put"
	}
	if *s == 2 {
		strs = "Append"
	}
	return strs
}

func (s *ShardKV) String() string {
	strs := fmt.Sprintf("%+v", s.config)
	return "{me:" + strconv.Itoa(s.me) +
		" || " + "gid:" + strconv.Itoa(s.gid) +
		" || " + "maxraftstate:" + strconv.Itoa(s.maxraftstate) +
		" || " + "snapshotIndex:" + strconv.Itoa(s.snapshotIndex) +
		" || " + "config:" + strs + "}"
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

// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	kv.doneCh <- struct{}{}
}

//group 迁移期间返回wrongGroup
func (kv *ShardKV) GroupValid(cmd Op) bool {
	var key string
	if cmd.OpType == Get {
		args := cmd.Args.(GetArgs)
		key = args.Key
	} else {
		args := cmd.Args.(PutAppendArgs)
		key = args.Key
	}
	shard := key2shard(key)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	res := kv.config.Shards[shard] == kv.gid
	return res
}

//判断shard 是否处于迁移中
//判断当前config 的上一任中是否有该 shard
func (kv *ShardKV) isShardPending(cmd Op) bool {
	var key string
	if cmd.OpType == Get {
		args := cmd.Args.(GetArgs)
		key = args.Key
	} else {
		args := cmd.Args.(PutAppendArgs)
		key = args.Key
	}
	shard := key2shard(key)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	currentNum := kv.config.Num
	_, ok := kv.inShards[currentNum-1][shard]

	DPrintf("ShardKV[%d][%d][%d]:isShardPending kv:%+v , cmd:%+v , kv.inShards:%+v \n", kv.gid, kv.me, kv.config.Num, kv, cmd, kv.inShards)
	return ok
}

//server 端对于过期重复请求，之前是直接false，导致后续大量重复请求，卡住。
//为什么有过期请求会卡住呢？因为网络不可靠时，请求在apply后 reply中丢失，对方重试就会出现过期请求，这时要同意。
func (kv *ShardKV) waitForAgree(cmd Op, fillReply func(err Err)) {

	if _, isLeader := kv.rf.GetState(); !isLeader {
		fillReply(ErrWrongLeader)
		return
	}

	if !kv.GroupValid(cmd) {
		DPrintf("ShardKV[%d][%d][%d]: 3.waitForAgree false,Err: WrongGroup return \n", kv.gid, kv.me, kv.config.Num)
		fillReply(ErrWrongGroup)
		return
	}

	if kv.isShardPending(cmd) {
		DPrintf("ShardKV[%d][%d][%d]: 3.waitForAgree false,Err:isShardPending return \n", kv.gid, kv.me, kv.config.Num)
		fillReply(ErrWrongLeader)
		return
	}

	kv.mu.Lock()
	latestSeq, ok := kv.historyRequest[cmd.ClientId]
	kv.mu.Unlock()

	//过期请求就返回 WrongLeader
	if ok && cmd.SeqNum <= latestSeq {
		fillReply(OK)
		DPrintf("ShardKV[%d][%d][%d] 3.waitForAgree false,Err: duplicate request. cmd.SeqNum<=latestSeq(%d<=%d) \n", kv.gid, kv.me, kv.config.Num, cmd.SeqNum, latestSeq)
		return
	}

	CommandIndex, term, _ := kv.rf.Start(cmd)
	notifyCh := make(chan struct{})
	//这里chan设计请求不能并发

	kv.mu.Lock()
	kv.agreementNotifyCh[CommandIndex] = notifyCh
	kv.mu.Unlock()

	select {
	case <-kv.doneCh:
		return
	case <-notifyCh:
		if currentTerm, isLeader := kv.rf.GetState(); !isLeader || term != currentTerm {
			DPrintf("ShardKV[%d][%d][%d] 3.waitForAgree false,Err: WrongLeader,after raft applych, leader changed \n", kv.gid, kv.me, kv.config.Num)
			fillReply(ErrWrongLeader)
			return
		}
		fillReply(OK)
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	//1.过滤重复请求 2.等待applyChDaemon 的通知 3.过滤非leader 请求
	DPrintf("ShardKV[%d][%d][%d] 2.Get RPC Request, args:%+v ", kv.gid, kv.me, kv.config.Num, args)
	command := Op{OpType: Get, Args: *args, ClientId: args.ClientId, SeqNum: args.SeqNum}

	kv.waitForAgree(command, func(err Err) {
		if err == OK {
			reply.WrongLeader = false
			shard := key2shard(args.Key)
			kv.mu.Lock()
			value, ok := kv.dbPool[shard][args.Key]
			kv.mu.Unlock()
			if ok {
				reply.Value = value
				reply.Err = OK
				DPrintf("ShardKV[%d][%d][%d][%d] 4.GET RPC Finished , kvDB:%+v args:%+v, reply:%+v\n", kv.gid, kv.me, kv.config.Num, kv.dbPool, args, reply)
			} else {
				reply.Err = ErrNoKey
			}
		} else {
			reply.Err = err
			reply.WrongLeader = true
		}
	})
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//1.过滤重复请求 2.等待applyChDaemon 的通知 3.过滤非leader 请求
	DPrintf("ShardKV[%d][%d][%d] 2.PutAppend RPC Request, args:%+v ", kv.gid, kv.me, kv.config.Num, args)
	command := Op{OpType: args.OpType, Args: *args, ClientId: args.ClientId, SeqNum: args.SeqNum}

	kv.waitForAgree(command, func(err Err) {
		if err == OK {
			reply.WrongLeader = false
			reply.Err = OK
			DPrintf("ShardKV[%d][%d][%d][%d] 4.PutAppend RPC Finished , kvDB:%+v args:%+v, reply:%+v\n", kv.gid, kv.me, kv.config.Num, kv.dbPool, args, reply)
		} else {
			reply.Err = err
			reply.WrongLeader = true
		}
	})
}

//把日志应用到自己的状态机中
//去重,验证请求是否合法然后根据请求类型作出不同的动作
//当 config 快速变化时，如何防止去拉取数据时，对方还没拉取到你要的新的数据
//1.从configNum控制，2,对方相应时，先等待自己为完成的迁移完成。
func (kv *ShardKV) ApplyChDaemon() {
	for {
		select {
		case <-kv.doneCh:
			return
		case msg := <-kv.applyCh:
			if !msg.CommandValid {
				DPrintf("ShardKV:[%d][%d][%d]:ApplyChDaemon ApplySnapshot to DB, msg:%+v \n", kv.gid, kv.me, kv.config.Num, msg)
				kv.mu.Lock()
				kv.readSnapshot(msg.Command.([]byte))
				kvSnapshotData := kv.NewSnapshot(msg.CommandIndex)
				kv.persister.SaveStateAndSnapshot(kv.persister.ReadRaftState(), kvSnapshotData)
				kv.mu.Unlock()
				continue
			}

			if msg.Command != nil && msg.CommandIndex > kv.snapshotIndex {
				DPrintf("ShardKV:[%d][%d][%d] ApplyChDaemon:0, msg:%+v \n", kv.gid, kv.me, kv.config.Num, msg)

				switch op := msg.Command.(type) {
				case shardmaster.Config:
					DPrintf("ShardKV:[%d][%d][%d] ApplyChDaemon:1 UpdateShardMigrationInfo start, msg:%+v \n", kv.gid, kv.me, kv.config.Num, msg)
					kv.UpdateShardMigrationInfo(op)
				case ShardMigrationReply:
					DPrintf("ShardKV:[%d][%d][%d] ApplyChDaemon applyMigration, msg:%+v  op:%+v\n", kv.gid, kv.me, kv.config.Num, msg, op)
					kv.applyMigration(op)
				case GC:
					DPrintf("ShardKV:[%d][%d]:ApplyChDaemon doGarbageCollection, msg:%+v  op:%+v\n", kv.gid, kv.me, msg, op)
					kv.doGarbageCollection(op)
				case Op:
					// 必须严格过滤重复请求，不然会导致数据不一致
					DPrintf("ShardKV:[%d][%d][%d] ApplyChDaemon  Op Operation, msg:%v  op:%+v\n", kv.gid, kv.me, kv.config.Num, msg, op.Args)
					kv.mu.Lock()
					latestSeq, ok := kv.historyRequest[op.ClientId]
					if !ok || op.SeqNum > latestSeq {
						kv.updateKv(op.OpType, op.Args)
						kv.historyRequest[op.ClientId] = op.SeqNum
						DPrintf("ShardKV:[%d][%d][%d] ApplyChDaemon  Got log agreement, apply successfully to state machine,op is %+v kvdb is : %+v", kv.gid, kv.me, kv.config.Num, op, kv.dbPool)
					} else {
						DPrintf("ShardKV:[%d][%d][%d] ApplyChDaemon Got log agreement,op is %+v kvdb is : %+v,op.SeqNum(%d) >= latestSeq(%d)", kv.gid, kv.me, kv.config.Num, op, kv.dbPool, op.SeqNum, latestSeq)
					}
					kv.mu.Unlock()
				}

				kv.handleSnapshot(msg.CommandIndex)

				// 如果是请求就回应请求，如果是回放日志就读不到通道，不回应请求。
				kv.mu.Lock()
				if ch, ok := kv.agreementNotifyCh[msg.CommandIndex]; ok && ch != nil {
					close(ch)
					delete(kv.agreementNotifyCh, msg.CommandIndex)
				}
				kv.mu.Unlock()

			} else {
				DPrintf("ShardKV[%d][%d][%d] ApplyChDaemon:000 error, msg:%+v \n", kv.gid, kv.me, kv.config.Num, msg)
			}
		}
	}
}

func (kv *ShardKV) doGarbageCollection(arg GC) {
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	delete(kv.dbPool, arg.Shard)

	// if _,ok := kv.outShards[arg.ConfigNum];ok{
	// 	delete(kv.outShards[arg.ConfigNum], arg.Shard)

	// }
}

func (kv *ShardKV) updateKv(t OpT, args interface{}) {
	DPrintf("ShardKV[%d][%d][%d] updateKv  start, Op:%+v args:%+v \n", kv.gid, kv.me, kv.config.Num, t, args)
	switch t {
	case Put:
		cmd := args.(PutAppendArgs)
		shard := key2shard(cmd.Key)

		if _, ok := kv.dbPool[shard]; !ok {
			kv.dbPool[shard] = make(map[string]string)
		}
		kv.dbPool[shard][cmd.Key] = cmd.Value
		DPrintf("ShardKV[%d][%d][%d] updateKv  finished, Op:%+v args:%+v , kv db:%+v\n", kv.gid, kv.me, kv.config.Num, t, args, kv.dbPool)
	case Append:
		cmd := args.(PutAppendArgs)
		shard := key2shard(cmd.Key)
		if _, ok := kv.dbPool[shard]; !ok {
			kv.dbPool[shard] = make(map[string]string)
		}
		kv.dbPool[shard][cmd.Key] += cmd.Value
		DPrintf("ShardKV[%d][%d][%d] updateKv  finished, Op:%+v args:%+v ,kv db:%+v \n", kv.gid, kv.me, kv.config.Num, t, args, kv.dbPool)
	}
}

func (kv *ShardKV) handleSnapshot(index int) {
	if kv.maxraftstate < 0 {
		return
	}

	if kv.persister.RaftStateSize() < kv.maxraftstate*10/9 {
		return
	}

	DPrintf("ShardKV:[%d][%d][%d] HandleSnapshot start snapshot kv.persister.RaftStateSize() > kv.maxraftstate*10/9(%d > %d)", kv.gid, kv.me, kv.config.Num, kv.persister.RaftStateSize(), kv.maxraftstate*10/9)

	kvSnapshotData := kv.NewSnapshot(index)
	kv.rf.TakeSnapshot(index, kvSnapshotData)
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	kv.dbPool = make(map[int]map[string]string)

	kv.historyRequest = make(map[int64]int)

	d.Decode(&kv.dbPool)
	d.Decode(&kv.historyRequest)
	d.Decode(&kv.snapshotIndex)
	d.Decode(&kv.inShards)
	d.Decode(&kv.config)
	DPrintf("ShardKV:[%d][%d]:readSnapshot finished . config.Num(%d) , kv.dbPool:%+v", kv.gid, kv.me, kv.config.Num, kv.dbPool)

	if kv.config.Num!=0{
		go kv.restartMigration()
	}

}

func (kv *ShardKV) NewSnapshot(index int) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	kv.snapshotIndex = index

	e.Encode(kv.dbPool)
	e.Encode(kv.historyRequest)
	e.Encode(kv.snapshotIndex)
	e.Encode(kv.inShards)
	e.Encode(kv.config)

	data := w.Bytes()
	DPrintf("ShardKV:[%d][%d]:NewSnapshot finished . config.Num(%d) , kv.dbPool:%+v", kv.gid, kv.me, kv.config.Num, kv.dbPool)
	return data
}

//1. 如果snapshot中不加入Inshards,当shard拉取了一般宕机重启后，会丢失未拉取的数据
//2. snapshot 记录了Inshard, 当所有节点都拉取完毕后，重启集群发生，会读到旧的Inshards

func get_shards_by_gid(shards [shardmaster.NShards]int, gid int) []int {
	//DPrintf("get_shards_by_gid shards:%+v gid:%d \n", shards, gid)
	shardIds := make([]int, 0)
	for i := 0; i < len(shards); i++ {
		if shards[i] == gid {
			shardIds = append(shardIds, i)
		}
	}
	//DPrintf("get_shards_by_gid finished shards:%+v gid:%d shard_ids:%+v \n", shards, gid, shardIds)
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

//因为Check config 存在重复写入raft的可能性，所以要过滤,传进来的ConfigNum必须大于当前的Config(过滤Check config的重复写入)
//对于从snapshot中恢复出来的时候，会check config并且写入老得请求，但是在这里会被过滤掉。
//TODO 这里有没有必要把OutShard做一个备份，如果对方还没拉取完对应的Shard,但是 这部分Shard的数据变动了，
func (kv *ShardKV) UpdateShardMigrationInfo(config shardmaster.Config) {
	//算出出去的 configNum: shard  shard gid
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if config.Num <= kv.config.Num {
		DPrintf("ShardKV:[%d][%d][%d] UpdateShardMigrationInfo duplicate Config, return. config.Num(%d) <= kv.config.Num(%d)", kv.gid, kv.me, kv.config.Num, config.Num, kv.config.Num)
		return
	}

	if config.Num != kv.config.Num+1 {
		DPrintf("ShardKV:[%d][%d][%d] UpdateShardMigrationInfo config.Num:%d, kv.config.Num:%d", kv.gid, kv.me, kv.config.Num, config.Num, kv.config.Num)
		panic("kv.configs.Num+1 != newConfig.Num")
		return
	}
	oldConfig := kv.config
	kv.config = config

	if oldConfig.Num == 0 {
		DPrintf("ShardKV:[%d][%d][%d] UpdateShardMigrationInfo finished configNum 0 return")
		return
	}

	oldShards := get_shards_by_gid(oldConfig.Shards, kv.gid)
	newShards := get_shards_by_gid(config.Shards, kv.gid)

	for _, newShard := range newShards {
		if !contains(oldShards, newShard) {
			if _, ok := kv.inShards[oldConfig.Num]; !ok {
				kv.inShards[oldConfig.Num] = make(map[int]bool)
			}
			kv.inShards[oldConfig.Num][newShard] = true
		}
	}

	DPrintf("ShardKV:[%d][%d][%d] UpdateShardMigrationInfo finished", kv.gid, kv.me, kv.config.Num)
	//For Leader Pull Shards
	if _, isLeader := kv.rf.GetState(); isLeader {
		if shards, ok := kv.inShards[oldConfig.Num]; ok && len(shards) != 0 {
			DPrintf("ShardKV:[%d][%d][%d] UpdateShardMigrationInfo finished then try to pull shard, oldConfig.Num:%d", kv.gid, kv.me, kv.config.Num, oldConfig.Num)
			go kv.PullShard(oldConfig.Num)
			//kv.PullShard(oldConfig.Num)
		}
		return
	} else {
		DPrintf("ShardKV:[%d][%d][%d] UpdateShardMigrationInfo  not leader ++++++++++++++")
	}
}

// 当config变化后尝试拉取Shard，如果得到reply就写入日志，然后所有节点执行数据操纵
//周期性的 pull shard,所以不用担心 对方group 重选导致的第一轮请求不到数据问题

//给Key 排序
//考虑重复写入日志请求的情况下，会在updateConfiguration一轮中过滤掉比自己小的config

// golang for range 数组和slice 时， 两个值分别为 index,value := range
// golang slcie 初始化时 , make 指定cap len , cap 是最终容量, len指定会初始化自动填充值。keys := make([]int,1) 如果不需要自动填充值，那就用以下方式初始化 var keys []int

//BUG snapshot 时记录了 inShard, 当inShard 变更时没有更新snapshot的值，导致集群重启后，拉取到了错误的数据

//1. 要干掉比自己当前任期小的Inshards数据, 否则会导致后续的数据进不来，因为Inshards始终不为空
//2. PullShard中有多次写入的数据，或者集群重启后老的InShards 这里根据 ConfigNum过滤部分，后续Shard Data Migration处也要过滤
//3. Pull Shard 这里保证Shard 的有序性，其实很难,  PullShard 是根据Inshard中 ConfigNum作为Key排序来保证写入日志的有序性，
// Pull shard 中用了 go 程去拉取Shard，不同ConfigNum返回的数据顺序无法得到保证，这里如果不用GO程，会很慢。
//
//snapshot后的pull shard 问题, 因为PullShard 放到了applyconfig中，从而避免了snapshot恢复后不断PullShard
// 这里要避免 CheckConfig 写入后，会过滤比自己小的config，因为日志的有序性，这里不用担心 snapshot 后的Config问题
//TODO 突然宕机后，未完成的PullShard

//失败了重复请求
func (kv *ShardKV) PullShard(oldConfigNum int) {

	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	DPrintf("ShardKV:[%d][%d][%d] PullShard in kv start:%+v ", kv.gid, kv.me, kv.config.Num, kv)
	//按照config num 的顺序写入日志

	oldConfig := kv.sm.Query(oldConfigNum)
	taskShards := kv.inShards[oldConfigNum]

	for shard, _ := range taskShards {
		go func(shard int, cfg shardmaster.Config) {

			args := ShardMigrationArgs{Shard: shard, ConfigNum: cfg.Num}
			gid := cfg.Shards[shard]

			//请求失败后for 不断拉取
			for {
				select {
				case <-kv.doneCh:
					return
				default:
				}

				if _, isLeader := kv.rf.GetState(); !isLeader {
					return
				}

				for _, server := range cfg.Groups[gid] {
					srv := kv.make_end(server)
					reply := ShardMigrationReply{}
					DPrintf("ShardKV:[%d][%d][%d] PullShard start kv:%+v args:%+v,kv.inShards:%+v server:%+v", kv.gid, kv.me, kv.config.Num, kv, args, kv.inShards, server)

					//wrong leader 重试， 对方config 比当前小时，返回wrong leader重试。
					if ok := srv.Call("ShardKV.ShardMigration", &args, &reply); ok && reply.Err == OK {
						if _, isLeader := kv.rf.GetState(); !isLeader {
							return
						}

						kv.mu.Lock()
						//当前Kv的config没有更新, 并且pull Shard没有完成，防止重复PullShard
						if kv.config.Num == oldConfigNum+1 && !kv.isMigrationDone() {
							DPrintf("ShardKV:[%d][%d][%d] PullShard finished kv:%+v args:%+v,kv.inShards:%+v", kv.gid, kv.me, kv.config.Num, kv, args, kv.inShards)
							kv.rf.Start(reply)
						}
						kv.mu.Unlock()
						return
					} else {
						DPrintf("ShardKV:[%d][%d][%d]:PullShard  failed  kv:%+v args:%+v, reply:%+v  ok:%+v", kv.gid, kv.me, kv.config.Num, kv, args, reply, ok)
					}
					time.Sleep(100 * time.Millisecond)
				}
			}

		}(shard, oldConfig)
	}

}

//ShardMigration RPC
//收到RPC 后发送对应的数据给请求者
//这里要过滤和清理集群重启后从Snapshot中读取到的旧的Shard发来的数据。
//TODO 这里对方重复的Pullshard 请求实际上还是会成功的，只不过在applyMigration 里会过滤掉
func (kv *ShardKV) ShardMigration(args *ShardMigrationArgs, reply *ShardMigrationReply) {
	DPrintf("ShardKV:[%d][%d][%d]:ShardMigration in, args:%+v", kv.gid, kv.me, kv.config.Num, args)
	reply.ConfigNum = args.ConfigNum
	reply.Shard = args.Shard
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	//args.ConfigNum 是config更新前的一个config, 如果kv的当前config小于请求者当前的Config,说明还没更新到，然后延迟，返回wrongleader，等待下次请求
	// args.ConfigNum+1 等于发送者的任期，所以至少等到发送者任期和我一样
	//至于我大于对方多个任期时，拉取的数据会不会不一致，不会，被拉取shard的更新已经被pullShard指向了新的shard
	//
	if args.ConfigNum+1 > kv.config.Num {
		reply.Err = ErrWrongLeader
		DPrintf("ShardKV:[%d][%d][%d]:ShardMigration PostPone=== args.ConfigNum+1 > kv.config.Num, args:%+v, kv.inShards:%+v", kv.gid, kv.me, kv.config.Num, args, kv.inShards)
		return
	}

	//其次要等待我拉取完成,因为如果config 快速更新，对方要的数据恰好是我还没拉取的数据，就会拉取到脏数据
	//如果对方要的shard 在inshard中才拒绝，不在就放行, 否则，如果A和B A需要B的shard, B 需要A的shard, 互相未完成，导致互相阻塞
	//args.Shard
	_, ok := kv.inShards[args.ConfigNum][args.Shard]
	if !kv.isMigrationDone() && ok {
		reply.Err = ErrWrongLeader
		DPrintf("ShardKV:[%d][%d][%d]:ShardMigration PostPone==========, args:%+v, kv.inShards:%+v", kv.gid, kv.me, kv.config.Num, args, kv.inShards)
		return
	}

	shardData := make(map[string]string)
	for k, v := range kv.dbPool[args.Shard] {
		shardData[k] = v
	}

	historyRequest := make(map[int64]int)
	for client, seqNum := range kv.historyRequest {
		historyRequest[client] = seqNum
	}
	DPrintf("ShardKV:[%d][%d]:ShardMigration start, kv.historyRequest:%+v historyRequest:%+v", kv.gid, kv.me, kv.historyRequest, historyRequest)
	reply.Err = OK
	reply.ShardData = shardData
	reply.HistoryRequest = historyRequest

}


//做了快照后，正 pullshard 时，宕机导致 shard 拉取没结束，导致最后数据缺失，所以要重新拉取
//TODO 重新拉取会不会导致 inshard 重复
//如果 重复pullshard 请求，config变更时这里会停止

func (kv *ShardKV) restartMigration() {
	kv.mu.Lock()

	_, ok := kv.inShards[kv.config.Num-1]
	curConfigNum := kv.config.Num
	oldConfigNum := curConfigNum-1

	kv.mu.Unlock()

	if curConfigNum == 0 || !ok {
		return
	}

	for {
		select {
		case <-kv.doneCh:
			return
		default:
			kv.mu.Lock()
			curKvConfigNum := kv.config.Num
			kv.mu.Unlock()

			if curKvConfigNum != oldConfigNum+1 {
				return
			}
			if _, isLeader := kv.rf.GetState(); isLeader {
				go kv.PullShard(oldConfigNum)
			}
			DPrintf("ShardKV:[%d][%d][%d]:restartMigration , kv.inshards:%+v", kv.gid, kv.me,kv.config.Num, kv.inShards)
			time.Sleep(300 * time.Millisecond)
		}
	}

}

// 收到来自 ShardMigration RPC 请求的leader 把reply 写入日志，
//apply Daemon 收到ShardMigrationReply类型的日志后，执行实际的DB变动操作。
//然后设置GC 信息，等待GC

//这里有一个重要的h过滤，就是过滤 config 日期比自己小的请求
//当 config 连续变化时，checkConfig 可以快速更新 group 成员的config到最新的 Config
//此时server 就不会拉取到旧的数据了
//
//shard流程：CheckConfig->writeToLog->applyLog, configChanged,updateInshards->pullShard -> getShardData->writeToLog->ShardMigration
//当pullShard时用的未更新前的Shard的ConfigNum,在 configChanged 步中更新了ConfigNum,所以ShardMigration中的ConfigNum一定比当前的ConfigNum小
//TODO 问题是 CheckConfig 快速更新了 kv configNum 但是这里如何保证 apply Config num 的顺序性
//试着在源头保证 Pull Shard 那一侧保证 写入的有序性
//CheckConfig会快速更新Config到最新， shardData的Migration必须按照config变化的顺序来。

func (kv *ShardKV) applyMigration(replyData ShardMigrationReply) {
	//all server
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("ShardKV:[%d][%d][%d]:applyMigration start, kv:%+v replyData:%+v", kv.gid, kv.me,kv.config.Num, kv, replyData)
	if replyData.ConfigNum+1 != kv.config.Num || kv.isMigrationDone() {
		return
	}

	//如果apply重复就返回
	if _, ok := kv.migrationApplyHistory[replyData.ConfigNum]; ok {
		if _, ok := kv.migrationApplyHistory[replyData.ConfigNum][replyData.Shard]; ok {
			DPrintf("ShardKV:[%d][%d]:applyMigration Duplicate Error , kv:%+v replyData:%+v", kv.gid, kv.me, kv, replyData)
			return
		}
	}

	//清楚完成的Shard
	delete(kv.inShards[replyData.ConfigNum], replyData.Shard)
	if len(kv.inShards[replyData.ConfigNum]) == 0 {
		delete(kv.inShards, replyData.ConfigNum)
	}

	if _, ok := kv.dbPool[replyData.Shard]; !ok {
		kv.dbPool[replyData.Shard] = make(map[string]string)
	}

	//TODO set garbage collection info
	/*if _,ok := kv.garbages[replyData.ConfigNum];!ok{
		kv.garbages[replyData.ConfigNum] = make(map[int]bool)
	}
	kv.garbages[replyData.ConfigNum][replyData.Shard]=true*/

	//apply to DB
	for k, v := range replyData.ShardData {
		kv.dbPool[replyData.Shard][k] = v
	}

	for client_id, seqNum := range replyData.HistoryRequest {
		original_seq, ok := kv.historyRequest[client_id]
		if ok {
			if seqNum > original_seq {
				kv.historyRequest[client_id] = seqNum
			}
		} else {
			kv.historyRequest[client_id] = seqNum
		}
	}

	if _, ok := kv.migrationApplyHistory[replyData.ConfigNum]; !ok {
		kv.migrationApplyHistory[replyData.ConfigNum] = make(map[int]bool)
	}
	kv.migrationApplyHistory[replyData.ConfigNum][replyData.Shard] = true

	DPrintf("ShardKV:[%d][%d]:applyMigration finished, kv:%+v replyData:%+v, kv.inShards:%+v", kv.gid, kv.me, kv, replyData, kv.inShards)
}

//GarbageCollecttion RPC
//进行垃圾回收，干掉已经应用了的 shard data, leader收到GC 请求后后写入日志,然后等待apply后真正执行GC操作
func (kv *ShardKV) GarbageCollecttion(args *ShardMigrationArgs, reply *ShardMigrationReply) {
	reply = &ShardMigrationReply{ConfigNum: args.ConfigNum, Shard: args.Shard}
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	//TODO config num 判断 不能大于当前的config,reply 日志到达一直性判断

	//判断 out shards 是否包含
	reply.Err = ErrWrongGroup
	//if _,ok := kv.outShards[args.ConfigNum]; !ok{return	}
	//if _,ok := kv.outShards[args.ConfigNum][args.Shard]; !ok{return}

	cmd := GC{ConfigNum: args.ConfigNum, Shard: args.Shard}

	kv.rf.Start(cmd)
	//CommandIndex, term, isLeader := kv.rf.Start(cmd)
	reply.Err = OK

}

//clear remote shard that already pulled
func (kv *ShardKV) tryGarbageCollecttion() {
	if _, isLeader := kv.rf.GetState(); !isLeader || len(kv.garbages) == 0 {
		return
	}

	for configNum, shards := range kv.garbages {
		config := kv.sm.Query(configNum)

		for shard, _ := range shards {

			go func(cfg shardmaster.Config) {
				args := ShardMigrationArgs{Shard: shard, ConfigNum: cfg.Num}
				gid := cfg.Shards[shard]
				for _, server := range kv.config.Groups[gid] {
					srv := kv.make_end(server)
					reply := ShardMigrationReply{}
					if ok := srv.Call("ShardKV.GarbageCollecttion", &args, &reply); ok && reply.Err == OK {
						delete(kv.garbages[configNum], shard)
						if len(kv.garbages[configNum]) == 0 {
							delete(kv.garbages, configNum)
						}
					}
				}
			}(config)
		}
	}

}

func (kv *ShardKV) isMigrationDone() bool {
	_, ok := kv.inShards[kv.config.Num-1]
	return !ok
}

//每次间隔300ms 去拉取一次 master端的config信息,拉取到以后写入日志
//1.每隔300ms拉取一次Config,只拉取比自己大一个的Config
//2.拉取到的Config写入日志，存在重复写入同一个Config的可能性
func (kv *ShardKV) CheckConfig() {
	//BUG fix 没有用锁保护下面的读取  kv.config.Num，导致拉取到了错误的config num
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || !kv.isMigrationDone() {
		kv.mu.Unlock()
		return
	}

	DPrintf("ShardKV:[%d][%d][%d]:CheckConfig start.", kv.gid, kv.me, kv.config.Num)

	nextConfigNum := kv.config.Num + 1
	kv.mu.Unlock()

	newConfig := kv.sm.Query(nextConfigNum)
	if nextConfigNum == newConfig.Num {
		DPrintf("ShardKV:[%d][%d][%d]:CheckConfig CONFIG CHANGED. config:%+v new_config:%+v", kv.gid, kv.me, kv.config.Num, newConfig)
		kv.rf.Start(newConfig)
	}

}

func (kv *ShardKV) Daemon(task func(), terminalMs int) {
	for {
		select {
		case <-kv.doneCh:
			return
		case <-time.After(time.Duration(terminalMs) * time.Millisecond):
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
	kv.historyRequest = make(map[int64]int)
	kv.agreementNotifyCh = make(map[int]chan struct{})

	kv.agreementNotifyCh = make(map[int]chan struct{})
	kv.config = shardmaster.Config{}
	kv.inShards = make(map[int]map[int]bool)
	kv.garbages = make(map[int]map[int]bool)
	kv.migrationApplyHistory = make(map[int]map[int]bool)
	kv.sm = shardmaster.MakeClerk(masters)

	//BUG fixsnapshot 放的靠前，导致值被覆盖了
	kv.readSnapshot(kv.persister.ReadSnapshot())
	go kv.Daemon(kv.CheckConfig, 200)
	go kv.ApplyChDaemon()
	DPrintf("ShardKV:[%d][%d]: server  Started------------ \n", kv.gid, kv.me)

	return kv
}

//问题1，check config 设置每次读取比当前大的一个config,不会读取最新的config
//问题2 集群重启后导致数据异常

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
