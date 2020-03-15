package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "labrpc"
import "crypto/rand"
import "math/big"
import "shardmaster"
import "time"
import "sync"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}



func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	ClientId int
	SeqNum    int
	LeaderId int
	mu           sync.Mutex
}


var ClientIdCh chan int
var once  sync.Once


func getClientIdCh() chan int {
    once.Do(func() {
        ClientIdCh = make(chan int, 1)
        max := 1 << 32
        go func(){
            for i :=0;i<max;i++{
                ClientIdCh <- i
            }
        }()
    })
    return ClientIdCh 
}

// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.
	clientIdCh := getClientIdCh()
	ck.ClientId = <- clientIdCh
	ck.SeqNum = 0
	ck.config = ck.sm.Query(-1)
	ck.LeaderId = 0

	DPrintf("shardKv MakeClerk: %+v", ck)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key, ClientId: ck.ClientId, SeqNum: ck.SeqNum}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[ck.LeaderId])
				var reply GetReply
				DPrintf("Client:[%d]: Client  GET  RPC Call, LeaderId: %d, key is: %s,shard is: %d, ck.SeqNum:%d", ck.ClientId, ck.LeaderId, key, shard, ck.SeqNum)
				ok := srv.Call("ShardKV.Get", &args, &reply)

				if ok && reply.WrongLeader == false && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.SeqNum += 1
					DPrintf("Client:[%d]: Client  GET requestResbreakponse success, WrongLeader: %v LeaderId: %d, key is: %s value is %s,shard is: %d, ck.SeqNum:%d", ck.ClientId, reply.WrongLeader, ck.LeaderId,key, reply.Value,shard, ck.SeqNum)
					return reply.Value
				}

				if ok && (reply.Err == ErrWrongGroup) {
					break
				}

				if ok && reply.WrongLeader == true{
					ck.LeaderId = si+1
					ck.LeaderId %= len(servers)
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}


// shared by Put and Append.
// You will have to modify this function.

func (ck *Clerk) PutAppend(key string, value string, op OpT) {

	args := PutAppendArgs{Key: key, Value: value, OpType: op, SeqNum: ck.SeqNum, ClientId: ck.ClientId}

	for {
		ck.config = ck.sm.Query(-1)
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		DPrintf("Client[%d]:1. send a  PutAppend to leader. LeaderId:%d, args:%v", ck.ClientId, ck.LeaderId, args)
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[ck.LeaderId])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.WrongLeader == false && reply.Err == OK {
					ck.SeqNum += 1
					DPrintf("Client:[%d]: Client  PutAppend requestResponse success, WrongLeader: %v LeaderId: %d, key is: %s , ck.SeqNum:%d", ck.ClientId, reply.WrongLeader, ck.LeaderId,key, ck.SeqNum)
					return
				}

				if ok && reply.Err == ErrWrongGroup {
					break
				}

				if ok && reply.WrongLeader == true{
					ck.LeaderId = si+1
					ck.LeaderId %= len(servers)
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, Put)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, Append)
}
