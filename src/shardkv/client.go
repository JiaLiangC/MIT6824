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
	ClientId int64
	SeqNum    int
	LeaderId int
	mu           sync.Mutex
}


var ClientIdCh chan int64
var once  sync.Once


func getClientIdCh() chan int64 {
    once.Do(func() {
        ClientIdCh = make(chan int64, 1)
        max := 1 << 32
        go func(){
            for i :=0;i<max;i++{
                ClientIdCh <- int64(i)
            }
        }()
    })
    return ClientIdCh 
}

// the tester calls MakeClerk.
// masters[] is needed to call shardmaster.MakeClerk().
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.
	//ck.ClientId = nrand()
	clientIdCh := getClientIdCh()
	ck.ClientId = <-clientIdCh

	ck.SeqNum = 0
	ck.config = ck.sm.Query(-1)
	ck.LeaderId = 0

	DPrintf("shardKv MakeClerk finished: %+v", ck)
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
				DPrintf("ShardKvClient[%d]:GET 1 Started, LeaderId: %d, args:%+v", ck.ClientId, ck.LeaderId, args)
				ok := srv.Call("ShardKV.Get", &args, &reply)
				DPrintf("ShardKvClient[%d]:GET 2 finished, LeaderId: %d, key is: %s,shard is: %d, reply:%+v", ck.ClientId, ck.LeaderId, key, shard, reply)

				if ok && reply.WrongLeader == false && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.SeqNum += 1
					return reply.Value
				}

				if ok && (reply.Err == ErrWrongGroup) {
					break
				}

				ck.LeaderId = si+1
				ck.LeaderId %= len(servers)
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
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[ck.LeaderId])
				var reply PutAppendReply
				DPrintf("ShardKvClient[%d]:PutAppend 1 Started:  LeaderId:%d, args:%v", ck.ClientId, ck.LeaderId, args)
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				DPrintf("ShardKvClient[%d]:PutAppend 2 Finished. LeaderId:%d, args:%+v reply:%+v", ck.ClientId, ck.LeaderId, args, reply)
				if ok && reply.WrongLeader == false && reply.Err == OK {
					ck.SeqNum += 1
					return
				}

				if ok && reply.Err == ErrWrongGroup {
					break
				}

				ck.LeaderId = si+1
				ck.LeaderId %= len(servers)

			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, Put)
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, Append)
}
