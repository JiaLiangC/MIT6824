package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	id int
	seqnum int
	LeaderId int
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


func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	clientIdCh := getClientIdCh()
	ck.id = <- clientIdCh
	ck.LeaderId = len(servers)-1
	ck.seqnum = 0
	DPrintf("shardmaster MakeClerk: %+v", ck)
	return ck
}



//这里的Call 是同步的，不加超时和重试，如果网络隔离会一直卡住，得不到返回，why？
// func (ck *Clerk) Query(num int) Config {
// 	args := &QueryArgs{}
// 	// Your code here.
// 	args.Num = num
// 	args.ClientId = ck.id
// 	args.SeqNum = ck.seqnum
// 	for {
// 		for idx, _ := range ck.servers {
// 			var reply QueryReply
// 			DPrintf("Client[%d]:1. send a  Query to leader. LeaderId:%d, ck.seqnum:%d", ck.id, ck.LeaderId,ck.seqnum)
// 			ok := ck.servers[ck.LeaderId].Call("ShardMaster.Query", args, &reply)
// 			if ok && reply.WrongLeader == false {
// 				ck.seqnum += 1
// 				DPrintf("Client:[%d]:4.Query Client  GET QueryResponse success, num is %d, ck.SeqNo: %d, LeaderId:%d, reply.Config:%v", ck.id, num, ck.seqnum, ck.LeaderId, reply.Config)
// 				return reply.Config
// 			}else{
// 				ck.LeaderId = idx
// 			}
			
// 		}
// 		time.Sleep(100 * time.Millisecond)
// 	}
// }

//这里的Call 是同步的，不加超时和重试，如果网络隔离会一直卡住，得不到返回，why？
func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientId = ck.id
	args.SeqNum = ck.seqnum
	for {
		for idx, _ := range ck.servers {
			var reply QueryReply
			DPrintf("Client[%d]:1. send a  Query to leader. LeaderId:%d, ck.seqnum:%d", ck.id, ck.LeaderId,ck.seqnum)
			ok := ck.servers[idx].Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.seqnum += 1
				DPrintf("Client:[%d]:4.Query Client  GET QueryResponse success, num is %d, ck.SeqNo: %d, LeaderId:%d, reply.Config:%v", ck.id, num, ck.seqnum, ck.LeaderId, reply.Config)
				return reply.Config
			}else{
				//ck.LeaderId = idx
			}
			
		}
		time.Sleep(100 * time.Millisecond)
	}
}


// func (ck *Clerk) Join(servers map[int][]string) {
// 	args := &JoinArgs{}
// 	// Your code here.
// 	args.Servers = servers
// 	args.ClientId = ck.id
// 	args.SeqNum = ck.seqnum

// 	for {
// 		// try each known server.
// 		for idx, _ := range ck.servers {
// 			var reply JoinReply
// 			DPrintf("Client[%d]:1. send a  Join to leader. LeaderId:%d, ck.seqnum:%d, servers: %v", ck.id, ck.LeaderId,ck.seqnum,servers)
// 			ok := ck.servers[ck.LeaderId].Call("ShardMaster.Join", args, &reply)
// 			if ok && reply.WrongLeader == false {
// 				ck.seqnum += 1
// 				DPrintf("Client:[%d]:4.Join Client  GET JoinResponse success, ck.SeqNo: %d, LeaderId:%d", ck.id, ck.seqnum, ck.LeaderId)
// 				return
// 			}else{
// 				ck.LeaderId = idx
// 			}
			
// 		}
// 		time.Sleep(100 * time.Millisecond)
// 	}
// }

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.id
	args.SeqNum = ck.seqnum

	for {
		// try each known server.
		for idx, _ := range ck.servers {
			var reply JoinReply
			DPrintf("Client[%d]:1. send a  Join to leader. LeaderId:%d, ck.seqnum:%d, servers: %v", ck.id, ck.LeaderId,ck.seqnum,servers)
			ok := ck.servers[idx].Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.seqnum += 1
				DPrintf("Client:[%d]:4.Join Client  GET JoinResponse success, ck.SeqNo: %d, LeaderId:%d", ck.id, ck.seqnum, ck.LeaderId)
				return
			}else{
				//ck.LeaderId = idx
			}
			
		}
		time.Sleep(100 * time.Millisecond)
	}
}



// func (ck *Clerk) Leave(gids []int) {
// 	args := &LeaveArgs{}
// 	// Your code here.
// 	args.GIDs = gids
// 	args.ClientId = ck.id
// 	args.SeqNum = ck.seqnum
// 	for {
// 		// try each known server.
// 		for idx,_ := range ck.servers {
// 			var reply LeaveReply
// 			DPrintf("Client[%d]:1. send a  Leave to leader. LeaderId:%d, ck.seqnum:%d, gids:%v", ck.id, ck.LeaderId,ck.seqnum,gids)
// 			ok := ck.servers[ck.LeaderId].Call("ShardMaster.Leave", args, &reply)
// 			if ok && reply.WrongLeader == false {
// 				ck.seqnum += 1
// 				DPrintf("Client:[%d]:4.Leave Client  GET LeaveResponse success, ck.SeqNo: %d, LeaderId:%d", ck.id, ck.seqnum, ck.LeaderId)
// 				return
// 			}else{
// 				ck.LeaderId = idx
// 			}
			
// 		}
// 		time.Sleep(100 * time.Millisecond)
// 	}
// }

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.id
	args.SeqNum = ck.seqnum
	for {
		// try each known server.
		for idx,_ := range ck.servers {
			var reply LeaveReply
			DPrintf("Client[%d]:1. send a  Leave to leader. LeaderId:%d, ck.seqnum:%d, gids:%v", ck.id, ck.LeaderId,ck.seqnum,gids)
			ok := ck.servers[idx].Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.seqnum += 1
				DPrintf("Client:[%d]:4.Leave Client  GET LeaveResponse success, ck.SeqNo: %d, LeaderId:%d", ck.id, ck.seqnum, ck.LeaderId)
				return
			}else{
				//ck.LeaderId = idx
			}
			
		}
		time.Sleep(100 * time.Millisecond)
	}
}


// func (ck *Clerk) Move(shard int, gid int) {
// 	args := &MoveArgs{}
// 	// Your code here.
// 	args.Shard = shard
// 	args.GID = gid
// 	args.ClientId = ck.id
// 	args.SeqNum = ck.seqnum
// 	for {
// 		// try each known server.
// 		for idx,_ := range ck.servers {
// 			var reply MoveReply
// 			DPrintf("Client[%d]:1. send a  Move to leader. LeaderId:%d, ck.seqnum:%d, shard:%v, gid:%v", ck.id, ck.LeaderId,ck.seqnum, shard, gid)
// 			ok := ck.servers[ck.LeaderId].Call("ShardMaster.Move", args, &reply)
// 			if ok && reply.WrongLeader == false {
// 				ck.seqnum += 1
// 				DPrintf("Client:[%d]:4.Move Client  GET MoveResponse success, ck.SeqNo: %d, LeaderId:%d", ck.id, ck.seqnum, ck.LeaderId)
// 				return
// 			}else{
// 				ck.LeaderId = idx
// 			}
			
// 		}
// 		time.Sleep(100 * time.Millisecond)
// 	}
// }

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.id
	args.SeqNum = ck.seqnum
	for {
		// try each known server.
		for idx,_ := range ck.servers {
			var reply MoveReply
			DPrintf("Client[%d]:1. send a  Move to leader. LeaderId:%d, ck.seqnum:%d, shard:%v, gid:%v", ck.id, ck.LeaderId,ck.seqnum, shard, gid)
			ok := ck.servers[idx].Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.seqnum += 1
				DPrintf("Client:[%d]:4.Move Client  GET MoveResponse success, ck.SeqNo: %d, LeaderId:%d", ck.id, ck.seqnum, ck.LeaderId)
				return
			}else{
				//ck.LeaderId = idx
			}
			
		}
		time.Sleep(100 * time.Millisecond)
	}
}
