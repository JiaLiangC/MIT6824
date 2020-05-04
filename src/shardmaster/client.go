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
	id int64
	seqnum int
	LeaderId int
}

var ClientIdCh chan int64
var once  sync.Once


func getClientIdCh() chan int64 {
    once.Do(func() {
        ClientIdCh = make(chan int64, 1)
        max := 1 << 32
        go func(){
            for i :=0; i <max;i++ {
                ClientIdCh <- int64(i)
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
	DPrintf("ShardMasterClient make clerk start")
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
func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientId = ck.id
	args.SeqNum = ck.seqnum
	for {
		for _,srv := range ck.servers {
			var reply QueryReply
			DPrintf("ShardMasterClient[%d]:1.send a  Query to leader. LeaderId:%d, ck.seqnum:%d", ck.id, ck.LeaderId,ck.seqnum)
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.seqnum += 1
				DPrintf("ShardMasterShardMasterClient[%d]:4.Query  Success, num is %d, ck.SeqNo: %d, LeaderId:%d, reply.Config:%v", ck.id, num, ck.seqnum, srv, reply.Config)
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}


//这里的Call 是同步的，不加超时和重试，如果网络隔离会一直卡住，得不到返回，why？
//func (ck *Clerk) Query(num int) Config {
//	args := &QueryArgs{}
//	// Your code here.
//	args.Num = num
//	args.ClientId = ck.id
//	args.SeqNum = ck.seqnum
//	for {
//		for idx, _ := range ck.servers {
//			workDoneChan := make(chan Config)
//			go func(){
//				var reply QueryReply
//				DPrintf("ShardMasterClient[%d]:1. send a  Query to leader. LeaderId:%d, ck.seqnum:%d", ck.id, ck.LeaderId,ck.seqnum)
//				ok := ck.servers[idx].Call("ShardMaster.Query", args, &reply)
//				if ok && reply.WrongLeader == false {
//					ck.seqnum += 1
//					DPrintf("Client:[%d]:4.Query Client  GET QueryResponse success, num is %d, ck.SeqNo: %d, LeaderId:%d, reply.Config:%v", ck.id, num, ck.seqnum, ck.LeaderId, reply.Config)
//					workDoneChan<-reply.Config
//					//return reply.Config
//				}
//			}()
//			select {
//			case <-time.After(1000 * time.Millisecond):
//				goto lable
//			case resconfig := <-workDoneChan:
//				return  resconfig
//			}
//		lable:
//			DPrintf("Client:[%d]:5.Query Client  GET timeout")
//		}
//		//time.Sleep(100 * time.Millisecond)
//	}
//}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.id
	args.SeqNum = ck.seqnum

	for {
		// try each known server.
		for _,srv := range ck.servers {
			var reply JoinReply
			DPrintf("ShardMasterClient[%d]:1. send a  Join to leader. LeaderId:%d, ck.seqnum:%d, servers: %v", ck.id, ck.LeaderId,ck.seqnum,servers)
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.seqnum += 1
				DPrintf("ShardMasterClient[%d]:4.Join Client  GET JoinResponse success, ck.SeqNo: %d, LeaderId:%d", ck.id, ck.seqnum, srv)
				return
			}
			
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.id
	args.SeqNum = ck.seqnum
	for {
		// try each known server.
		for _,srv := range ck.servers {
			var reply LeaveReply
			DPrintf("ShardMasterClient[%d]:1. send a  Leave to leader. LeaderId:%d, ck.seqnum:%d, gids:%v", ck.id, ck.LeaderId,ck.seqnum,gids)
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.seqnum += 1
				DPrintf("ShardMasterClient[%d]:4.Leave Client  GET LeaveResponse success, ck.SeqNo: %d, LeaderId:%d", ck.id, ck.seqnum, srv)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.id
	args.SeqNum = ck.seqnum
	for {
		// try each known server.
		for _,srv := range ck.servers {
			var reply MoveReply
			DPrintf("ShardMasterClient[%d]:1. send a  Move to leader. LeaderId:%d, ck.seqnum:%d, shard:%v, gid:%v", ck.id, ck.LeaderId,ck.seqnum, shard, gid)
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.seqnum += 1
				DPrintf("ShardMasterClient[%d]:4.Move Client  GET MoveResponse success, ck.SeqNo: %d, LeaderId:%d", ck.id, ck.seqnum, srv)
				return
			}
			
		}
		time.Sleep(100 * time.Millisecond)
	}
}
