package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"
import "time"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ClientId int
	SeqNo    int
	LeaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type singleton struct {
	ClientIdCh chan int
}

var instance * singleton
var once sync.Once

var ClientIdCh *singleton



func GetInstance() * singleton {
	var capacity int = 10000
    once.Do(func() {
		out := make(chan int,1)
		go func(){
			defer close(out)
			for i:=0; i< capacity; i++{
				out <- i
			}
		}()
        instance = & singleton{ClientIdCh: out}
    })
    return instance
}



func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	singleton := GetInstance()
	ck.SeqNo = 1
	ck.ClientId = <- singleton.ClientIdCh
	ck.LeaderId = len(servers)
	DPrintf("Client:MakeClerk:[%d]: Client %d  created", ck.ClientId, ck.ClientId)

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//设置一个RPC超时时间
//
func (ck *Clerk) Get(key string) string {

	//设置一个RPC请求超时，不然某个机器隔离的时候会无限制等待
	server_cnt := len(ck.servers)
	for{
		reply := new(GetReply)
		args := &GetArgs{Key: key, ClientId: ck.ClientId, SeqNo: ck.SeqNo}

		requestResponse := make(chan bool, 1)
		ck.LeaderId %= server_cnt
		//DPrintf("Client:[%d]:GET LeaderId is %d", ck.ClientId, ck.LeaderId)

		go func() {
			// DPrintf("Client:[%d]: send RPC to Server:[%d]. OP:GET  Key:%s \n", ck.ClientId, ck.LeaderId, key)
			ok := ck.servers[ck.LeaderId].Call("KVServer.Get", args, reply)
			requestResponse <- ok
			// DPrintf("Client:[%d]:  received RPC response from server %d .  OP:GET  key:%s \n", ck.ClientId, ck.LeaderId, key)
		}()

		//select 一直阻塞，直到触发一个通道
		select{
			case <-time.After(500*time.Millisecond):
				DPrintf("Client:[%d]: LeaderId is  %d GET TimeOut", ck.ClientId, ck.LeaderId)
				ck.LeaderId+=1
				continue
			case ok:= <-requestResponse:
				// DPrintf("Client:[%d]: Client  GET requestResponse, LeaderId: %d,  WrongLeader: %v ", ck.ClientId, ck.LeaderId, reply.WrongLeader)
				if ok && !reply.WrongLeader{
					// DPrintf("Client:[%d]: Client  GET requestResponse, LeaderId: %d,  WrongLeader: %v ", ck.ClientId, ck.LeaderId, reply.WrongLeader)
					ck.SeqNo+=1
					if reply.Err == OK {
						DPrintf("Client:[%d]: Client  GET requestResponse success, WrongLeader: %v LeaderId: %d, key is: %s value is %s, ck.SeqNo:%d", ck.ClientId, reply.WrongLeader, ck.LeaderId,key, reply.Value, ck.SeqNo)
						return reply.Value
					}
					//这里为啥返回空,因为目前error类型只有一种，既key对应的值不存在，就默认返回空
					return ""
				}
				ck.LeaderId+=1
		}

	}
	// You will have to modify this function.
	return ""
}



// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//设置一个RPC请求超时，不然某个机器隔离的时候会无限制等待
	server_cnt := len(ck.servers)
	for{

		reply := new(PutAppendReply)
		args := &PutAppendArgs{Key: key, Value: value,Op: op,SeqNo: ck.SeqNo, ClientId: ck.ClientId}

		requestResponse := make(chan bool,1)
		ck.LeaderId %= server_cnt

		DPrintf("Client:[%d]:GET LeaderId is %d", ck.ClientId, ck.LeaderId)


		go func () {
			DPrintf("Client:[%d]: send RPC to Server:[%d]. OP:%s  Key:%s \n", ck.ClientId, ck.LeaderId,op, key)
			ok := ck.servers[ck.LeaderId].Call("KVServer.PutAppend", args, reply)
			requestResponse <- ok
			DPrintf("Client:[%d]:  received RPC response from server %d .  OP:%s  key:%s \n", ck.ClientId, ck.LeaderId, op, key)
		}()

		//不断循环检测,对一个请求最多等待200ms

		select{
			case <-time.After(200*time.Millisecond):
				ck.LeaderId+=1
				DPrintf("Client:[%d]: PutAppend Client  timeOut from server %d . OP: %s .WrongLeader: %v \n", ck.ClientId, ck.LeaderId, op, reply.WrongLeader)
				continue
			case ok:= <-requestResponse:
				DPrintf("Client:[%d]: PutAppend Client  requestResponse  receive cmd from server %d . OP: %s .WrongLeader: %v \n", ck.ClientId, ck.LeaderId, op, reply.WrongLeader)
				if ok && !reply.WrongLeader && reply.Err == OK{
					ck.SeqNo+=1
					DPrintf("Client:[%d]: PutAppend Client  success  receive cmd from server %d . OP: %s .WrongLeader: %v \n", ck.ClientId, ck.LeaderId, op, reply.WrongLeader)
					return
				}
				ck.LeaderId+=1
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}


func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
