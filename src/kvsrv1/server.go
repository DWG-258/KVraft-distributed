package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ValueVersion struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu       sync.Mutex
	KeyValue map[string]ValueVersion
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.KeyValue = make(map[string]ValueVersion)
	//open a server
	// server := labrpc.MakeServer()
	// service := labrpc.MakeService(kv)
	// server.AddService(service)

	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	result, ok := kv.KeyValue[args.Key]
	if ok {

		reply.Value = result.value
		reply.Version = result.version
		reply.Err = rpc.OK
	} else {

		reply.Err = rpc.ErrNoKey
	}
	// Your code here.
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	result, ok := kv.KeyValue[args.Key]
	// print("Put called with key: " + args.Key + " value: " + args.Value + " version: " + strconv.FormatUint(uint64(args.Version), 10) + "\n")
	if ok {
		//key exists
		if result.version == args.Version {
			//version match , update value
			kv.KeyValue[args.Key] = ValueVersion{value: args.Value, version: result.version + 1}
			reply.Err = rpc.OK
		} else {
			//version mismatch
			reply.Err = rpc.ErrVersion

		}

	} else {
		//key doesn't exist
		if args.Version == 0 {

			kv.KeyValue[args.Key] = ValueVersion{value: args.Value, version: 1}
			reply.Err = rpc.OK
		} else if args.Version > 0 {
			reply.Err = rpc.ErrNoKey
		}

	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
