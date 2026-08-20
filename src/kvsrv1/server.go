package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu          sync.Mutex
	key2value   map[string]string
	key2version map[string]rpc.Tversion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, exists := kv.key2value[args.Key]

	if exists {
		reply.Value = value
		reply.Version = kv.key2version[args.Key]
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, exists := kv.key2value[args.Key]
	if exists {
		kv.updateExistingKey(args, reply)
	} else {
		kv.createNewKey(args, reply)
	}
}
func (kv *KVServer) updateExistingKey(args *rpc.PutArgs, reply *rpc.PutReply) {
	version := kv.key2version[args.Key]

	if version == args.Version {
		// version一致, 更新value和version
		kv.key2value[args.Key] = args.Value
		kv.key2version[args.Key]++
		reply.Err = rpc.OK
	} else {
		// version不一致
		reply.Err = rpc.ErrVersion
	}
}
func (kv *KVServer) createNewKey(args *rpc.PutArgs, reply *rpc.PutReply) {
	if kv.key2version[args.Key] == 0 {
		// version == 0, 创建tuple
		kv.key2value[args.Key] = args.Value
		kv.key2version[args.Key] = 1
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}

}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
