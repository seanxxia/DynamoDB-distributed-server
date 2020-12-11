package mydynamo

import (
	"log"
	"net/rpc"
)

type RPCClient struct {
	ServerAddr string
	rpcConn    *rpc.Client
}

//Removes the RPC connection associated with this client
func (dynamoClient *RPCClient) CleanConn() {
	var e error
	if dynamoClient.rpcConn != nil {
		e = dynamoClient.rpcConn.Close()
		if e != nil {
			log.Println("CleanConnError", e)
		}
	}
	dynamoClient.rpcConn = nil
}

//Establishes an RPC connection to the server this Client is associated with
func (dynamoClient *RPCClient) RpcConnect() error {
	if dynamoClient.rpcConn != nil {
		return nil
	}

	var e error

	for i := 0; i < RPC_CLIENT_CONNECT_RETRY_MAX; i++ {
		dynamoClient.rpcConn, e = rpc.DialHTTP("tcp", dynamoClient.ServerAddr)
		if e == nil {
			break
		}
	}

	if e != nil {
		dynamoClient.rpcConn = nil
	}
	return e
}

//Removes and re-establishes an RPC connection to the server
func (dynamoClient *RPCClient) CleanAndConn() error {
	var e error
	if dynamoClient.rpcConn != nil {
		e = dynamoClient.rpcConn.Close()
		if e != nil {
			log.Println("CleanConnError", e)
		}
	}

	dynamoClient.rpcConn = nil
	e = dynamoClient.RpcConnect()
	return e
}

//Puts a value to the server.
func (dynamoClient *RPCClient) Put(value PutArgs) bool {
	var result bool
	if dynamoClient.rpcConn == nil {
		return false
	}
	err := dynamoClient.rpcConn.Call("MyDynamo.Put", value, &result)
	if err != nil {
		log.Println(err)
		return false
	}
	return result
}

//Puts a value to the server without incrementing clock and replicating to other servers.
func (dynamoClient *RPCClient) PutRaw(value PutArgs) bool {
	var result bool
	if dynamoClient.rpcConn == nil {
		return false
	}
	err := dynamoClient.rpcConn.Call("MyDynamo.PutRaw", value, &result)
	if err != nil {
		log.Println(err)
		return false
	}
	return result
}

//Gets a value from a server.
func (dynamoClient *RPCClient) Get(key string) *DynamoResult {
	var result DynamoResult
	if dynamoClient.rpcConn == nil {
		return nil
	}
	err := dynamoClient.rpcConn.Call("MyDynamo.Get", key, &result)
	if err != nil {
		log.Println(err)
		return nil
	}
	return &result
}

//Gets a value from a server.
func (dynamoClient *RPCClient) GetRaw(key string, result *DynamoResult) bool {
	if dynamoClient.rpcConn == nil {
		return false
	}
	err := dynamoClient.rpcConn.Call("MyDynamo.GetRaw", key, result)
	if err != nil {
		log.Println(err)
		return false
	}
	return true
}

//Emulates a crash on the server this client is connected to
func (dynamoClient *RPCClient) Crash(seconds int) bool {
	if dynamoClient.rpcConn == nil {
		return false
	}
	var success bool
	err := dynamoClient.rpcConn.Call("MyDynamo.Crash", seconds, &success)
	if err != nil {
		log.Println(err)
		return false
	}
	return success
}

//Emulates a crash on the server this client is connected to
func (dynamoClient *RPCClient) ForceCrash() {
	if dynamoClient.rpcConn == nil {
		return
	}

	var v Empty
	err := dynamoClient.rpcConn.Call("MyDynamo.ForceCrash", v, &v)
	if err != nil {
		log.Println(err)
		return
	}
}

//Make the server restore from the emulated crash state
func (dynamoClient *RPCClient) ForceRestore() {
	if dynamoClient.rpcConn == nil {
		return
	}

	var v Empty
	err := dynamoClient.rpcConn.Call("MyDynamo.ForceRestore", v, &v)
	if err != nil {
		log.Println(err)
		return
	}
}

//Instructs the server this client is connected to gossip
func (dynamoClient *RPCClient) Gossip() {
	if dynamoClient.rpcConn == nil {
		return
	}
	var v Empty
	err := dynamoClient.rpcConn.Call("MyDynamo.Gossip", v, &v)
	if err != nil {
		log.Println(err)
		return
	}
}

//Creates a new DynamoRPCClient
func NewDynamoRPCClient(serverAddr string) *RPCClient {
	return &RPCClient{
		ServerAddr: serverAddr,
		rpcConn:    nil,
	}
}

//Creates a new DynamoRPCClient from DynamoNode (address and port) and establishes the RPC connection
func NewDynamoRPCClientFromDynamoNodeAndConnect(node DynamoNode) *RPCClient {
	client := NewDynamoRPCClient(node.Address + ":" + node.Port)

	if err := client.CleanAndConn(); err != nil {
		panic(err)
	}
	return client
}
