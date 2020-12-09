package mydynamo

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

type DynamoServer struct {
	/*------------Dynamo-specific-------------*/
	wValue          int          //Number of nodes to write to on each Put
	rValue          int          //Number of nodes to read from on each Get
	preferenceList  []DynamoNode //Ordered list of other Dynamo nodes to perform operations o
	selfNode        DynamoNode   //This node's address and port info
	nodeID          string       //ID of this node
	localEntriesMap ObjectEntriesMap
	crashTimeout    time.Time
}

// Returns error if the server is in crash state, otherwise nil
func (s *DynamoServer) checkCrashed() error {
	if time.Now().Before(s.crashTimeout) {
		return errors.New("Server is crashed")
	}

	return nil
}

func (s *DynamoServer) SendPreferenceList(incomingList []DynamoNode, _ *Empty) error {
	if err := s.checkCrashed(); err != nil {
		return err
	}

	s.preferenceList = incomingList
	return nil
}

// Forces server to gossip
// As this method takes no arguments, we must use the Empty placeholder.
// Replicates all keys and values from the current server to all other servers.
func (s *DynamoServer) Gossip(_ Empty, _ *Empty) error {
	if err := s.checkCrashed(); err != nil {
		return err
	}

	entryKeys := s.localEntriesMap.GetKeys()
	rpcClientMap := map[DynamoNode]*RPCClient{}

	for _, key := range entryKeys {
		for _, preferredDynamoNode := range s.preferenceList {
			if preferredDynamoNode == s.selfNode {
				continue
			}

			if _, ok := rpcClientMap[preferredDynamoNode]; !ok {
				rpcClient := NewDynamoRPCClientFromDynamoNode(preferredDynamoNode)
				defer rpcClient.CleanConn()

				rpcClientMap[preferredDynamoNode] = rpcClient
			}

			s.localEntriesMap.RLock(key)
			localEntries := s.localEntriesMap.Get(key)
			s.localEntriesMap.RUnlock(key)

			for _, localEntry := range localEntries {
				putArgs := PutArgs{
					Key:     key,
					Context: localEntry.Context,
					Value:   localEntry.Value,
				}

				rpcClientMap[preferredDynamoNode].PutRaw(putArgs)
			}
		}
	}
	return nil
}

// Makes server unavailable for some seconds
// NOTE: Do not use this method in tests. It is hard to control timing in test and may make the test unstable.
// NOTE: Use `DynamoServer.ForceCrash` and `DynamoServer.ForceRestore` instead.
func (s *DynamoServer) Crash(seconds int, success *bool) error {
	if err := s.checkCrashed(); err != nil {
		return err
	}

	s.crashTimeout = time.Now().Add(time.Second * time.Duration(seconds))
	*success = true
	return nil
}

// Makes server unavailable forever
func (s *DynamoServer) ForceCrash(_ Empty, _ *Empty) error {
	// Set server.crashTimeout to a long time after now (3 years)
	s.crashTimeout = time.Now().AddDate(3, 0, 0)
	return nil
}

// Makes server available
func (s *DynamoServer) ForceRestore(_ Empty, _ *Empty) error {
	// Set server.crashTimeout to a time before now (1 day before)
	s.crashTimeout = time.Now().AddDate(0, 0, -1)
	return nil
}

// Put a file to this server and W other servers
// Put will replicate the files to the first W nodes of its preference list. (spec)
// If enough nodes are crashed that there are not W available nodes, Put will simply attempt to Put
// to as many nodes as possible. (spec)
// Returns an error if there is an internal error.
// The parameter `result *bool` is set to true when successfully Put to W other servers, otherwise set to false.
// Reference:
// - https://piazza.com/class/kfqynl4r6a0317?cid=906
func (s *DynamoServer) Put(putArgs PutArgs, result *bool) error {
	if err := s.checkCrashed(); err != nil {
		return err
	}

	putArgs.Context.Clock.Increment(s.nodeID)
	if err := s.PutRaw(putArgs, result); err != nil {
		*result = false
		return err
	}

	wCount := 1
	for _, preferredDynamoNode := range s.preferenceList {
		if wCount >= s.wValue {
			break
		}
		if preferredDynamoNode == s.selfNode {
			continue
		}

		// TODO: Store success / fail results to reduce redundant data transfer in gossip
		rpcClient := NewDynamoRPCClientFromDynamoNode(preferredDynamoNode)
		defer rpcClient.CleanConn()
		if rpcClient.PutRaw(putArgs) {
			wCount++
		}
	}

	*result = wCount >= s.wValue
	return nil
}

// Put a file to this server
// This is an internal method used by this and other server to put file to this server (through RPC).
// Unlike method `DynamoServer.Put`, this method does not increment the vector clock nor
// replicate the file to other servers.
func (s *DynamoServer) PutRaw(putArgs PutArgs, result *bool) error {
	if err := s.checkCrashed(); err != nil {
		return err
	}

	key := putArgs.Key
	vClock := putArgs.Context.Clock
	value := putArgs.Value

	s.localEntriesMap.Lock(key)
	defer s.localEntriesMap.Unlock(key)

	localEntries := s.localEntriesMap.Get(key)

	indicesToRemove := make([]int, 0)
	for i, localEntry := range localEntries {
		if vClock.LessThan(localEntry.Context.Clock) || vClock.Equals(localEntry.Context.Clock) {
			*result = true
			return nil
		}

		if localEntry.Context.Clock.LessThan(vClock) {
			indicesToRemove = append(indicesToRemove, i)
		}
	}

	for i := len(indicesToRemove) - 1; i >= 0; i-- {
		localEntries = remove(localEntries, indicesToRemove[i])
	}

	localEntries = append(localEntries, ObjectEntry{
		Context: NewContext(vClock),
		Value:   value,
	})

	s.localEntriesMap.Put(key, localEntries)

	*result = true
	return nil
}

// Get a file from this server, matched with R other servers
// Get will get files from the top R nodes of its preference list. (spec)
func (s *DynamoServer) Get(key string, result *DynamoResult) error {
	if err := s.checkCrashed(); err != nil {
		return err
	}

	if err := s.GetRaw(key, result); err != nil {
		return err
	}

	rCount := 1
	for _, preferredDynamoNode := range s.preferenceList {
		if rCount >= s.rValue {
			break
		}
		if preferredDynamoNode == s.selfNode {
			continue
		}

		// TODO: Reuse RPC client
		rpcClient := NewDynamoRPCClientFromDynamoNode(preferredDynamoNode)
		defer rpcClient.CleanConn()

		remoteResult := DynamoResult{EntryList: nil}
		if rpcClient.GetRaw(key, &remoteResult) {
			rCount++

			// Iterate over remote entries and add concurrent entries to result
			// TODO: Improve performance
			for _, remoteEntry := range remoteResult.EntryList {
				isRemoteEntryConcurrent := true
				indicesToRemove := make([]int, 0)
				for i, localEntry := range result.EntryList {
					if localEntry.Context.Clock.LessThan(remoteEntry.Context.Clock) {
						indicesToRemove = append(indicesToRemove, i)
					} else if !remoteEntry.Context.Clock.Concurrent(localEntry.Context.Clock) {
						isRemoteEntryConcurrent = false
					}
				}

				for i := len(indicesToRemove) - 1; i >= 0; i-- {
					result.EntryList = remove(result.EntryList, indicesToRemove[i])
				}
				if isRemoteEntryConcurrent {
					result.EntryList = append(result.EntryList, remoteEntry)
				}
			}
		}
	}

	return nil
}

// Get a file from this server
func (s *DynamoServer) GetRaw(key string, result *DynamoResult) error {
	if err := s.checkCrashed(); err != nil {
		return err
	}

	result.EntryList = make([]ObjectEntry, 0)

	s.localEntriesMap.RLock(key)
	defer s.localEntriesMap.RUnlock(key)

	result.EntryList = append(result.EntryList, s.localEntriesMap.Get(key)...)

	return nil
}

/* Belows are functions that implement server boot up and initialization */
func NewDynamoServer(w int, r int, hostAddr string, hostPort string, id string) DynamoServer {
	preferenceList := make([]DynamoNode, 0)
	selfNodeInfo := DynamoNode{
		Address: hostAddr,
		Port:    hostPort,
	}

	return DynamoServer{
		wValue:          w,
		rValue:          r,
		preferenceList:  preferenceList,
		selfNode:        selfNodeInfo,
		nodeID:          id,
		localEntriesMap: NewObjectEntriesMap(),
		crashTimeout:    time.Now().AddDate(0, 0, -1),
	}
}

func ServeDynamoServer(dynamoServer DynamoServer) error {
	rpcServer := rpc.NewServer()
	e := rpcServer.RegisterName("MyDynamo", &dynamoServer)
	if e != nil {
		log.Println(DYNAMO_SERVER, "Server Can't start During Name Registration")
		return e
	}

	log.Println(DYNAMO_SERVER, "Successfully Registered the RPC Interfaces")

	l, e := net.Listen("tcp", dynamoServer.selfNode.Address+":"+dynamoServer.selfNode.Port)
	if e != nil {
		log.Println(DYNAMO_SERVER, "Server Can't start During Port Listening")
		return e
	}

	log.Println(DYNAMO_SERVER, "Successfully Listening to Target Port ", dynamoServer.selfNode.Address+":"+dynamoServer.selfNode.Port)
	log.Println(DYNAMO_SERVER, "Serving Server Now")

	return http.Serve(l, rpcServer)
}
