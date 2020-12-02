package mydynamo

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type DynamoServer struct {
	/*------------Dynamo-specific-------------*/
	wValue                int          //Number of nodes to write to on each Put
	rValue                int          //Number of nodes to read from on each Get
	preferenceList        []DynamoNode //Ordered list of other Dynamo nodes to perform operations o
	selfNode              DynamoNode   //This node's address and port info
	nodeID                string       //ID of this node
	objectEntriesMap      map[string][]ObjectEntry
	objectEntriesMapMutex sync.Mutex
	crashTimeout          time.Time
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

	// TODO: Use fine-grained lock (Do we need read lock here?)
	s.objectEntriesMapMutex.Lock()
	defer s.objectEntriesMapMutex.Unlock()

	for _, preferedDynamoNode := range s.preferenceList {
		dynamoRPCClient := NewDynamoRPCClientFromDynamoNode(preferedDynamoNode)
		for key, objectEntries := range s.objectEntriesMap {
			for _, objectEntry := range objectEntries {
				putArgs := PutArgs{
					Key:     key,
					Context: objectEntry.Context,
					Value:   objectEntry.Value,
				}
				dynamoRPCClient.PutRaw(putArgs)
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

	wCount := 0
	for _, preferedDynamoNode := range s.preferenceList {
		if wCount >= s.wValue {
			break
		}

		// TODO: Reuse RPC client
		// TODO: Store success / fail results to reduce redundant data transfer in gossip
		dynamoRPCClient := NewDynamoRPCClientFromDynamoNode(preferedDynamoNode)
		if dynamoRPCClient.PutRaw(putArgs) {
			wCount++
		}
	}

	if wCount < s.wValue {
		*result = false
	}
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

	// TODO: Use fine-grained lock
	s.objectEntriesMapMutex.Lock()
	defer s.objectEntriesMapMutex.Unlock()

	objectEntries, isObjectEntriesExisted := s.objectEntriesMap[key]
	if !isObjectEntriesExisted {
		s.objectEntriesMap[key] = []ObjectEntry{
			ObjectEntry{
				Context: NewContext(vClock),
				Value:   value,
			},
		}

		return nil
	}

	for i := 0; i < len(objectEntries); {
		objectEntry := objectEntries[i]
		if vClock.LessThan(objectEntry.Context.Clock) || vClock.Equals(objectEntry.Context.Clock) {
			return nil
		}

		if objectEntry.Context.Clock.LessThan(vClock) {
			remove(objectEntries, i)
		} else {
			// vClock.Concurrent(objectEntry.Context.Clock) == true
			i++
		}
	}

	objectEntries = append(objectEntries, ObjectEntry{
		Context: NewContext(vClock),
		Value:   value,
	})
	s.objectEntriesMap[key] = objectEntries

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

	rCount := 0
	for _, preferedDynamoNode := range s.preferenceList {
		if rCount >= s.rValue {
			break
		}

		// TODO: Reuse RPC client
		dynamoRPCClient := NewDynamoRPCClientFromDynamoNode(preferedDynamoNode)
		remoteResult := DynamoResult{EntryList: nil}
		if dynamoRPCClient.GetRaw(key, &remoteResult) {
			rCount++

			// Iterate over remote entries and add concurrent entries to result
			// TODO: Improve performance
			for _, remoteEntry := range remoteResult.EntryList {
				isRemoteEntryConcurrent := true
				for _, localEntry := range result.EntryList {
					if !remoteEntry.Context.Clock.Concurrent(localEntry.Context.Clock) {
						isRemoteEntryConcurrent = false
						break
					}
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

	// TODO: Use fine-grained lock (Do we need read lock here?)
	s.objectEntriesMapMutex.Lock()
	defer s.objectEntriesMapMutex.Unlock()

	if objectEntries, ok := s.objectEntriesMap[key]; ok {
		result.EntryList = append(result.EntryList, objectEntries...)
	}

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
		wValue:           w,
		rValue:           r,
		preferenceList:   preferenceList,
		selfNode:         selfNodeInfo,
		nodeID:           id,
		objectEntriesMap: make(map[string][]ObjectEntry),
		crashTimeout:     time.Now().AddDate(0, 0, -1),
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
