package mydynamotest

import (
	dy "mydynamo"
	"os/exec"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/gomega/gexec"
)

const SERVER_COORDINATOR_STARTUP_WAIT_SECONDS = 3

// The server coordinator type for test
type ServerCoordinator struct {
	session      *gexec.Session
	rpcClientMap map[int]*dy.RPCClient
	StartingPort int
	RValue       int
	WValue       int
	ClusterSize  int
}

// Create (run) a new server coordinator process with given configs.
func NewServerCoordinator(startingPort int, rValue int, wValue int, clusterSize int) ServerCoordinator {
	coordinatorCmd := exec.Command("DynamoTestCoordinator",
		strconv.Itoa(startingPort), strconv.Itoa(rValue), strconv.Itoa(wValue), strconv.Itoa(clusterSize))

	session, err := gexec.Start(coordinatorCmd, GinkgoWriter, GinkgoWriter)
	if err != nil {
		panic(err)
	}

	time.Sleep(SERVER_COORDINATOR_STARTUP_WAIT_SECONDS * time.Second)

	return ServerCoordinator{
		session:      session,
		rpcClientMap: make(map[int]*dy.RPCClient),
		StartingPort: startingPort,
		RValue:       rValue,
		WValue:       wValue,
		ClusterSize:  clusterSize,
	}
}

// Kill the server coordinator process.
func (s *ServerCoordinator) Kill() {
	var _ = s.session.Kill().Wait()
}

// Get the RPC client for the ith server created by server coordinator.
// The port for the server is computed by `s.StartingPort + serverIndex`.
func (s *ServerCoordinator) GetClient(serverIndex int) *dy.RPCClient {
	if client, ok := s.rpcClientMap[serverIndex]; !ok {
		client = dy.NewDynamoRPCClient("localhost:" + strconv.Itoa(s.StartingPort+serverIndex))
		client.RpcConnect()
		s.rpcClientMap[serverIndex] = client
	}

	return s.rpcClientMap[serverIndex]
}

// Get the server ID (nodeID) for the ith server created by server coordinator.
// This method is designed for testing vector clock
func (s *ServerCoordinator) GetID(serverIndex int) string {
	return "s" + strconv.Itoa(serverIndex)
}

//Creates a PutArgs with the associated key and value, but a Context corresponding
//to a new VectorClock
func MakePutFreshEntry(key string, value []byte) dy.PutArgs {
	return dy.NewPutArgs(key, dy.NewContext(dy.NewVectorClock()), value)
}

//Creates a PutArgs with the associated key and ObjectEntry
func MakePutFromEntry(key string, entry dy.ObjectEntry) dy.PutArgs {
	return dy.PutArgs{
		Key:     key,
		Context: entry.Context,
		Value:   entry.Value,
	}
}

//Creates a PutArgs with the associated key, vector clock map, and value
func MakePutFromVectorClockMapAndValue(key string, vectorClockMap map[string]uint64, value []byte) dy.PutArgs {
	return dy.PutArgs{
		Key: key,
		Context: dy.Context{
			Clock: NewVectorClockFromMap(vectorClockMap),
		},
		Value: value,
	}
}

// Creates a new VectorClock from local clock map (for testing only)
func NewVectorClockFromMap(localClocks map[string]uint64) dy.VectorClock {
	return dy.VectorClock{
		NodeClocks: localClocks,
	}
}

// Returns the list of DynamoResult's entry values. The order of the elements in the returned list
// is the same as the corresponding entries in DynamoResult's entry list. Two entries with same value but different
// context will lead to duplicated elements in the returned list.
func GetEntryValues(result *dy.DynamoResult) [][]byte {
	values := make([][]byte, 0)
	if result == nil || result.EntryList == nil {
		return values
	}

	for _, entry := range result.EntryList {
		values = append(values, entry.Value)
	}

	return values
}

// Returns the list of DynamoResult's entry clock of contexts. The order of the elements in the returned list
// is the same as the corresponding contexts in DynamoResult's entry list.
func GetEntryContextClocks(result *dy.DynamoResult) []dy.VectorClock {
	values := make([]dy.VectorClock, 0)
	if result == nil || result.EntryList == nil {
		return values
	}

	for _, entry := range result.EntryList {
		values = append(values, entry.Context.Clock)
	}

	return values
}

//

//Interate over the test cases array and apply the index and test case as arguments to the input function
func MapTestCases(testCases [][]string, f func(i int, c []string)) {
	for i, testCase := range testCases {
		f(i, testCase)
	}
}
