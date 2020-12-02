package mydynamotest

import (
	"mydynamo"
	"os"
	"os/exec"
	"strconv"
	"time"
)

const SERVER_COORDINATOR_STARTUP_WAIT_SECONDS = 3

// The server coordinator type for test
type ServerCoordinator struct {
	cmd          *exec.Cmd
	clientMap    map[int]*mydynamo.RPCClient
	StartingPort int
	RValue       int
	WValue       int
	ClusterSize  int
}

// Create (run) a new server coordinator process with given configs.
func NewServerCoordinator(startingPort int, rValue int, wValue int, clusterSize int) ServerCoordinator {
	coordinatorCmd := exec.Command("DynamoTestCoordinator",
		strconv.Itoa(startingPort), strconv.Itoa(rValue), strconv.Itoa(wValue), strconv.Itoa(clusterSize))

	coordinatorCmd.Stderr = os.Stderr
	coordinatorCmd.Stdout = os.Stdout

	err := coordinatorCmd.Start()
	if err != nil {
		panic(err)
	}

	time.Sleep(SERVER_COORDINATOR_STARTUP_WAIT_SECONDS * time.Second)

	return ServerCoordinator{
		cmd:          coordinatorCmd,
		clientMap:    make(map[int]*mydynamo.RPCClient),
		StartingPort: startingPort,
		RValue:       rValue,
		WValue:       wValue,
		ClusterSize:  clusterSize,
	}
}

// Kill the server coordinator process.
func (s *ServerCoordinator) Kill() {
	if s.cmd == nil {
		return
	}

	_ = s.cmd.Process.Kill()
}

// Get the RPC client for the ith server created by server coordinator.
// The port for the server is computed by `s.StartingPort + serverIndex`.
func (s *ServerCoordinator) GetClient(serverIndex int) *mydynamo.RPCClient {
	if client, ok := s.clientMap[serverIndex]; !ok {
		client = mydynamo.NewDynamoRPCClient("localhost:" + strconv.Itoa(s.StartingPort+serverIndex))
		client.RpcConnect()
		s.clientMap[serverIndex] = client
	}

	return s.clientMap[serverIndex]
}

//Creates a PutArgs with the associated key and value, but a Context corresponding
//to a new VectorClock
func MakePutFreshContext(key string, value []byte) mydynamo.PutArgs {
	return mydynamo.NewPutArgs(key, mydynamo.NewContext(mydynamo.NewVectorClock()), value)
}

//Interate over the test cases array and apply the index and test case as arguments to the input function
func MapTestCases(testCases [][]string, f func(i int, c []string)) {
	for i, testCase := range testCases {
		f(i, testCase)
	}
}
