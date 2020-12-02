package mydynamotest

import (
	"bytes"
	dy "mydynamo"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"time"
)

const SERVER_COORDINATOR_STARTUP_WAIT_SECONDS = 3

// The server coordinator type for test
type ServerCoordinator struct {
	cmd          *exec.Cmd
	clientMap    map[int]*dy.RPCClient
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
		clientMap:    make(map[int]*dy.RPCClient),
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
func (s *ServerCoordinator) GetClient(serverIndex int) *dy.RPCClient {
	if client, ok := s.clientMap[serverIndex]; !ok {
		client = dy.NewDynamoRPCClient("localhost:" + strconv.Itoa(s.StartingPort+serverIndex))
		client.RpcConnect()
		s.clientMap[serverIndex] = client
	}

	return s.clientMap[serverIndex]
}

// Get the server ID (nodeID) for the ith server created by server coordinator.
// This method is designed for testing vector clock
func (s *ServerCoordinator) GetServerID(serverIndex int) string {
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

// The pair type for representing ObjectEntry in tests
type Pair struct {
	ClockMap map[string]uint64
	Value    []byte
}

// Creates DynamoResult from pairs of vector clock and value
func MakeResult(pairs []Pair) dy.DynamoResult {
	entries := make([]dy.ObjectEntry, 0)

	for _, pair := range pairs {
		entries = append(entries, dy.ObjectEntry{
			Context: dy.Context{
				Clock: dy.NewVectorClockFromMap(pair.ClockMap),
			},
			Value: pair.Value,
		})
	}

	return SortResultEntries(&dy.DynamoResult{
		EntryList: entries,
	})
}

func SortResultEntries(result *dy.DynamoResult) dy.DynamoResult {
	getVectorClockBinary := func(clock dy.VectorClock) []byte {
		var data string

		clockMap := clock.ToMap()
		keys := make([]string, 0)
		for key := range clockMap {
			keys = append(keys, key)
		}

		sort.Strings(keys)
		for _, key := range keys {
			data += key + ";,=,;" + strconv.FormatInt(int64(clockMap[key]), 16) + "?:=:?"
		}

		return []byte(data)
	}

	getEntryBinary := func(entry dy.ObjectEntry) []byte {
		data := getVectorClockBinary(entry.Context.Clock)
		data = append(data, []byte("-+.;.+-")...)
		data = append(data, entry.Value...)
		return data
	}

	sort.SliceStable(result.EntryList, func(i, j int) bool {
		a := getEntryBinary(result.EntryList[i])
		b := getEntryBinary(result.EntryList[j])
		return bytes.Compare(a, b) < 0
	})

	return *result
}

//Interate over the test cases array and apply the index and test case as arguments to the input function
func MapTestCases(testCases [][]string, f func(i int, c []string)) {
	for i, testCase := range testCases {
		f(i, testCase)
	}
}
