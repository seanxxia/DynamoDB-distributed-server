package main

import (
	"fmt"
	"log"
	"mydynamo"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	ARG_COUNT                      = 5
	CONFIG_STARTING_PORT_ARG_INDEX = 1
	CONFIG_R_VALUE_ARG_INDEX       = 2
	CONFIG_W_VALUE_ARG_INDEX       = 3
	CONFIG_CLUSTER_SIZE_ARG_INDEX  = 4
	SERVER_STARTUP_WAIT_SECONDS    = 2
)

func main() {
	var err error
	/*-----------------------------*/
	// When the input argument is less than 1
	if len(os.Args) != ARG_COUNT {
		log.Println(mydynamo.USAGE_STRING)
		os.Exit(mydynamo.EX_USAGE)
	}

	// Load the configuration file
	config := make(map[string]int)
	config["starting_port"], err = strconv.Atoi(os.Args[CONFIG_STARTING_PORT_ARG_INDEX])
	config["r_value"], err = strconv.Atoi(os.Args[CONFIG_R_VALUE_ARG_INDEX])
	config["w_value"], err = strconv.Atoi(os.Args[CONFIG_W_VALUE_ARG_INDEX])
	config["cluster_size"], err = strconv.Atoi(os.Args[CONFIG_CLUSTER_SIZE_ARG_INDEX])

	fmt.Println("Done loading configurations: ", config)

	//keep a list of servers so we can communicate with them
	serverList := make([]mydynamo.DynamoServer, 0)

	//spin up a dynamo cluster
	dynamoNodeList := make([]mydynamo.DynamoNode, 0)

	//Use a waitgroup to ensure that we don't exit this goroutine until all servers have exited
	wg := new(sync.WaitGroup)
	wg.Add(config["cluster_size"])
	for idx := 0; idx < config["cluster_size"]; idx++ {

		//Create a server instance
		serverInstance := mydynamo.NewDynamoServer(config["w_value"], config["r_value"], "localhost", strconv.Itoa(config["starting_port"]+idx), strconv.Itoa(idx))
		serverList = append(serverList, serverInstance)

		//Create an anonymous function in a goroutine that starts the server
		go func() {
			log.Fatal(mydynamo.ServeDynamoServer(serverInstance))
			wg.Done()
		}()
		nodeInfo := mydynamo.DynamoNode{
			Address: "localhost",
			Port:    strconv.Itoa(config["starting_port"] + idx),
		}
		dynamoNodeList = append(dynamoNodeList, nodeInfo)
	}

	//Create a duplicate of dynamoNodeList that we can rotate
	//so that each node has a distinct preference list
	nodePreferenceList := dynamoNodeList

	// Wait for server to start
	time.Sleep(SERVER_STARTUP_WAIT_SECONDS * time.Second)

	//Send the preference list to all servers
	for _, info := range dynamoNodeList {
		var empty mydynamo.Empty
		c, _ := rpc.DialHTTP("tcp", info.Address+":"+info.Port)
		if err != nil {
			log.Println("Failed to send preference list")
		} else {
			err2 := c.Call("MyDynamo.SendPreferenceList", nodePreferenceList, &empty)
			if err2 != nil {
				log.Println("Failed to send preference list")
			}
		}
		nodePreferenceList = mydynamo.RotateServerList(nodePreferenceList)
	}
	/*---------------------------------------------*/

	//wait for all servers to finish
	wg.Wait()
}
