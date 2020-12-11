package main

import (
	"mydynamo"
	"strconv"
)

func main() {
	//Spin up a client for some testing
	//Expects server to be started, starting at port 8080
	serverPort := 8080

	//this client connects to the server on port 8080
	clientInstance := mydynamo.NewDynamoRPCClient("localhost:" + strconv.Itoa(serverPort+0))

	if err := clientInstance.CleanAndConn(); err != nil {
		panic(err)
	}

	//You can use the space below to write some operations that you want your client to do
}
