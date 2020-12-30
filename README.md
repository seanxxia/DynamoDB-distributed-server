# DynamoDB

![Lint & Format](https://github.com/summer110669/module-4-project-cse224-chan-xia/workflows/Lint%20&%20Format/badge.svg)
![Test](https://github.com/summer110669/module-4-project-cse224-chan-xia/workflows/Test/badge.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

<div align="center"><img width="300" src="./ucsd-logo.png" /></div>

An implementation of distributed key-value store based on Amazon’s DynamoDB. This key-value store will have a gossip-based replication system, as well as a configurable quorum-type system for reads and writes. DynamoDB makes no effort to resolve conflicts in writes made by different nodes outside of direct causality, and will simply store and return multiple values if concurrent, conflicting writes are made. Though the original variant of DynamoDB uses consistent hashing, we will not be using it in this project. In addition, the original method which gossips periodically, but it is now invoked only when the client calls the function to simply the testing strategy.

## Usage
### Step 0: Install Go and Node.js

You need to [install Go](https://golang.org/doc/install) to build and run the web server.

The tests are written in JavaScript. To run the tests locally, you need to install [Node.js](https://nodejs.org/en/).


### Step 1: Set environment variables

If you have Node.js installed locally, there are npm scripts to build the project and run the test that sets the environment variables automatically. To use the npm scripts, you need to install the dependencies first with the command:
```
npm install
```

Otherwise, you need to add directory into ```.bash_profile``` (for OS X environment) or ```.bashrc``` to let the compiler knows where to find the dependencies
```
export PATH=$PATH:/usr/local/go/bin     # making sure go is on path
export GOPATH=<path-to-repo>
export PATH=$PATH:$GOPATH/bin
 ```
example:
```
export GOPATH=/[The directory you put this folder]/Project-1/
export PATH=$PATH:$GOPATH/bin
```

### Step 2: Build and run

```shell
./build.sh
```
```shell
./run-server.sh [config file]
```
where `config file` is a .ini file. We have provided `myconfig.ini` as an example config file for you to use.
To run your server in the background, you can use
```
nohup ./run-server.sh [config file] &
```
This will start your server in the background and append the output to a file called `nohup.out`

To run your client, run
```
./run-client.sh
```

### Step 3: Testing
To run the test, you need to install the Node.js test dependencies first with the following command:
```
npm install
```
The test is also available through npm script. With the following command, it will automatically build the code and run the test suites.
```
npm run test
```

## Project Structure

1. `Dynamo_Types.go` has structures that define the VectorClock, DynamoNode, and the function for Put and Get.
2. `Dynamo_VectorClock.go` has helper methods to handle different scenarios of VectorClock.
3. `Dynamo_Server.go` has methods that defines an RPC interface for a Dynamo node.
4. `Dynamo_RPCClient.go` provides the rpc client stub for the surfstore rpc server.
5. `Dynamo_Utils.go` has utility functions.
