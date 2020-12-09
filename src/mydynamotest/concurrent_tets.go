package mydynamotest

import (
	"fmt"
	"sync"

	dy "mydynamo"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
)

const RANDOM_DATA_BYTES int = 1024
const CONCURRENT_CLIENTS_NUM int = 100

var _ = Describe("Concurrent", func() {

	Describe("Single Server", func() {
		var sc ServerCoordinator

		BeforeEach(func() {
			// StartingPort: 8000, R-Value: 1, W-Value: 1, ClusterSize: 1
			sc = NewServerCoordinator(8000+config.GinkgoConfig.ParallelNode*100, 1, 1, 1)
		})

		AfterEach(func() {
			sc.Kill()
		})

		It("should handle concurrent entries put with different keys.", func(done Done) {
			var wg sync.WaitGroup

			wg.Add(CONCURRENT_CLIENTS_NUM)
			for i := 0; i < CONCURRENT_CLIENTS_NUM; i++ {
				go func(i int) {
					defer GinkgoRecover()

					client := sc.MakeNewClient(0)
					defer client.CleanConn()

					key := fmt.Sprintf("k%d", i)
					value := MakeRandomBytes(RANDOM_DATA_BYTES)
					client.Put(MakePutFreshEntry(key, value))

					res := client.Get(key)

					Expect(res).NotTo(BeNil())
					Expect(GetEntryValues(res)).To(ConsistOf([][]byte{value}))

					wg.Done()
				}(i)
			}

			wg.Wait()
			close(done)
		}, 20.0)

		It("should handle concurrent entries put with the same key.", func(done Done) {
			var wg sync.WaitGroup

			wg.Add(CONCURRENT_CLIENTS_NUM)
			for i := 0; i < CONCURRENT_CLIENTS_NUM; i++ {
				go func(i int) {
					defer GinkgoRecover()

					RandomSleep()

					client := sc.MakeNewClient(0)
					defer client.CleanConn()

					res := client.Get("key")

					var putArgs dy.PutArgs
					if len(res.EntryList) == 0 {
						putArgs = MakePutFreshEntry("key", MakeRandomBytes(RANDOM_DATA_BYTES))
					} else {
						entry := res.EntryList[0]
						entry.Value = MakeRandomBytes(RANDOM_DATA_BYTES)
						putArgs = MakePutFromEntry("key", entry)
					}

					client.Put(putArgs)
					wg.Done()
				}(i)
			}
			wg.Wait()

			// Ignore all the previously put entries, get and put a new entry here
			value := MakeRandomBytes(RANDOM_DATA_BYTES)

			res := sc.GetClient(0).Get("key")
			entry := res.EntryList[0]
			entry.Value = value
			sc.GetClient(0).Put(MakePutFromEntry("key", entry))

			res = sc.GetClient(0).Get("key")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{value}))

			close(done)
		}, 20.0)
	})

	Describe("Multiple Servers", func() {
		var sc ServerCoordinator
		const serverNum = 10

		BeforeEach(func() {
			// StartingPort: 8000, R-Value: 5, W-Value: 5, ClusterSize: 10
			sc = NewServerCoordinator(8000+config.GinkgoConfig.ParallelNode*100, 5, 5, serverNum)
		})

		AfterEach(func() {
			sc.Kill()
		})

		It("should handle concurrent put and gossip", func(done Done) {
			var wg sync.WaitGroup

			expectedEntryValues := make([][]byte, 0, serverNum)
			for i := 0; i < serverNum; i++ {
				expectedEntryValues = append(expectedEntryValues, MakeRandomBytes(RANDOM_DATA_BYTES))
			}

			wg.Add(serverNum)
			for serverID := 0; serverID < serverNum; serverID++ {
				go func(serverID int) {
					defer GinkgoRecover()

					client := sc.MakeNewClient(serverID)
					defer client.CleanConn()
					client.Put(MakePutFreshEntry("key", expectedEntryValues[serverID]))

					for i := 0; i < 10; i++ {
						client.Gossip()
					}
					wg.Done()
				}(serverID)
			}
			wg.Wait()

			wg.Add(serverNum)
			for serverID := 0; serverID < serverNum; serverID++ {
				go func(serverID int) {
					defer GinkgoRecover()

					client := sc.MakeNewClient(serverID)
					defer client.CleanConn()

					res := client.Get("key")
					Expect(res).NotTo(BeNil())
					Expect(GetEntryValues(res)).To(ConsistOf(expectedEntryValues))

					wg.Done()
				}(serverID)
			}
			wg.Wait()

			close(done)
		}, 20.0)

		It("should not deadline and handle concurrent get, put, and gossip.", func(done Done) {
			const operationNum = 10

			var wg sync.WaitGroup
			wg.Add(CONCURRENT_CLIENTS_NUM * serverNum * 3)

			// Getters
			for clientID := 0; clientID < CONCURRENT_CLIENTS_NUM; clientID++ {
				for serverID := 0; serverID < serverNum; serverID++ {
					go func(clientID int, serverID int) {
						defer GinkgoRecover()

						client := sc.MakeNewClient(serverID)
						defer client.CleanConn()

						for opi := 0; operationNum < 10; opi++ {
							RandomSleep()

							// Do get
							key := fmt.Sprintf("key-%d", clientID)
							res := client.Get(key)
							Expect(res).NotTo(BeNil())
						}
						wg.Done()
					}(clientID, serverID)
				}
			}

			// Putters
			for clientID := 0; clientID < CONCURRENT_CLIENTS_NUM; clientID++ {
				for serverID := 0; serverID < serverNum; serverID++ {
					go func(clientID int, serverID int) {
						defer GinkgoRecover()

						client := sc.MakeNewClient(serverID)
						defer client.CleanConn()

						for opi := 0; operationNum < 10; opi++ {
							RandomSleep()

							// Do get and put
							key := fmt.Sprintf("key-%d", clientID)

							res := client.Get(key)
							var putArgs dy.PutArgs
							if len(res.EntryList) == 0 {
								putArgs = MakePutFreshEntry(key, MakeRandomBytes(RANDOM_DATA_BYTES))
							} else {
								entry := res.EntryList[0]
								entry.Value = MakeRandomBytes(RANDOM_DATA_BYTES)
								putArgs = MakePutFromEntry(key, entry)
							}

							client.Put(putArgs)
						}
						wg.Done()
					}(clientID, serverID)
				}
			}

			// Gossiper
			for clientID := 0; clientID < CONCURRENT_CLIENTS_NUM; clientID++ {
				for serverID := 0; serverID < serverNum; serverID++ {
					go func(clientID int, serverID int) {
						defer GinkgoRecover()

						client := sc.MakeNewClient(serverID)
						defer client.CleanConn()

						for opi := 0; operationNum < 10; opi++ {
							RandomSleep()

							// Do get and put
							client.Gossip()
						}
						wg.Done()
					}(clientID, serverID)
				}
			}

			wg.Wait()
			close(done)
		}, 60.0)
	})
})
