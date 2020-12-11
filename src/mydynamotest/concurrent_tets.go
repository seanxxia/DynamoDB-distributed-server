package mydynamotest

import (
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
)

var _ = Describe("Concurrent", func() {
	Describe("Multiple Servers", func() {
		const serverNum int = 10

		var sc ServerCoordinator
		BeforeEach(func() {
			// StartingPort: 8000, R-Value: 5, W-Value: 5, ClusterSize: 10
			sc = NewServerCoordinator(8000+config.GinkgoConfig.ParallelNode*100, 5, 5, serverNum)
		})

		AfterEach(func() {
			sc.Kill()
		})
		It("should handle concurrent put and gossip (2)", func(done Done) {

			sc.GetClient(5).Put(MakePutFreshEntry("k1", []byte("v1")))
			go func(serverID int) {
				defer GinkgoRecover()
				sc.GetClient(serverID).Put(MakePutFreshEntry("k1", []byte("v0")))
			}(0)

			go func(serverID int) {
				defer GinkgoRecover()
				sc.GetClient(serverID).Gossip()
			}(1)

			res0 := sc.GetClient(0).Get("k1")
			Expect(res0).NotTo(BeNil())
			Expect(GetEntryValues(res0)).To(ConsistOf([][]byte{
				[]byte("v0"),
			}))

			res1 := sc.GetClient(1).Get("k1")
			Expect(res1).NotTo(BeNil())
			Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
				[]byte("v0"),
				[]byte("v1"),
			}))

			close(done)
		}, 20.0)
	})

})
