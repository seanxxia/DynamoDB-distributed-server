package mydynamotest

import (
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
)

var _ = Describe("Gossip", func() {

	Describe("Five Servers", func() {

		var sc ServerCoordinator

		BeforeEach(func() {
			// StartingPort: 8080, R-Value: 1, W-Value: 1, ClusterSize: 5
			sc = NewServerCoordinator(8080+config.GinkgoConfig.ParallelNode*100, 1, 1, 5)
		})

		AfterEach(func() {
			sc.Kill()
		})

		It("should replicate entry to other server.", func() {
			sc.GetClient(0).Put(MakePutFreshEntry("s1", []byte("abcde")))
			sc.GetClient(0).Gossip()

			res1 := sc.GetClient(1).Get("s1")
			Expect(res1).NotTo(BeNil())
			Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
				[]byte("abcde"),
			}))

		})

	})

})