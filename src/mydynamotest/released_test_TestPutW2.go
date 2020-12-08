package mydynamotest

import (
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
)

var _ = Describe("Basic Put & Get", func() {

	Describe("Two Servers", func() {

		var sc ServerCoordinator

		BeforeEach(func() {
			// StartingPort: 8080, R-Value: 2, W-Value: 2, ClusterSize: 5
			sc = NewServerCoordinator(8080+config.GinkgoConfig.ParallelNode*100, 2, 2, 5)
		})

		AfterEach(func() {
			sc.Kill()
		})

		It("should handle entry put and get", func() {
			sc.GetClient(0).Put(MakePutFreshEntry("s1", []byte("abcde")))

			res1 := sc.GetClient(1).Get("s1")
			Expect(res1).NotTo(BeNil())
			Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
				[]byte("abcde"),
			}))
		})

	})

})
