package mydynamotest

import (
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
)

var _ = Describe("TestInvalidPut", func() {

	Describe("Five Servers", func() {

		var sc ServerCoordinator

		BeforeEach(func() {
			// StartingPort: 8080, R-Value: 2, W-Value: 2, ClusterSize: 5
			sc = NewServerCoordinator(8080+config.GinkgoConfig.ParallelNode*100, 2, 2, 5)
		})

		AfterEach(func() {
			sc.Kill()
		})

		It("should ignore put with ancestor context.", func() {
			// Spec: If the context that the Node has already stored associated with the
			// specified key is causally descended from the context provided to Put, i.e
			// newContext < oldContext, Put will fail and the existing value will remain.
			sc.GetClient(0).Put(MakePutFreshEntry("s1", []byte("abcde")))
			sc.GetClient(0).Put(MakePutFreshEntry("s1", []byte("efghi")))

			res := sc.GetClient(0).Get("s1")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("abcde"),
			}))
		})

	})

})
