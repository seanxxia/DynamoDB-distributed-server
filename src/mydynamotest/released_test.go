package mydynamotest

import (
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
)

var _ = Describe("TA Released Tests", func() {
	// Reference:
	// - https://piazza.com/class/kfqynl4r6a0317?cid=1074
	// - https://github.com/ucsd-cse-x24-fa20/module4/blob/main/src/mydynamotest/released_test.go#L76

	Describe("Two Servers", func() {

		var sc ServerCoordinator

		BeforeEach(func() {
			// StartingPort: 8000, R-Value: 2, W-Value: 2, ClusterSize: 5
			sc = NewServerCoordinator(8000+config.GinkgoConfig.ParallelNode*100, 2, 2, 5)
		})

		AfterEach(func() {
			sc.Kill()
		})

		It("should pass test: PutW2", func() {
			sc.GetClient(0).Put(MakePutFreshEntry("s1", []byte("abcde")))

			res1 := sc.GetClient(1).Get("s1")
			Expect(res1).NotTo(BeNil())
			Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
				[]byte("abcde"),
			}))
		})

	})

	Describe("Five Servers", func() {

		var sc ServerCoordinator

		BeforeEach(func() {
			// StartingPort: 8000, R-Value: 1, W-Value: 1, ClusterSize: 5
			sc = NewServerCoordinator(8000+config.GinkgoConfig.ParallelNode*100, 1, 1, 5)
		})

		AfterEach(func() {
			sc.Kill()
		})

		It("should pass test: Gossip", func() {
			sc.GetClient(0).Put(MakePutFreshEntry("s1", []byte("abcde")))
			sc.GetClient(0).Gossip()

			res1 := sc.GetClient(1).Get("s1")
			Expect(res1).NotTo(BeNil())
			Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
				[]byte("abcde"),
			}))

		})

		It("should pass test: MultipleKeys", func() {
			sc.GetClient(0).Put(MakePutFreshEntry("s1", []byte("abcde")))

			sc.GetClient(0).Gossip()

			res0 := sc.GetClient(0).Get("s1")
			Expect(res0).NotTo(BeNil())
			Expect(GetEntryValues(res0)).To(ConsistOf([][]byte{
				[]byte("abcde"),
			}))

			res1 := sc.GetClient(1).Get("s1")
			Expect(res1).NotTo(BeNil())
			Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
				[]byte("abcde"),
			}))

			entry := res1.EntryList[0]
			entry.Value = []byte("efghi")

			sc.GetClient(1).Put(MakePutFromEntry("s1", entry))

			res1 = sc.GetClient(1).Get("s1")
			Expect(res1).NotTo(BeNil())
			Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
				[]byte("efghi"),
			}))

		})

		It("should pass test: DynamoPaper", func() {
			sc.GetClient(0).Put(MakePutFreshEntry("s1", []byte("abcde")))

			res0 := sc.GetClient(0).Get("s1")
			Expect(res0).NotTo(BeNil())
			Expect(GetEntryValues(res0)).To(ConsistOf([][]byte{
				[]byte("abcde"),
			}))

			entry0 := res0.EntryList[0]
			entry0.Value = []byte("efghi")

			sc.GetClient(0).Put(MakePutFromEntry("s1", entry0))

			res0 = sc.GetClient(0).Get("s1")
			Expect(res0).NotTo(BeNil())
			Expect(GetEntryValues(res0)).To(ConsistOf([][]byte{
				[]byte("efghi"),
			}))

			sc.GetClient(0).Gossip()

			entry1 := res0.EntryList[0]
			entry1.Value = []byte("cdefg")
			sc.GetClient(1).Put(MakePutFromEntry("s1", entry1))

			entry2 := res0.EntryList[0]
			entry2.Value = []byte("defgh")
			sc.GetClient(2).Put(MakePutFromEntry("s1", entry2))

			res1 := sc.GetClient(1).Get("s1")
			Expect(res1).NotTo(BeNil())
			Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
				[]byte("cdefg"),
			}))

			res2 := sc.GetClient(2).Get("s1")
			Expect(res2).NotTo(BeNil())
			Expect(GetEntryValues(res2)).To(ConsistOf([][]byte{
				[]byte("defgh"),
			}))

			sc.GetClient(1).Gossip()

			sc.GetClient(2).Gossip()

			res0 = sc.GetClient(0).Get("s1")

			entry0 = res0.EntryList[0]
			entry0.Context.Clock.Combine(GetEntryContextClocks(res0))
			entry0.Value = []byte("zyxw")
			sc.GetClient(0).Put(MakePutFromEntry("s1", entry0))
			res0 = sc.GetClient(0).Get("s1")
			Expect(res0).NotTo(BeNil())
			Expect(GetEntryValues(res0)).To(ConsistOf([][]byte{
				[]byte("zyxw"),
			}))

		})

		It("should pass test: TestInvalidPut", func() {
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
