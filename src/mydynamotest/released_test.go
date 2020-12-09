package mydynamotest

import (
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
)

var _ = Describe("TA Released Tests", func() {

	Describe("twoserver.ini: clusterSize:5 r-Value:3 w-Value:3", func() {
		var sc ServerCoordinator

		BeforeEach(func() {
			sc = NewServerCoordinator(8000+config.GinkgoConfig.ParallelNode*100, 2, 2, 5)
		})

		AfterEach(func() {
			sc.Kill()
		})

		It("should pass test: PutW2", func() {
			// Reference:
			// - https://github.com/ucsd-cse-x24-fa20/module4/blob/main/src/mydynamotest/released_test.go#L9

			sc.GetClient(0).Put(MakePutFreshEntry("s1", []byte("abcde")))

			res1 := sc.GetClient(1).Get("s1")
			Expect(res1).NotTo(BeNil())
			Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
				[]byte("abcde"),
			}))
		})

		It("should pass test: GossipW2", func() {
			// Reference:
			// - https://github.com/ucsd-cse-x24-fa20/module4/blob/main/src/mydynamotest/released_test.go#L219

			sc.GetClient(0).Put(MakePutFreshEntry("s1", []byte("abcde")))

			sc.GetClient(0).Gossip()

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

			res0 := sc.GetClient(0).Get("s1")
			Expect(res0).NotTo(BeNil())
			Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
				[]byte("efghi"),
			}))

		})

	})

	Describe("myconfig.ini: clusterSize:5 r-Value:1 w-Value:1", func() {

		var sc ServerCoordinator

		BeforeEach(func() {
			sc = NewServerCoordinator(8000+config.GinkgoConfig.ParallelNode*100, 1, 1, 5)
		})

		AfterEach(func() {
			sc.Kill()
		})

		It("should pass test: Gossip", func() {
			// Reference:
			// - https://github.com/ucsd-cse-x24-fa20/module4/blob/main/src/mydynamotest/released_test.go#L35

			sc.GetClient(0).Put(MakePutFreshEntry("s1", []byte("abcde")))
			sc.GetClient(0).Gossip()

			res1 := sc.GetClient(1).Get("s1")
			Expect(res1).NotTo(BeNil())
			Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
				[]byte("abcde"),
			}))

		})

		It("should pass test: MultipleKeys", func() {
			// Reference:
			// - https://github.com/ucsd-cse-x24-fa20/module4/blob/main/src/mydynamotest/released_test.go#L62

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
			// Reference:
			// - https://github.com/ucsd-cse-x24-fa20/module4/blob/main/src/mydynamotest/released_test.go#L107

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

		It("should pass test: InvalidPut", func() {
			// Reference:
			// - https://github.com/ucsd-cse-x24-fa20/module4/blob/main/src/mydynamotest/released_test.go#L199

			sc.GetClient(0).Put(MakePutFreshEntry("s1", []byte("abcde")))
			sc.GetClient(0).Put(MakePutFreshEntry("s1", []byte("efghi")))

			sc.GetClient(0).Gossip()

			res1 := sc.GetClient(1).Get("s1")
			Expect(res1).NotTo(BeNil())
			Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
				[]byte("abcde"),
			}))

		})

		It("should pass test: ReplaceMultipleVersions", func() {
			// Reference:
			// - https://github.com/ucsd-cse-x24-fa20/module4/blob/main/src/mydynamotest/released_test.go#L270

			sc.GetClient(0).Put(MakePutFreshEntry("s1", []byte("abcde")))
			sc.GetClient(1).Put(MakePutFreshEntry("s1", []byte("efghi")))

			sc.GetClient(0).Gossip()

			res1 := sc.GetClient(1).Get("s1")

			entry := res1.EntryList[0]
			entry.Context.Clock.Combine(GetEntryContextClocks(res1))
			entry.Value = []byte("zyxw")
			sc.GetClient(1).Put(MakePutFromEntry("s1", entry))

			res1 = sc.GetClient(1).Get("s1")
			Expect(res1).NotTo(BeNil())
			Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
				[]byte("zyxw"),
			}))

		})

	})

	Describe("consistent.ini: clusterSize:5 r-Value:3 w-Value:3", func() {
		var sc ServerCoordinator

		BeforeEach(func() {
			sc = NewServerCoordinator(8000+config.GinkgoConfig.ParallelNode*100, 3, 3, 5)
		})

		AfterEach(func() {
			sc.Kill()
		})

		It("should pass test: Consistent", func() {
			// Reference:
			// - https://github.com/ucsd-cse-x24-fa20/module4/blob/main/src/mydynamotest/released_test.go#L319

			sc.GetClient(0).Put(MakePutFreshEntry("s1", []byte("abcde")))

			res1 := sc.GetClient(1).Get("s1")
			Expect(res1).NotTo(BeNil())
			Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
				[]byte("abcde"),
			}))

			entry := res1.EntryList[0]
			entry.Value = []byte("zyxw")
			sc.GetClient(3).Put(MakePutFromEntry("s1", entry))

			sc.GetClient(0).Crash(3)
			sc.GetClient(1).Crash(3)
			sc.GetClient(4).Crash(3)

			res2 := sc.GetClient(2).Get("s1")
			Expect(res2).NotTo(BeNil())
			Expect(GetEntryValues(res2)).To(ConsistOf([][]byte{
				[]byte("zyxw"),
			}))
		})

	})

})
