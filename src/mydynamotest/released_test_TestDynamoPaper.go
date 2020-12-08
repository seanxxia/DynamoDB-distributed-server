package mydynamotest

import (
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
)

var _ = Describe("TestDynamoPaper", func() {

	Describe("Five Servers", func() {

		var sc ServerCoordinator

		BeforeEach(func() {
			// StartingPort: 8080, R-Value: 1, W-Value: 1, ClusterSize: 5
			sc = NewServerCoordinator(8080+config.GinkgoConfig.ParallelNode*100, 1, 1, 5)
		})

		AfterEach(func() {
			sc.Kill()
		})

		It("should handle put and get conflict entries.", func() {
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

	})

})
