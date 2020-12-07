package mydynamotest

import (
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
)

var _ = Describe("Put & Get Replicate", func() {

	Describe("R=1, W=2, ClusterSize=3", func() {
		var sc ServerCoordinator

		BeforeEach(func() {
			// StartingPort: 8000, R-Value: 1, W-Value: 2, ClusterSize: 3
			sc = NewServerCoordinator(8000+config.GinkgoConfig.ParallelNode*100, 1, 2, 3)
		})

		AfterEach(func() {
			sc.Kill()
		})

		It("should replicate put entry to W-1 other nodes.", func() {
			sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0")))

			res := sc.GetClient(0).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0"),
			}))

			res = sc.GetClient(1).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0"),
			}))

			res = sc.GetClient(2).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{}))
		})

		It("should replicate put entry and overwrite ancestor entries.", func() {
			sc.GetClient(1).Put(MakePutFreshEntry("k0", []byte("v0-1")))
			sc.GetClient(0).Put(MakePutFromVectorClockMapAndValue(
				"k0",
				map[string]uint64{sc.GetID(1): 1},
				[]byte("v0-0"),
			))

			res := sc.GetClient(0).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0-0"),
			}))

			res = sc.GetClient(1).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0-0"),
			}))

			res = sc.GetClient(2).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0-1"),
			}))
		})

		It("should replicate put entry and ignore descendant entries.", func() {
			sc.GetClient(1).Put(MakePutFromVectorClockMapAndValue(
				"k0",
				map[string]uint64{sc.GetID(0): 1},
				[]byte("v0-1"),
			))
			sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0-0")))

			res := sc.GetClient(0).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0-0"),
			}))

			res = sc.GetClient(1).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0-1"),
			}))

			res = sc.GetClient(2).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0-1"),
			}))
		})

		It("should replicate put entry and ignore entries with equal context.", func() {
			sc.GetClient(1).Put(MakePutFromVectorClockMapAndValue(
				"k0",
				map[string]uint64{sc.GetID(0): 1},
				[]byte("v0-1"),
			))
			sc.GetClient(0).Put(MakePutFromVectorClockMapAndValue(
				"k0",
				map[string]uint64{sc.GetID(1): 1},
				[]byte("v0-0"),
			))

			res := sc.GetClient(0).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0-0"),
			}))

			res = sc.GetClient(1).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0-1"),
			}))

			res = sc.GetClient(2).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0-1"),
			}))
		})

		It("should replicate put entry and keep conflict entries.", func() {
			sc.GetClient(1).Put(MakePutFreshEntry("k0", []byte("v0-1")))
			sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0-0")))

			res := sc.GetClient(0).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0-0"),
			}))

			res = sc.GetClient(1).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0-0"),
				[]byte("v0-1"),
			}))

			res = sc.GetClient(2).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0-1"),
			}))
		})
	})

	Describe("R=2, W=1, ClusterSize=3", func() {
		var sc ServerCoordinator

		BeforeEach(func() {
			// StartingPort: 8000, R-Value: 2, W-Value: 1, ClusterSize: 3
			sc = NewServerCoordinator(8000+config.GinkgoConfig.ParallelNode*100, 2, 1, 3)
		})

		AfterEach(func() {
			sc.Kill()
		})

		It("should get entries from R-1 other nodes", func() {
			sc.GetClient(1).Put(MakePutFreshEntry("k0", []byte("v0")))

			res := sc.GetClient(0).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0"),
			}))

			res = sc.GetClient(1).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0"),
			}))

			res = sc.GetClient(2).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{}))
		})

		It("should get entries and ignore local ancestor entries.", func() {
			sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0-0")))
			sc.GetClient(1).Put(MakePutFromVectorClockMapAndValue(
				"k0",
				map[string]uint64{sc.GetID(0): 1},
				[]byte("v0-1"),
			))

			res := sc.GetClient(0).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0-1"),
			}))

			res = sc.GetClient(1).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0-1"),
			}))

			res = sc.GetClient(2).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0-0"),
			}))
		})

		It("should get entries and keep local descendant entries.", func() {
			sc.GetClient(0).Put(MakePutFromVectorClockMapAndValue(
				"k0",
				map[string]uint64{sc.GetID(1): 1},
				[]byte("v0-0"),
			))
			sc.GetClient(1).Put(MakePutFreshEntry("k0", []byte("v0-1")))

			res := sc.GetClient(0).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0-0"),
			}))

			res = sc.GetClient(1).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0-1"),
			}))

			res = sc.GetClient(2).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0-0"),
			}))
		})

		It("should get all conflict entries.", func() {
			sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0-0")))
			sc.GetClient(1).Put(MakePutFreshEntry("k0", []byte("v0-1")))

			res := sc.GetClient(0).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0-0"),
				[]byte("v0-1"),
			}))

			res = sc.GetClient(1).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0-1"),
			}))

			res = sc.GetClient(2).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0-0"),
			}))
		})
	})
})
