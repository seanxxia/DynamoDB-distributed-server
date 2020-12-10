package mydynamotest

import (
	"strconv"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
)

var _ = Describe("End-to-End", func() {

	Describe("R-Value: 1, W-Value: 5, ClusterSize: 9", func() {

		var sc ServerCoordinator

		BeforeEach(func() {
			sc = NewServerCoordinator(8000+config.GinkgoConfig.ParallelNode*100, 1, 5, 9)
			sc.GetClient(3).ForceCrash()
			sc.GetClient(4).ForceCrash()
			sc.GetClient(5).ForceCrash()
			sc.GetClient(6).ForceCrash()
			sc.GetClient(7).ForceCrash()
			sc.GetClient(8).ForceCrash()
		})

		AfterEach(func() {
			sc.Kill()
		})

		It("should put as many as possible and return false when availble servers < (W - 1).", func() {
			Expect(sc.GetClient(0).Put(MakePutFreshEntry("k1", []byte("v1")))).To(BeFalse())

			sc.GetClient(3).ForceRestore()
			sc.GetClient(4).ForceRestore()
			sc.GetClient(5).ForceRestore()
			sc.GetClient(6).ForceRestore()
			sc.GetClient(7).ForceRestore()
			sc.GetClient(8).ForceRestore()

			res0 := sc.GetClient(0).Get("k1")
			Expect(res0).NotTo(BeNil())
			Expect(GetEntryValues(res0)).To(ConsistOf([][]byte{
				[]byte("v1"),
			}))

			res1 := sc.GetClient(1).Get("k1")
			Expect(res1).NotTo(BeNil())
			Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
				[]byte("v1"),
			}))

			res2 := sc.GetClient(2).Get("k1")
			Expect(res2).NotTo(BeNil())
			Expect(GetEntryValues(res2)).To(ConsistOf([][]byte{
				[]byte("v1"),
			}))

			res3 := sc.GetClient(3).Get("k1")
			Expect(res3).NotTo(BeNil())
			Expect(GetEntryValues(res3)).To(ConsistOf([][]byte{}))

			res4 := sc.GetClient(4).Get("k1")
			Expect(res4).NotTo(BeNil())
			Expect(GetEntryValues(res4)).To(ConsistOf([][]byte{}))

			res5 := sc.GetClient(5).Get("k1")
			Expect(res5).NotTo(BeNil())
			Expect(GetEntryValues(res5)).To(ConsistOf([][]byte{}))

			res6 := sc.GetClient(6).Get("k1")
			Expect(res6).NotTo(BeNil())
			Expect(GetEntryValues(res6)).To(ConsistOf([][]byte{}))

			res7 := sc.GetClient(7).Get("k1")
			Expect(res7).NotTo(BeNil())
			Expect(GetEntryValues(res7)).To(ConsistOf([][]byte{}))

			res8 := sc.GetClient(8).Get("k1")
			Expect(res8).NotTo(BeNil())
			Expect(GetEntryValues(res8)).To(ConsistOf([][]byte{}))
		})

	})

	Describe("R-Value: 5, W-Value: 1, ClusterSize: 9", func() {
		var sc ServerCoordinator

		BeforeEach(func() {
			sc = NewServerCoordinator(8000+config.GinkgoConfig.ParallelNode*100, 5, 1, 9)
		})

		AfterEach(func() {
			sc.Kill()
		})

		It("should get avaible servers (skip unavaible servers).", func() {
			for idx := 0; idx < 5; idx++ {
				sc.GetClient(idx).Put(MakePutFreshEntry("k0", []byte("v"+strconv.Itoa(idx))))
			}
			sc.GetClient(3).ForceCrash()
			sc.GetClient(4).ForceCrash()

			res0 := sc.GetClient(0).Get("k0")
			Expect(res0).NotTo(BeNil())
			Expect(GetEntryValues(res0)).To(ConsistOf([][]byte{
				[]byte("v0"),
				[]byte("v1"),
				[]byte("v2"),
			}))

		})
	})

	Describe("R-Value: 1, W-Value: 3, ClusterSize: 5", func() {
		var sc ServerCoordinator

		BeforeEach(func() {
			sc = NewServerCoordinator(8000+config.GinkgoConfig.ParallelNode*100, 1, 3, 5)
		})

		AfterEach(func() {
			sc.Kill()
		})

		It("should gossip avaible servers (skip unavaible servers).", func() {
			sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0")))

			sc.GetClient(3).ForceCrash()
			sc.GetClient(4).ForceCrash()

			sc.GetClient(0).Gossip()

			sc.GetClient(3).ForceRestore()
			sc.GetClient(4).ForceRestore()

			res0 := sc.GetClient(0).Get("k0")
			Expect(res0).NotTo(BeNil())
			Expect(GetEntryValues(res0)).To(ConsistOf([][]byte{
				[]byte("v0"),
			}))

			res1 := sc.GetClient(1).Get("k0")
			Expect(res1).NotTo(BeNil())
			Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
				[]byte("v0"),
			}))

			res2 := sc.GetClient(2).Get("k0")
			Expect(res2).NotTo(BeNil())
			Expect(GetEntryValues(res2)).To(ConsistOf([][]byte{
				[]byte("v0"),
			}))

			res3 := sc.GetClient(3).Get("k0")
			Expect(res3).NotTo(BeNil())
			Expect(GetEntryValues(res3)).To(ConsistOf([][]byte{}))

			res4 := sc.GetClient(4).Get("k0")
			Expect(res4).NotTo(BeNil())
			Expect(GetEntryValues(res4)).To(ConsistOf([][]byte{}))

		})
	})

	Describe("R-Value: 3, W-Value: 3, ClusterSize: 5", func() {
		var sc ServerCoordinator

		BeforeEach(func() {
			sc = NewServerCoordinator(8000+config.GinkgoConfig.ParallelNode*100, 3, 3, 5)

		})

		AfterEach(func() {
			sc.Kill()
		})

		It("should restore and keep its original schedule.", func() {
			sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0")))

			res2 := sc.GetClient(2).Get("k0")
			Expect(res2).NotTo(BeNil())
			Expect(GetEntryValues(res2)).To(ConsistOf([][]byte{
				[]byte("v0"),
			}))

			sc.GetClient(2).ForceCrash()

			sc.GetClient(1).Put(MakePutFromVectorClockMapAndValue(
				"k0",
				map[string]uint64{sc.GetID(0): 1},
				[]byte("v1"),
			))

			sc.GetClient(2).ForceRestore()

			sc.GetClient(3).ForceCrash()
			sc.GetClient(4).ForceCrash()

			res2 = sc.GetClient(2).Get("k0")
			Expect(res2).NotTo(BeNil())
			Expect(GetEntryValues(res2)).To(ConsistOf([][]byte{
				[]byte("v0"),
			}))

		})
	})

})
