package mydynamotest

import (
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
)

var _ = Describe("End-to-End", func() {

	Describe("R-Value: 1, W-Value: 5, ClusterSize: 9", func() {

		var sc ServerCoordinator

		BeforeEach(func() {
			sc = NewServerCoordinator(8000+config.GinkgoConfig.ParallelNode*100, 5, 5, 9)
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

		It("should put as many as possible.", func() {
			sc.GetClient(0).Put(MakePutFreshEntry("k1", []byte("v1")))

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

})
