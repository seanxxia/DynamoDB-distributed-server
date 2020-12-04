package mydynamotest

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ForceCrash", func() {

	var sc ServerCoordinator

	BeforeEach(func() {
		// StartingPort: 8000, R-Value: 1, W-Value: 1, ClusterSize: 1
		sc = NewServerCoordinator(8000, 1, 1, 1)

		// Put key "k1" to the server for testing
		sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0")))
	})

	AfterEach(func() {
		sc.Kill()
	})

	Context("after calling ForceCrash() through RPC client", func() {
		BeforeEach(func() {
			sc.GetClient(0).ForceCrash()
		})

		Context("when calling Get(...)", func() {
			It("should return nil.", func() {
				Expect(sc.GetClient(0).Get("k0")).To(BeNil())
				Expect(sc.GetClient(0).Get("k1")).To(BeNil())
			})
		})

		Context("when calling Put(...)", func() {
			It("should return false.", func() {
				Expect(sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0")))).To(BeFalse())
				Expect(sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v1")))).To(BeFalse())
				Expect(sc.GetClient(0).Put(MakePutFreshEntry("k1", []byte("v1")))).To(BeFalse())
			})

			It("should not put value to the server (check after calling ForceRestore()).", func() {
				sc.GetClient(0).Put(MakePutFreshEntry("k1", []byte("v1")))

				sc.GetClient(0).ForceRestore()

				res := sc.GetClient(0).Get("k1")
				Expect(res).NotTo(BeNil())
				Expect(GetEntryValues(res)).To(ConsistOf([][]byte{}))
			})
		})

		Context("after calling ForceRestore()", func() {
			BeforeEach(func() {
				sc.GetClient(0).ForceRestore()
			})

			It("should restore and handle get.", func() {
				res := sc.GetClient(0).Get("k0")
				Expect(res).NotTo(BeNil())
				Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
					[]byte("v0"),
				}))
			})

			It("should restore and handle put and get.", func() {
				sc.GetClient(0).Put(MakePutFreshEntry("k1", []byte("v1")))

				res := sc.GetClient(0).Get("k1")
				Expect(res).NotTo(BeNil())
				Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
					[]byte("v1"),
				}))
			})
		})
	})
})
