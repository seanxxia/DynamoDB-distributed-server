package mydynamotest

import (
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
)

var _ = Describe("Crash", func() {

	var sc ServerCoordinator

	BeforeEach(func() {
		// StartingPort: 8000, R-Value: 1, W-Value: 1, ClusterSize: 1
		sc = NewServerCoordinator(8000+config.GinkgoConfig.ParallelNode * 100, 1, 1, 1)

		// Put key "k1" to the server for testing
		sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0")))
	})

	AfterEach(func() {
		sc.Kill()
	})

	It("should return true if server is crashed by the call.", func() {
		Expect(sc.GetClient(0).Crash(2)).To(BeTrue())
	})

	Context("after calling Crash(2) through RPC client", func() {
		BeforeEach(func() {
			var _ = sc.GetClient(0).Crash(2)
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

			It("should not put value to the server (check after server restore).", func() {
				sc.GetClient(0).Put(MakePutFreshEntry("k1", []byte("v1")))

				time.Sleep(3 * time.Second)

				res := sc.GetClient(0).Get("k1")
				Expect(res).NotTo(BeNil())
				Expect(GetEntryValues(res)).To(ConsistOf([][]byte{}))
			})
		})

		Context("when calling Crash(...) again", func() {
			It("should return false.", func() {
				Expect(sc.GetClient(0).Crash(1)).To(BeFalse())
				Expect(sc.GetClient(0).Crash(3)).To(BeFalse())
				Expect(sc.GetClient(0).Crash(5)).To(BeFalse())
			})

			It("should keep its original schedule to restore (check after server restore).", func() {
				sc.GetClient(0).Crash(1000)

				time.Sleep(3 * time.Second)

				// Test if rpc client can get entries correctly
				res := sc.GetClient(0).Get("k0")
				Expect(res).NotTo(BeNil())
				Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
					[]byte("v0"),
				}))
			})
		})

		Context("after 3 seconds", func() {
			BeforeEach(func() {
				time.Sleep(3 * time.Second)
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

			It("shold be able to crash again.", func() {
				Expect(sc.GetClient(0).Crash(3)).To(BeTrue())
			})
		})
	})
})
