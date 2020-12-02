package mydynamotest

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = FDescribe("Basic", func() {
	var sc ServerCoordinator

	BeforeEach(func() {
		sc = NewServerCoordinator(8000, 1, 1, 1, true)
	})

	AfterEach(func() {
		sc.Kill()
	})

	It("should put and get entry.", func() {
		sc.GetClient(0).Put(MakePutFreshEntry("k1", []byte("v1")))

		res := sc.GetClient(0).Get("k1")

		Expect(res).NotTo(BeNil())
		Expect(res.EntryList).To(HaveLen(1))
		Expect(res.EntryList[0].Value).To(Equal([]byte("v1")))
	})

	It("should put, get, and update entry.", func() {
		sc.GetClient(0).Put(MakePutFreshEntry("k1", []byte("v1")))

		entry := sc.GetClient(0).Get("k1").EntryList[0]
		entry.Value = []byte("v1-1")

		sc.GetClient(0).Put(MakePutFromEntry("k1", entry))

		res := sc.GetClient(0).Get("k1")
		Expect(res).NotTo(BeNil())
		Expect(res.EntryList).To(HaveLen(1))
		Expect(res.EntryList[0].Value).To(Equal([]byte("v1-1")))
	})
})
