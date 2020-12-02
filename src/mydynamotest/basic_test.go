package mydynamotest

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = FDescribe("Basic", func() {
	var sc ServerCoordinator

	BeforeEach(func() {
		sc = NewServerCoordinator(8000, 1, 1, 1)
	})

	AfterEach(func() {
		sc.Kill()
	})

	It("should put and get key-value pair.", func() {
		sc.GetClient(0).Put(MakePutFreshContext("s1", []byte("abcde")))

		gotValue := sc.GetClient(0).Get("s1")

		Expect(gotValue).NotTo(BeNil())
		Expect(gotValue.EntryList).To(HaveLen(1))
		Expect(gotValue.EntryList[0].Value).To(Equal([]byte("abcde")))
	})
})
