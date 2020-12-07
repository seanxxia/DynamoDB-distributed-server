package mydynamotest

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("End-to-End", func() {
	It("should pass.", func() {
		Expect(1 + 1).To(Equal(2))
	})
})
