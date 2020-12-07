package mydynamotest

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("End-to-End", func() {
	FIt("should fail.", func() {
		Expect(1 + 1).To(Equal(3))
	})
})
