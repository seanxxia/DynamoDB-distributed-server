package mydynamotest

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestVectorClock(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "VectorClock Suite")
}

var _ = Describe("VectorClock", func() {
	Context("1 and 2", func() {
		It("should be 3", func() {
			Expect(1 + 2).To(Equal(3))
		})
	})
})
