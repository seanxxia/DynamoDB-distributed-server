package mydynamotest

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestMyDynamo(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "MyDynamo Suite")
}
