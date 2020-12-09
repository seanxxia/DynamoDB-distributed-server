package mydynamotest

import (
	"fmt"
	dy "mydynamo"
	"strings"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("VectorClock", func() {
	Describe("Empty vector clocks", func() {
		var vClock1 dy.VectorClock
		var vClock2 dy.VectorClock

		BeforeEach(func() {
			vClock1 = dy.NewVectorClock()
			vClock2 = dy.NewVectorClock()
		})

		It("should be equal.", func() {
			Expect(vClock1.Equals(vClock2)).To(BeTrue())
		})

		It("should not be less than each other.", func() {
			Expect(vClock1.LessThan(vClock2)).To(BeFalse())
		})

		It("should be concurrent.", func() {
			Expect(vClock1.LessThan(vClock2)).To(BeFalse())
		})

		Context("after combine", func() {
			It("should equal to empty vector clock.", func() {
				vClock1.Combine([]dy.VectorClock{vClock2})
				Expect(vClock1.Equals(dy.NewVectorClock())).To(BeTrue())
			})
		})

		Context("after increment s1's local clock", func() {
			BeforeEach(func() {
				vClock1.Increment("s1")
			})

			It("should not be equal to empty vector clock.", func() {
				Expect(vClock1.Equals(vClock2)).To(BeFalse())
				Expect(vClock2.Equals(vClock1)).To(BeFalse())
			})

			It("should be greater than empty vector clock.", func() {
				Expect(vClock1.LessThan(vClock2)).To(BeFalse())
				Expect(vClock2.LessThan(vClock1)).To(BeTrue())
			})

			It("should not be concurrent to empty vector clock.", func() {
				Expect(vClock1.Concurrent(vClock2)).To(BeFalse())
				Expect(vClock2.Concurrent(vClock1)).To(BeFalse())
			})

			Context("after combine to a new vector clock", func() {

				It("should be equal to the incremented vector clock.", func() {
					vClockCombined := dy.NewVectorClock()
					vClockCombined.Combine([]dy.VectorClock{vClock1, vClock2})

					Expect(vClockCombined.Equals(vClock1)).To(BeTrue())
					Expect(vClockCombined.Equals(vClock2)).To(BeFalse())
				})
			})

			Context("after combine to empty vector clock", func() {

				It("should be equal.", func() {
					vClock2.Combine([]dy.VectorClock{vClock1})
					Expect(vClock2.Equals(vClock1)).To(BeTrue())
					Expect(vClock1.Equals(vClock2)).To(BeTrue())
				})
			})

			Context("after combine to incremented vector clock", func() {
				It("should not change.", func() {
					vClockSame := dy.NewVectorClock()
					vClockSame.Increment("s1")

					vClock1.Combine([]dy.VectorClock{vClock2})
					vClock1.Equals(vClockSame)
				})
			})
		})
	})

	Describe("Equal vector clocks", func() {
		testCases := [][]string{
			[]string{ // TestCase 1
				"s2",
				"s2",
			},
			[]string{ // TestCase 2
				"s1,s1",
				"s1,s1",
			},
			[]string{ // TestCase 3
				"s1,s2,s3",
				"s1,s2,s3",
			},
		}

		MapTestCases(testCases, func(i int, testCase []string) {
			Describe(fmt.Sprintf("Case %d", i+1), func() {
				var vClock1 dy.VectorClock
				var vClock2 dy.VectorClock
				BeforeEach(func() {
					vClock1 = dy.NewVectorClock()
					vClock2 = dy.NewVectorClock()

					for _, vClock1Increment := range strings.Split(testCase[0], ",") {
						vClock1Increment = strings.TrimSpace(vClock1Increment)
						if len(vClock1Increment) > 0 {
							vClock1.Increment(vClock1Increment)
						}
					}
					for _, vClock2Increment := range strings.Split(testCase[1], ",") {
						vClock2Increment = strings.TrimSpace(vClock2Increment)
						if len(vClock2Increment) > 0 {
							vClock2.Increment(vClock2Increment)
						}
					}
				})

				It("should be equal", func() {
					Expect(vClock1.Equals(vClock2)).To(BeTrue())
					Expect(vClock2.Equals(vClock1)).To(BeTrue())
				})

				It("should not be less than each other.", func() {
					Expect(vClock1.LessThan(vClock2)).To(BeFalse())
					Expect(vClock2.LessThan(vClock1)).To(BeFalse())
				})

				It("should not be concurrent.", func() {
					Expect(vClock1.Concurrent(vClock2)).To(BeFalse())
					Expect(vClock2.Concurrent(vClock1)).To(BeFalse())
				})

				Context("after combine", func() {
					var vClockCombined dy.VectorClock

					BeforeEach(func() {
						vClockCombined = dy.NewVectorClock()
						vClockCombined.Combine([]dy.VectorClock{vClock1, vClock2})
					})

					It("should be equal to original vector clocks.", func() {
						Expect(vClockCombined.Equals(vClock1)).To(BeTrue())
						Expect(vClockCombined.Equals(vClock2)).To(BeTrue())
					})

					It("should not be concurrent to original vector clocks.", func() {
						Expect(vClockCombined.Concurrent(vClock1)).To(BeFalse())
						Expect(vClockCombined.Concurrent(vClock2)).To(BeFalse())
					})
				})
			})
		})
	})

	Describe("Concurrent vector clocks", func() {
		testCases := [][]string{
			[]string{ // TestCase 1
				"s1",
				"s2",
			},
			[]string{ // TestCase 2
				"s1,s1",
				"s2",
			},
			[]string{ // TestCase 3
				"s1,s1,s2,s3",
				"s1,s2,s2",
			},
			[]string{ // TestCase 4
				"s1,s2,s3",
				"s2,s2",
			},
			[]string{ // TestCase 5
				"s1,s2,s3",
				"s2,s2,s2,s3,s4,s5",
			},
		}

		MapTestCases(testCases, func(i int, testCase []string) {
			Describe(fmt.Sprintf("Case %d", i+1), func() {
				var vClock1 dy.VectorClock
				var vClock2 dy.VectorClock
				BeforeEach(func() {
					vClock1 = dy.NewVectorClock()
					vClock2 = dy.NewVectorClock()

					for _, vClock1Increment := range strings.Split(testCase[0], ",") {
						vClock1Increment = strings.TrimSpace(vClock1Increment)
						if len(vClock1Increment) > 0 {
							vClock1.Increment(vClock1Increment)
						}
					}
					for _, vClock2Increment := range strings.Split(testCase[1], ",") {
						vClock2Increment = strings.TrimSpace(vClock2Increment)
						if len(vClock2Increment) > 0 {
							vClock2.Increment(vClock2Increment)
						}
					}
				})

				It("should not be equal", func() {
					Expect(vClock1.Equals(vClock2)).To(BeFalse())
					Expect(vClock2.Equals(vClock1)).To(BeFalse())
				})

				It("should not be less than each other.", func() {
					Expect(vClock1.LessThan(vClock2)).To(BeFalse())
					Expect(vClock2.LessThan(vClock1)).To(BeFalse())
				})

				It("should be concurrent.", func() {
					Expect(vClock1.Concurrent(vClock2)).To(BeTrue())
					Expect(vClock2.Concurrent(vClock1)).To(BeTrue())
				})

				Context("after combine", func() {
					var vClockCombined dy.VectorClock

					BeforeEach(func() {
						vClockCombined = dy.NewVectorClock()
						vClockCombined.Combine([]dy.VectorClock{vClock1, vClock2})
					})

					It("should not be equal to original vector clocks.", func() {
						Expect(vClockCombined.Equals(vClock1)).To(BeFalse())
						Expect(vClockCombined.Equals(vClock2)).To(BeFalse())
					})

					It("should not be concurrent to original vector clocks.", func() {
						Expect(vClockCombined.Concurrent(vClock1)).To(BeFalse())
						Expect(vClockCombined.Concurrent(vClock2)).To(BeFalse())
					})

					It("should be greater than original vector clocks.", func() {
						Expect(vClockCombined.LessThan(vClock1)).To(BeFalse())
						Expect(vClockCombined.LessThan(vClock2)).To(BeFalse())

						Expect(vClock1.LessThan(vClockCombined)).To(BeTrue())
						Expect(vClock2.LessThan(vClockCombined)).To(BeTrue())
					})
				})
			})
		})
	})

	Describe("Ancestor and descendant", func() {
		testCases := [][]string{
			[]string{ // TestCase 1
				"s1,s2",
				"s2",
			},
			[]string{ // TestCase 2
				"s1,s1,s2",
				"s2",
			},
			[]string{ // TestCase 3
				"s1,s1,s2",
				"s1,s2",
			},
			[]string{ // TestCase 4
				"s2,s2",
				"s2",
			},
			[]string{ // TestCase 5
				"s1",
				"",
			},
			[]string{ // TestCase 6
				"s1,s2,s3,s4,s4",
				"s1,s2,s3,s4",
			},
			[]string{ // TestCase 7
				"s1,s2,s3,s4,s5",
				"s1,s2",
			},
		}

		MapTestCases(testCases, func(i int, testCase []string) {
			Describe(fmt.Sprintf("Case %d", i+1), func() {
				var vClock1 dy.VectorClock
				var vClock2 dy.VectorClock

				BeforeEach(func() {
					vClock1 = dy.NewVectorClock()
					vClock2 = dy.NewVectorClock()

					for _, vClock1Increment := range strings.Split(testCase[0], ",") {
						vClock1Increment = strings.TrimSpace(vClock1Increment)
						if len(vClock1Increment) > 0 {
							vClock1.Increment(vClock1Increment)
						}
					}
					for _, vClock2Increment := range strings.Split(testCase[1], ",") {
						vClock2Increment = strings.TrimSpace(vClock2Increment)
						if len(vClock2Increment) > 0 {
							vClock2.Increment(vClock2Increment)
						}
					}
				})

				It("should not be equal", func() {
					Expect(vClock1.Equals(vClock2)).To(BeFalse())
					Expect(vClock2.Equals(vClock1)).To(BeFalse())
				})

				It("descendant should be less than ancestor.", func() {
					Expect(vClock1.LessThan(vClock2)).To(BeFalse())
					Expect(vClock2.LessThan(vClock1)).To(BeTrue())
				})

				It("should not be concurrent.", func() {
					Expect(vClock1.Concurrent(vClock2)).To(BeFalse())
					Expect(vClock2.Concurrent(vClock1)).To(BeFalse())
				})

				Context("after combine", func() {
					var vClockCombined dy.VectorClock

					BeforeEach(func() {
						vClockCombined = dy.NewVectorClock()
						vClockCombined.Combine([]dy.VectorClock{vClock1, vClock2})
					})

					It("should be equal to ancestor.", func() {
						Expect(vClockCombined.Equals(vClock1)).To(BeTrue())
					})
				})
			})
		})
	})

	Describe("Combine multiple vector clocks", func() {
		testCases := [][]string{
			[]string{ // TestCase 1
				"s1,s2,s3",
				"s1,s2",
				"s1",
			},
			[]string{ // TestCase 2
				"s1,s2,s3",
				"s1,s1",
				"s1,s4",
			},
			[]string{ // TestCase 3
				"s1",
				"s2",
				"s3",
			},
			[]string{ // TestCase 4
				"s1,s2",
				"s3,s4",
				"s5,s6",
			},
			[]string{ // TestCase 5
				"s1,s2,s3",
				"s1,s2,s2,s4",
				"s1,s2",
				"",
			},
			[]string{ // TestCase 6
				"s1,s2,s2,s3",
				"s2,s2,s3,s3,s4",
				"s5,s5,s1,s2,s2,s2,s2,s2",
				"s1,s2,s3,s4,s5,s6",
				"",
				"s1,s1,s1,s1,s1,s1,s1,s1,s1",
			},
		}

		MapTestCases(testCases, func(i int, testCase []string) {
			Describe(fmt.Sprintf("Case %d", i+1), func() {
				var vClocks []dy.VectorClock

				BeforeEach(func() {
					vClocks = make([]dy.VectorClock, 0)
					for _, vClockIncrementsString := range testCase {
						vClock := dy.NewVectorClock()
						vClockIncrements := strings.Split(vClockIncrementsString, ",")

						for _, vClockIncrement := range vClockIncrements {
							vClockIncrement = strings.TrimSpace(vClockIncrement)
							if len(vClockIncrement) > 0 {
								vClock.Increment(vClockIncrement)
							}
						}

						vClocks = append(vClocks, vClock)
					}
				})

				Context("after combine", func() {
					var vClockCombined dy.VectorClock

					BeforeEach(func() {
						vClockCombined = dy.NewVectorClock()
						vClockCombined.Combine(vClocks)
					})

					It("should be not less than all vector clocks.", func() {
						for _, vClock := range vClocks {
							Expect(vClockCombined.LessThan(vClock)).To(BeFalse())
						}
					})
				})
			})
		})
	})
})
