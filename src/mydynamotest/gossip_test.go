package mydynamotest

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Gossip", func() {

	var sc ServerCoordinator

	Describe("Two Servers", func() {
		BeforeEach(func() {
			// StartingPort: 8000, R-Value: 1, W-Value: 1, ClusterSize: 2
			sc = NewServerCoordinator(8000, 1, 1, 2)
		})

		AfterEach(func() {
			sc.Kill()
		})

		It("should replicate entry to other server.", func() {
			sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0")))
			sc.GetClient(0).Gossip()

			res := sc.GetClient(1).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0"),
			}))
		})

		It("should work when there are no local entries.", func() {
			sc.GetClient(1).Put(MakePutFreshEntry("k0", []byte("v0")))
			sc.GetClient(0).Gossip()

			res := sc.GetClient(1).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0"),
			}))
		})

		It("should not replicate remote entries to local.", func() {
			sc.GetClient(1).Put(MakePutFreshEntry("k0", []byte("v0")))
			sc.GetClient(0).Gossip()

			res := sc.GetClient(0).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{}))
		})

		It("should replicate entries with different keys to other server.", func() {
			sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0")))
			sc.GetClient(0).Put(MakePutFreshEntry("k1", []byte("v1")))
			sc.GetClient(0).Gossip()

			res := sc.GetClient(1).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0"),
			}))

			res = sc.GetClient(1).Get("k1")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v1"),
			}))
		})

		It("should replicate conflict entries to other server.", func() {
			// Unsafe: create context (vector clock) on client side
			sc.GetClient(0).Put(MakePutFromVectorClockMapAndValue(
				"k0",
				map[string]uint64{sc.GetID(0): 1, sc.GetID(2): 2},
				[]byte("v0-0"),
			))
			sc.GetClient(0).Put(MakePutFromVectorClockMapAndValue(
				"k0",
				map[string]uint64{sc.GetID(0): 2, sc.GetID(2): 1},
				[]byte("v0-1"),
			))
			sc.GetClient(0).Gossip()

			res := sc.GetClient(1).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0-0"),
				[]byte("v0-1"),
			}))
		})

		Context("when there are conflict entries in remote server", func() {
			It("should replicate entries and keep conflict remote entries.", func() {
				sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0-0")))
				sc.GetClient(1).Put(MakePutFreshEntry("k0", []byte("v0-1")))

				sc.GetClient(0).Gossip()

				res := sc.GetClient(1).Get("k0")
				Expect(res).NotTo(BeNil())
				Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
					[]byte("v0-0"),
					[]byte("v0-1"),
				}))
			})

			It("should replicate entries and keep conflict remote entries (2).", func() {
				// Unsafe: create context (vector clock) on client side
				sc.GetClient(0).Put(MakePutFromVectorClockMapAndValue(
					"k0",
					map[string]uint64{sc.GetID(0): 1, sc.GetID(2): 2},
					[]byte("v0-0"),
				))
				sc.GetClient(0).Put(MakePutFromVectorClockMapAndValue(
					"k0",
					map[string]uint64{sc.GetID(0): 2, sc.GetID(2): 1},
					[]byte("v0-1"),
				))
				sc.GetClient(1).Put(MakePutFreshEntry("k0", []byte("v0-2")))

				sc.GetClient(0).Gossip()

				res := sc.GetClient(1).Get("k0")
				Expect(res).NotTo(BeNil())
				Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
					[]byte("v0-0"),
					[]byte("v0-1"),
					[]byte("v0-2"),
				}))
			})

			It("should not replicate local ancestor entries.", func() {
				// Unsafe: create context (vector clock) on client side
				sc.GetClient(0).Put(MakePutFromVectorClockMapAndValue(
					"k0",
					map[string]uint64{},
					[]byte("v0-0"),
				))
				sc.GetClient(1).Put(MakePutFromVectorClockMapAndValue(
					"k0",
					map[string]uint64{sc.GetID(0): 1},
					[]byte("v0-1"),
				))
				sc.GetClient(0).Gossip()

				res := sc.GetClient(1).Get("k0")
				Expect(res).NotTo(BeNil())
				Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
					[]byte("v0-1"),
				}))
			})

			It("should not replicate local ancestor entries (2).", func() {
				// Unsafe: create context (vector clock) on client side
				sc.GetClient(0).Put(MakePutFromVectorClockMapAndValue(
					"k0",
					map[string]uint64{sc.GetID(0): 1, sc.GetID(1): 1},
					[]byte("v0-0"),
				))
				sc.GetClient(1).Put(MakePutFromVectorClockMapAndValue(
					"k0",
					map[string]uint64{sc.GetID(0): 2, sc.GetID(1): 2},
					[]byte("v0-1"),
				))
				sc.GetClient(0).Gossip()

				res := sc.GetClient(1).Get("k0")
				Expect(res).NotTo(BeNil())
				Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
					[]byte("v0-1"),
				}))
			})

			It("should replicate entries and remove remote descendant entries.", func() {
				// Unsafe: create context (vector clock) on client side
				sc.GetClient(0).Put(MakePutFromVectorClockMapAndValue(
					"k0",
					map[string]uint64{sc.GetID(1): 1},
					[]byte("v0-0"),
				))
				sc.GetClient(1).Put(MakePutFromVectorClockMapAndValue(
					"k0",
					map[string]uint64{},
					[]byte("v0-1"),
				))
				sc.GetClient(0).Gossip()

				res := sc.GetClient(1).Get("k0")
				Expect(res).NotTo(BeNil())
				Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
					[]byte("v0-0"),
				}))
			})

			It("should replicate entries and remove remote descendant entries (2).", func() {
				// Unsafe: create context (vector clock) on client side
				sc.GetClient(0).Put(MakePutFromVectorClockMapAndValue(
					"k0",
					map[string]uint64{sc.GetID(0): 2, sc.GetID(1): 3},
					[]byte("v0-0"),
				))
				sc.GetClient(1).Put(MakePutFromVectorClockMapAndValue(
					"k0",
					map[string]uint64{sc.GetID(0): 1},
					[]byte("v0-1"),
				))
				sc.GetClient(0).Gossip()

				res := sc.GetClient(1).Get("k0")
				Expect(res).NotTo(BeNil())
				Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
					[]byte("v0-0"),
				}))
			})
		})
	})

	Describe("Three Servers", func() {
		BeforeEach(func() {
			// StartingPort: 8000, R-Value: 1, W-Value: 1, ClusterSize: 3
			sc = NewServerCoordinator(8000, 1, 1, 3)
		})

		AfterEach(func() {
			sc.Kill()
		})

		It("should replicate entry to other server.", func() {
			sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0")))
			sc.GetClient(0).Gossip()

			res1 := sc.GetClient(1).Get("k0")
			Expect(res1).NotTo(BeNil())
			Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
				[]byte("v0"),
			}))

			res2 := sc.GetClient(2).Get("k0")
			Expect(res2).NotTo(BeNil())
			Expect(GetEntryValues(res2)).To(ConsistOf([][]byte{
				[]byte("v0"),
			}))
		})

		It("should replicate entries with different keys to other server.", func() {
			sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0")))
			sc.GetClient(0).Put(MakePutFreshEntry("k1", []byte("v1")))
			sc.GetClient(0).Gossip()

			res1 := sc.GetClient(1).Get("k0")
			Expect(res1).NotTo(BeNil())
			Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
				[]byte("v0"),
			}))

			res1 = sc.GetClient(1).Get("k1")
			Expect(res1).NotTo(BeNil())
			Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
				[]byte("v1"),
			}))

			res2 := sc.GetClient(2).Get("k0")
			Expect(res2).NotTo(BeNil())
			Expect(GetEntryValues(res2)).To(ConsistOf([][]byte{
				[]byte("v0"),
			}))

			res2 = sc.GetClient(2).Get("k1")
			Expect(res2).NotTo(BeNil())
			Expect(GetEntryValues(res2)).To(ConsistOf([][]byte{
				[]byte("v1"),
			}))
		})

		It("should replicate conflict entries to other server.", func() {
			// Unsafe: create context (vector clock) on client side
			sc.GetClient(0).Put(MakePutFromVectorClockMapAndValue(
				"k0",
				map[string]uint64{sc.GetID(0): 1, sc.GetID(2): 2},
				[]byte("v0-0"),
			))
			sc.GetClient(0).Put(MakePutFromVectorClockMapAndValue(
				"k0",
				map[string]uint64{sc.GetID(0): 2, sc.GetID(2): 1},
				[]byte("v0-1"),
			))
			sc.GetClient(0).Gossip()

			res1 := sc.GetClient(1).Get("k0")
			Expect(res1).NotTo(BeNil())
			Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
				[]byte("v0-0"),
				[]byte("v0-1"),
			}))

			res2 := sc.GetClient(2).Get("k0")
			Expect(res2).NotTo(BeNil())
			Expect(GetEntryValues(res2)).To(ConsistOf([][]byte{
				[]byte("v0-0"),
				[]byte("v0-1"),
			}))
		})

		Context("when there are conflict entries in remote server", func() {
			It("should replicate entries and keep conflict remote entries.", func() {
				sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0-0")))
				sc.GetClient(1).Put(MakePutFreshEntry("k0", []byte("v0-1")))

				sc.GetClient(0).Gossip()

				res1 := sc.GetClient(1).Get("k0")
				Expect(res1).NotTo(BeNil())
				Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
					[]byte("v0-0"),
					[]byte("v0-1"),
				}))

				res2 := sc.GetClient(2).Get("k0")
				Expect(res2).NotTo(BeNil())
				Expect(GetEntryValues(res2)).To(ConsistOf([][]byte{
					[]byte("v0-0"),
				}))
			})

			It("should replicate entries and keep conflict remote entries (2).", func() {
				// Unsafe: create context (vector clock) on client side
				sc.GetClient(0).Put(MakePutFromVectorClockMapAndValue(
					"k0",
					map[string]uint64{sc.GetID(0): 1, sc.GetID(2): 2},
					[]byte("v0-0"),
				))
				sc.GetClient(0).Put(MakePutFromVectorClockMapAndValue(
					"k0",
					map[string]uint64{sc.GetID(0): 2, sc.GetID(2): 1},
					[]byte("v0-1"),
				))
				sc.GetClient(1).Put(MakePutFreshEntry("k0", []byte("v0-2")))

				sc.GetClient(0).Gossip()

				res1 := sc.GetClient(1).Get("k0")
				Expect(res1).NotTo(BeNil())
				Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
					[]byte("v0-0"),
					[]byte("v0-1"),
					[]byte("v0-2"),
				}))

				res2 := sc.GetClient(0).Get("k0")
				Expect(res2).NotTo(BeNil())
				Expect(GetEntryValues(res2)).To(ConsistOf([][]byte{
					[]byte("v0-0"),
					[]byte("v0-1"),
				}))
			})

			It("should not replicate local ancestor entries.", func() {
				// Unsafe: create context (vector clock) on client side
				sc.GetClient(0).Put(MakePutFromVectorClockMapAndValue(
					"k0",
					map[string]uint64{},
					[]byte("v0-0"),
				))
				sc.GetClient(1).Put(MakePutFromVectorClockMapAndValue(
					"k0",
					map[string]uint64{sc.GetID(0): 1},
					[]byte("v0-1"),
				))
				sc.GetClient(0).Gossip()

				res1 := sc.GetClient(1).Get("k0")
				Expect(res1).NotTo(BeNil())
				Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
					[]byte("v0-1"),
				}))

				res2 := sc.GetClient(2).Get("k0")
				Expect(res2).NotTo(BeNil())
				Expect(GetEntryValues(res2)).To(ConsistOf([][]byte{
					[]byte("v0-0"),
				}))
			})

			It("should not replicate local ancestor entries (2).", func() {
				// Unsafe: create context (vector clock) on client side
				sc.GetClient(0).Put(MakePutFromVectorClockMapAndValue(
					"k0",
					map[string]uint64{sc.GetID(0): 1, sc.GetID(1): 1},
					[]byte("v0-0"),
				))
				sc.GetClient(1).Put(MakePutFromVectorClockMapAndValue(
					"k0",
					map[string]uint64{sc.GetID(0): 2, sc.GetID(1): 2},
					[]byte("v0-1"),
				))
				sc.GetClient(0).Gossip()

				res1 := sc.GetClient(1).Get("k0")
				Expect(res1).NotTo(BeNil())
				Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
					[]byte("v0-1"),
				}))

				res2 := sc.GetClient(2).Get("k0")
				Expect(res2).NotTo(BeNil())
				Expect(GetEntryValues(res2)).To(ConsistOf([][]byte{
					[]byte("v0-0"),
				}))
			})

			It("should replicate entries and remove remote descendant entries.", func() {
				// Unsafe: create context (vector clock) on client side
				sc.GetClient(0).Put(MakePutFromVectorClockMapAndValue(
					"k0",
					map[string]uint64{sc.GetID(1): 1},
					[]byte("v0-0"),
				))
				sc.GetClient(1).Put(MakePutFromVectorClockMapAndValue(
					"k0",
					map[string]uint64{},
					[]byte("v0-1"),
				))
				sc.GetClient(0).Gossip()

				res1 := sc.GetClient(1).Get("k0")
				Expect(res1).NotTo(BeNil())
				Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
					[]byte("v0-0"),
				}))

				res2 := sc.GetClient(2).Get("k0")
				Expect(res2).NotTo(BeNil())
				Expect(GetEntryValues(res2)).To(ConsistOf([][]byte{
					[]byte("v0-0"),
				}))
			})

			It("should replicate entries and remove remote descendant entries (2).", func() {
				// Unsafe: create context (vector clock) on client side
				sc.GetClient(0).Put(MakePutFromVectorClockMapAndValue(
					"k0",
					map[string]uint64{sc.GetID(0): 2, sc.GetID(1): 3},
					[]byte("v0-0"),
				))
				sc.GetClient(1).Put(MakePutFromVectorClockMapAndValue(
					"k0",
					map[string]uint64{sc.GetID(0): 1},
					[]byte("v0-1"),
				))
				sc.GetClient(0).Gossip()

				res1 := sc.GetClient(1).Get("k0")
				Expect(res1).NotTo(BeNil())
				Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
					[]byte("v0-0"),
				}))

				res2 := sc.GetClient(2).Get("k0")
				Expect(res2).NotTo(BeNil())
				Expect(GetEntryValues(res2)).To(ConsistOf([][]byte{
					[]byte("v0-0"),
				}))
			})
		})
	})
})
