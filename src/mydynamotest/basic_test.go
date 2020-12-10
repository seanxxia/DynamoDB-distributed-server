package mydynamotest

import (
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
)

var _ = Describe("Basic Put & Get", func() {

	Describe("Single Server", func() {
		var sc ServerCoordinator

		BeforeEach(func() {
			// StartingPort: 8000, R-Value: 1, W-Value: 1, ClusterSize: 1
			sc = NewServerCoordinator(8000+config.GinkgoConfig.ParallelNode*100, 1, 1, 1)
		})

		AfterEach(func() {
			sc.Kill()
		})

		It("should handle entry put and get.", func() {
			sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0")))

			res := sc.GetClient(0).Get("k0")

			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0"),
			}))
		})

		It("should handle entry put, get, and update.", func() {
			sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0")))

			entry := sc.GetClient(0).Get("k0").EntryList[0]
			entry.Value = []byte("v0-1")

			sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

			res := sc.GetClient(0).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0-1"),
			}))
		})

		It("should handle non-existent entry(key) get.", func() {
			res := sc.GetClient(0).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{}))
		})

		It("should ignore put with ancestor context.", func() {
			// Spec: If the context that the Node has already stored associated with the
			// specified key is causally descended from the context provided to Put, i.e
			// newContext < oldContext, Put will fail and the existing value will remain.

			sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0")))

			entry := sc.GetClient(0).Get("k0").EntryList[0]
			entry.Value = []byte("v0-1")
			sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

			// Put a new value with the same key but new context
			sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v1")))

			res := sc.GetClient(0).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0-1"),
			}))
		})

		It("should ignore put with equal context.", func() {
			sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0")))

			entry := sc.GetClient(0).Get("k0").EntryList[0]
			entry.Value = []byte("v0-1")
			sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

			// Put a new value with the same key but context from previous get
			entry.Value = []byte("v0-2")
			sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

			res := sc.GetClient(0).Get("k0")
			Expect(res).NotTo(BeNil())
			Expect(GetEntryValues(res)).To(ConsistOf([][]byte{
				[]byte("v0-1"),
			}))
		})
	})

	Describe("Two servers", func() {
		var sc ServerCoordinator

		BeforeEach(func() {
			// StartingPort: 8000, R-Value: 1, W-Value: 1, ClusterSize: 1
			sc = NewServerCoordinator(8000+config.GinkgoConfig.ParallelNode*100, 1, 1, 2)
		})

		AfterEach(func() {
			sc.Kill()
		})

		It("should handle entry put and get without interfering each other (same key).", func() {
			sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0-s0")))
			sc.GetClient(1).Put(MakePutFreshEntry("k0", []byte("v0-s1")))

			res0 := sc.GetClient(0).Get("k0")
			Expect(res0).NotTo(BeNil())
			Expect(GetEntryValues(res0)).To(ConsistOf([][]byte{
				[]byte("v0-s0"),
			}))

			res1 := sc.GetClient(1).Get("k0")
			Expect(res1).NotTo(BeNil())
			Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
				[]byte("v0-s1"),
			}))
		})

		It("should handle entry put and get without interfering each other (different keys).", func() {
			sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0-s0")))
			sc.GetClient(1).Put(MakePutFreshEntry("k1", []byte("v1-s1")))

			res0 := sc.GetClient(0).Get("k0")
			Expect(res0).NotTo(BeNil())
			Expect(GetEntryValues(res0)).To(ConsistOf([][]byte{
				[]byte("v0-s0"),
			}))

			res1 := sc.GetClient(1).Get("k1")
			Expect(res1).NotTo(BeNil())
			Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
				[]byte("v1-s1"),
			}))
		})

		It("should handle entry put, get, and update without interfering each other.", func() {
			sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0-s0")))
			sc.GetClient(1).Put(MakePutFreshEntry("k1", []byte("v1-s1")))

			// Update entry with key "k0" on server-0
			entry := sc.GetClient(0).Get("k0").EntryList[0]
			entry.Value = []byte("v0-s0-1")
			sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

			res0 := sc.GetClient(0).Get("k0")
			Expect(res0).NotTo(BeNil())
			Expect(GetEntryValues(res0)).To(ConsistOf([][]byte{
				[]byte("v0-s0-1"),
			}))

			res1 := sc.GetClient(1).Get("k1")
			Expect(res1).NotTo(BeNil())
			Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
				[]byte("v1-s1"),
			}))
		})

		Context("when put and get conflict entries", func() {

			It("should handle put and get conflict entries.", func() {
				sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0-s0")))
				sc.GetClient(1).Put(MakePutFreshEntry("k0", []byte("v0-s1")))

				// Get entry from server-0, update it, and put it back to server-0
				entry := sc.GetClient(0).Get("k0").EntryList[0]
				entry.Value = []byte("v0-s0-1")
				sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

				// Get entry from server-1, update it, and put to server-0
				entry = sc.GetClient(1).Get("k0").EntryList[0]
				entry.Value = []byte("v0-s1-1")
				sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

				res0 := sc.GetClient(0).Get("k0")
				Expect(res0).NotTo(BeNil())
				Expect(GetEntryValues(res0)).To(ConsistOf([][]byte{
					[]byte("v0-s0-1"), // Original value
					[]byte("v0-s1-1"), // Conflict value
				}))
			})

			It("should handle put and get conflict entries with same value.", func() {
				sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0-s0")))
				sc.GetClient(1).Put(MakePutFreshEntry("k0", []byte("v0-s1")))

				// Get entry from server-0, update it, and put it back to server-0
				entry := sc.GetClient(0).Get("k0").EntryList[0]
				entry.Value = []byte("v0-s0-1")
				sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

				// Get entry from server-1, update it, and put to server-0
				entry = sc.GetClient(1).Get("k0").EntryList[0]
				entry.Value = []byte("v0-s0-1")
				sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

				res0 := sc.GetClient(0).Get("k0")
				Expect(res0).NotTo(BeNil())
				Expect(GetEntryValues(res0)).To(ConsistOf([][]byte{
					[]byte("v0-s0-1"), // Original value
					[]byte("v0-s0-1"), // Duplicated value with different context
				}))
			})

			It("should handle put and get conflict entry with same context.", func() {
				// Reference:
				// - https://piazza.com/class/kfqynl4r6a0317?cid=969

				sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0-s0")))

				// Get entry from server-0, update it, and put it back to server-0
				entry := sc.GetClient(0).Get("k0").EntryList[0]
				entry.Value = []byte("v0-s0-1")
				sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

				// Update the same entry previously get, and put it to server-0 again
				entry.Value = []byte("v0-s0-2")
				sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

				res0 := sc.GetClient(0).Get("k0")
				Expect(res0).NotTo(BeNil())
				Expect(GetEntryValues(res0)).To(ConsistOf([][]byte{
					[]byte("v0-s0-1"), // Should only keep the original value
				}))
			})

			It("should resolve conflicts by keeping entries with descendant context (1).", func() {
				sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0-s0")))
				sc.GetClient(1).Put(MakePutFreshEntry("k0", []byte("v0-s1")))

				// Get entry from server-0, update it, and put it back to server-0
				entry := sc.GetClient(0).Get("k0").EntryList[0]
				entry.Value = []byte("v0-s0-1")
				sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

				// Get entry from server-1, update it, and put to server-0
				entry = sc.GetClient(1).Get("k0").EntryList[0]
				entry.Value = []byte("v0-s1-1")
				sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

				// The result from server-0 here should have multiple conflict entries
				// Put the entry to server-0 with new value and combined context
				res0 := sc.GetClient(0).Get("k0")
				entry = res0.EntryList[0]
				entry.Context.Clock.Combine(GetEntryContextClocks(res0))
				entry.Value = []byte("v0-s0-2")
				sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

				res0 = sc.GetClient(0).Get("k0")
				Expect(res0).NotTo(BeNil())
				Expect(GetEntryValues(res0)).To(ConsistOf([][]byte{
					[]byte("v0-s0-2"),
				}))
			})

			It("should resolve conflicts by keeping entries with descendant context (2).", func() {
				sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0-s0")))
				sc.GetClient(1).Put(MakePutFreshEntry("k0", []byte("v0-s1")))

				// Get entry from server-0, update it, and put it back to server-0
				entry := sc.GetClient(0).Get("k0").EntryList[0]
				entry.Value = []byte("v0-s0-1")
				sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

				// Get entry from server-1, update it, and put to server-0
				entry = sc.GetClient(1).Get("k0").EntryList[0]
				entry.Value = []byte("v0-s1-1")
				sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

				// Get entry from server-1, update it, and put it back to server-1
				entry = sc.GetClient(1).Get("k0").EntryList[0]
				entry.Value = []byte("v0-s1-2")
				sc.GetClient(1).Put(MakePutFromEntry("k0", entry))

				// Get the entries from server-0 and server-1, combine their contexts, and put to server-0 with new value
				res0 := sc.GetClient(0).Get("k0")
				res1 := sc.GetClient(1).Get("k0")
				entry = res0.EntryList[0]
				entry.Context.Clock.Combine(GetEntryContextClocks(res0))
				entry.Context.Clock.Combine(GetEntryContextClocks(res1))
				entry.Value = []byte("v0-s0-3")
				sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

				res0 = sc.GetClient(0).Get("k0")
				Expect(res0).NotTo(BeNil())
				Expect(GetEntryValues(res0)).To(ConsistOf([][]byte{
					[]byte("v0-s0-3"),
				}))
			})

		})
	})

	Describe("Three servers", func() {
		var sc ServerCoordinator

		BeforeEach(func() {
			sc = NewServerCoordinator(8000+config.GinkgoConfig.ParallelNode*100, 1, 1, 3)
		})

		AfterEach(func() {
			sc.Kill()
		})

		It("should handle entry put and get without interfering each other (same key).", func() {
			sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0-s0")))
			sc.GetClient(1).Put(MakePutFreshEntry("k0", []byte("v0-s1")))
			sc.GetClient(2).Put(MakePutFreshEntry("k0", []byte("v0-s2")))

			res0 := sc.GetClient(0).Get("k0")
			Expect(res0).NotTo(BeNil())
			Expect(GetEntryValues(res0)).To(ConsistOf([][]byte{
				[]byte("v0-s0"),
			}))

			res1 := sc.GetClient(1).Get("k0")
			Expect(res1).NotTo(BeNil())
			Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
				[]byte("v0-s1"),
			}))

			res2 := sc.GetClient(2).Get("k0")
			Expect(res2).NotTo(BeNil())
			Expect(GetEntryValues(res2)).To(ConsistOf([][]byte{
				[]byte("v0-s2"),
			}))
		})

		It("should handle entry put and get without interfering each other (different keys).", func() {
			sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0-s0")))
			sc.GetClient(1).Put(MakePutFreshEntry("k1", []byte("v1-s1")))
			sc.GetClient(2).Put(MakePutFreshEntry("k2", []byte("v2-s2")))

			res0 := sc.GetClient(0).Get("k0")
			Expect(res0).NotTo(BeNil())
			Expect(GetEntryValues(res0)).To(ConsistOf([][]byte{
				[]byte("v0-s0"),
			}))

			res1 := sc.GetClient(1).Get("k1")
			Expect(res1).NotTo(BeNil())
			Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
				[]byte("v1-s1"),
			}))

			res2 := sc.GetClient(2).Get("k2")
			Expect(res2).NotTo(BeNil())
			Expect(GetEntryValues(res2)).To(ConsistOf([][]byte{
				[]byte("v2-s2"),
			}))
		})

		It("should handle entry put, get, and update without interfering each other.", func() {
			sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0-s0")))
			sc.GetClient(1).Put(MakePutFreshEntry("k1", []byte("v1-s1")))
			sc.GetClient(2).Put(MakePutFreshEntry("k2", []byte("v2-s2")))

			// Update entry with key "k0" on server-0
			entry := sc.GetClient(0).Get("k0").EntryList[0]
			entry.Value = []byte("v0-s0-1")
			sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

			res0 := sc.GetClient(0).Get("k0")
			Expect(res0).NotTo(BeNil())
			Expect(GetEntryValues(res0)).To(ConsistOf([][]byte{
				[]byte("v0-s0-1"),
			}))

			res1 := sc.GetClient(1).Get("k1")
			Expect(res1).NotTo(BeNil())
			Expect(GetEntryValues(res1)).To(ConsistOf([][]byte{
				[]byte("v1-s1"),
			}))

			res2 := sc.GetClient(2).Get("k2")
			Expect(res2).NotTo(BeNil())
			Expect(GetEntryValues(res2)).To(ConsistOf([][]byte{
				[]byte("v2-s2"),
			}))
		})

		Context("when put and get conflict entries", func() {

			It("should handle put and get conflict entries.", func() {
				sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0-s0")))
				sc.GetClient(1).Put(MakePutFreshEntry("k0", []byte("v0-s1")))
				sc.GetClient(2).Put(MakePutFreshEntry("k0", []byte("v0-s2")))

				// Get entry from server-0, update it, and put it back to server-0
				entry := sc.GetClient(0).Get("k0").EntryList[0]
				entry.Value = []byte("v0-s0-1")
				sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

				// Get entry from server-1, update it, and put to server-0
				entry = sc.GetClient(1).Get("k0").EntryList[0]
				entry.Value = []byte("v0-s1-1")
				sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

				// Get entry from server-1, update it, and put to server-0
				entry = sc.GetClient(2).Get("k0").EntryList[0]
				entry.Value = []byte("v0-s2-1")
				sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

				res0 := sc.GetClient(0).Get("k0")
				Expect(res0).NotTo(BeNil())
				Expect(GetEntryValues(res0)).To(ConsistOf([][]byte{
					[]byte("v0-s0-1"), // Original value
					[]byte("v0-s1-1"), // Conflict value
					[]byte("v0-s2-1"), // Conflict value
				}))
			})

			It("should handle put and get conflict entries with same value.", func() {
				sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0-s0")))
				sc.GetClient(1).Put(MakePutFreshEntry("k0", []byte("v0-s1")))
				sc.GetClient(2).Put(MakePutFreshEntry("k0", []byte("v0-s2")))

				// Get entry from server-0, update it, and put it back to server-0
				entry := sc.GetClient(0).Get("k0").EntryList[0]
				entry.Value = []byte("v0-s0-1")
				sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

				// Get entry from server-1, update it, and put to server-0
				entry = sc.GetClient(1).Get("k0").EntryList[0]
				entry.Value = []byte("v0-s0-1")
				sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

				// Get entry from server-1, update it, and put to server-0
				entry = sc.GetClient(2).Get("k0").EntryList[0]
				entry.Value = []byte("v0-s0-1")
				sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

				res0 := sc.GetClient(0).Get("k0")
				Expect(res0).NotTo(BeNil())
				Expect(GetEntryValues(res0)).To(ConsistOf([][]byte{
					[]byte("v0-s0-1"), // Original value
					[]byte("v0-s0-1"), // Duplicated value with different context
					[]byte("v0-s0-1"), // Duplicated value with different context
				}))
			})

			It("should resolve conflicts by keeping entries with descendant context (1).", func() {
				sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0-s0")))
				sc.GetClient(1).Put(MakePutFreshEntry("k0", []byte("v0-s1")))
				sc.GetClient(2).Put(MakePutFreshEntry("k0", []byte("v0-s2")))

				// Get entry from server-0, update it, and put it back to server-0
				entry := sc.GetClient(0).Get("k0").EntryList[0]
				entry.Value = []byte("v0-s0-1")
				sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

				// Get entry from server-1, update it, and put to server-0
				entry = sc.GetClient(1).Get("k0").EntryList[0]
				entry.Value = []byte("v0-s1-1")
				sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

				// Get entry from server-2, update it, and put to server-0
				entry = sc.GetClient(1).Get("k0").EntryList[0]
				entry.Value = []byte("v0-s2-1")
				sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

				// The result from server-0 here should have multiple conflict entries
				// Put the entry to server-0 with new value and combined context
				res0 := sc.GetClient(0).Get("k0")
				entry = res0.EntryList[0]
				entry.Context.Clock.Combine(GetEntryContextClocks(res0))
				entry.Value = []byte("v0-s0-2")
				sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

				res0 = sc.GetClient(0).Get("k0")
				Expect(res0).NotTo(BeNil())
				Expect(GetEntryValues(res0)).To(ConsistOf([][]byte{
					[]byte("v0-s0-2"),
				}))
			})

			It("should resolve conflicts by keeping entries with descendant context (2).", func() {
				sc.GetClient(0).Put(MakePutFreshEntry("k0", []byte("v0-s0")))
				sc.GetClient(1).Put(MakePutFreshEntry("k0", []byte("v0-s1")))
				sc.GetClient(2).Put(MakePutFreshEntry("k0", []byte("v0-s2")))

				// Get entry from server-0, update it, and put it back to server-0
				entry := sc.GetClient(0).Get("k0").EntryList[0]
				entry.Value = []byte("v0-s0-1")
				sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

				// Get entry from server-1, update it, and put to server-0
				entry = sc.GetClient(1).Get("k0").EntryList[0]
				entry.Value = []byte("v0-s1-1")
				sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

				// Get entry from server-1, update it, and put it back to server-1
				entry = sc.GetClient(1).Get("k0").EntryList[0]
				entry.Value = []byte("v0-s1-2")
				sc.GetClient(1).Put(MakePutFromEntry("k0", entry))

				// Get entry from server-2, update it, and put to server-0
				entry = sc.GetClient(2).Get("k0").EntryList[0]
				entry.Value = []byte("v0-s2-1")
				sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

				// Get entry from server-2, update it, and put it back to server-2
				entry = sc.GetClient(2).Get("k0").EntryList[0]
				entry.Value = []byte("v0-s2-2")
				sc.GetClient(1).Put(MakePutFromEntry("k0", entry))

				// Get the entries from server-0 ,server-1, and server-2, combine their contexts, and put to server-0 with new value
				res0 := sc.GetClient(0).Get("k0")
				res1 := sc.GetClient(1).Get("k0")
				res2 := sc.GetClient(2).Get("k0")

				entry = res0.EntryList[0]
				entry.Context.Clock.Combine(GetEntryContextClocks(res0))
				entry.Context.Clock.Combine(GetEntryContextClocks(res1))
				entry.Context.Clock.Combine(GetEntryContextClocks(res2))
				entry.Value = []byte("v0-s0-3")
				sc.GetClient(0).Put(MakePutFromEntry("k0", entry))

				res0 = sc.GetClient(0).Get("k0")
				Expect(res0).NotTo(BeNil())
				Expect(GetEntryValues(res0)).To(ConsistOf([][]byte{
					[]byte("v0-s0-3"),
				}))
			})

		})
	})
})
