package mydynamo

//The vector clock data type for dynamo server events
type VectorClock struct {
	NodeClocks map[string]uint64
}

//Creates a new VectorClock
func NewVectorClock() VectorClock {
	return VectorClock{
		NodeClocks: make(map[string]uint64),
	}
}

// Creates a new VectorClock from local clock map (for testing only)
func NewVectorClockFromMap(localClocks map[string]uint64) VectorClock {
	return VectorClock{
		NodeClocks: localClocks,
	}
}

//Returns true if the other VectorClock is causally descended from this one
func (s VectorClock) LessThan(otherVectorClock VectorClock) bool {
	if s.Equals(otherVectorClock) {
		return false
	}

	for nodeID, clock := range s.NodeClocks {
		if otherClock, ok := otherVectorClock.NodeClocks[nodeID]; !ok || clock > otherClock {
			return false
		}
	}

	return true
}

//Returns true if neither VectorClock is causally descended from the other
func (s VectorClock) Concurrent(otherVectorClock VectorClock) bool {
	// NOTE: Equal vector clocks are not concurrent
	// Reference:
	// - https://piazza.com/class/kfqynl4r6a0317?cid=915
	return !s.Equals(otherVectorClock) && !s.LessThan(otherVectorClock) && !otherVectorClock.LessThan(s)
}

//Increments this VectorClock at the element associated with nodeId
func (s *VectorClock) Increment(nodeID string) {
	if _, ok := s.NodeClocks[nodeID]; !ok {
		s.NodeClocks[nodeID] = 0
	}
	s.NodeClocks[nodeID]++
}

//Changes this VectorClock to be causally descended from all VectorClocks in clocks
//Notes from piazza:
//	The intent is for a client to be able to combine the vector clocks of conflicting elements,
//	and use the result of Combine() inside a Context as an argument to Put(), which would perform the
//	increment, and thus make this combined vector clock causally descended.
func (s *VectorClock) Combine(vectorClocks []VectorClock) {
	for _, vectorClock := range vectorClocks {
		for nodeID, clock := range vectorClock.NodeClocks {
			if s.NodeClocks[nodeID] < clock {
				s.NodeClocks[nodeID] = clock
			}
		}
	}
}

//Tests if two VectorClocks are equal
func (s *VectorClock) Equals(otherVectorClock VectorClock) bool {
	if len(s.NodeClocks) != len(otherVectorClock.NodeClocks) {
		return false
	}

	for nodeID, clock := range s.NodeClocks {
		if otherClock, ok := otherVectorClock.NodeClocks[nodeID]; !ok || clock != otherClock {
			return false
		}
	}

	return true
}

// Returns the vector clock as map. (for testing only)
func (s *VectorClock) ToMap() map[string]uint64 {
	localClocks := make(map[string]uint64)
	for k, v := range s.NodeClocks {
		localClocks[k] = v
	}
	return localClocks
}
