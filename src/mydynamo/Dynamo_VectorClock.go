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
	return !s.LessThan(otherVectorClock) && !otherVectorClock.LessThan(s)
}

//Increments this VectorClock at the element associated with nodeId
func (s *VectorClock) Increment(nodeID string) {
	if _, ok := s.NodeClocks[nodeID]; !ok {
		s.NodeClocks[nodeID] = 0
	}
	s.NodeClocks[nodeID]++
}

//Changes this VectorClock to be causally descended from all VectorClocks in clocks
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
