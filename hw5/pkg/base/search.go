package base

import (
	"container/list"
	"fmt"
	"reflect"
)

type SearchResult struct {
	Success    bool
	Targets    []*State
	Invalidate *State
	// Number of explored states
	N int
}

type stateHashMap map[uint64][]*State

func (m stateHashMap) add(s *State) {
	m[s.Hash()] = append(m[s.Hash()], s)
}

func (m stateHashMap) has(s *State) bool {
	states := m[s.Hash()]
	if states == nil {
		return false
	}

	for _, state := range states {
		if state.Equals(s) {
			return true
		}
	}

	return false
}

func BfsFind(initState *State, validate, goalPredicate func(*State) bool, limitDepth int) (result SearchResult) {
	queue := list.New()
	queue.PushBack(initState)

	explored := stateHashMap{}
	explored.add(initState)

	result.N = 0

	for queue.Len() > 0 {
		v := queue.Remove(queue.Front())
		state := v.(*State)
		result.N++

		if goalPredicate(state) {
			result.Success = true
			result.Targets = []*State{state}
			return
		}

		// No need to add more states into the queue if the depth is too large
		if limitDepth >= 0 && state.Depth == limitDepth {
			continue
		}

		for _, newState := range state.NextStates() {
			if explored.has(newState) {
				continue
			}

			if !validate(newState) {
				result.Success = false
				result.Invalidate = newState
				return
			}

			explored.add(newState)
			queue.PushBack(newState)
		}
	}

	result.Success = false
	return
}

func BfsFindAll(initState *State, validate, goalPredicate func(*State) bool, depth int) (result SearchResult) {
	if depth < 0 {
		panic("depth must be non-negative")
	}

	if !validate(initState) {
		result.Success = false
		result.Invalidate = initState
		return
	}

	queue := list.New()
	queue.PushBack(initState)

	explored := stateHashMap{}
	explored.add(initState)

	result.N = 0

	for queue.Len() > 0 {
		v := queue.Remove(queue.Front())
		state := v.(*State)
		result.N++

		if goalPredicate != nil && goalPredicate(state) {
			result.Targets = append(result.Targets, state)
		}

		// No need to add more states into the queue if the depth is too large
		if state.Depth == depth {
			continue
		}

		for _, newState := range state.NextStates() {
			if explored.has(newState) {
				continue
			}

			if !validate(newState) {
				result.Success = false
				result.Invalidate = newState
				return
			}

			explored.add(newState)
			queue.PushBack(newState)
		}
	}

	result.Success = true
	return
}

func RandomWalkFind(initState *State, validate, goalPredicate func(*State) bool, depth int) (result SearchResult) {
	if depth < 0 {
		panic("depth must be non-negative")
	}

	state := initState
	for state.Depth <= depth {
		if !validate(state) {
			result.Success = false
			result.Invalidate = state
			return
		}

		if goalPredicate(state) {
			result.Success = true
			result.Targets = append(result.Targets, state)
			return
		}

		state = state.RandomNextState()
	}

	return
}

func RandomWalkValidate(initState *State, validate, goalPredicate func(*State) bool, depth int) (result SearchResult) {
	if depth < 0 {
		panic("depth must be non-negative")
	}

	state := initState
	for state.Depth <= depth {
		if !validate(state) {
			result.Success = false
			result.Targets = nil
			result.Invalidate = state
			return
		}

		if goalPredicate != nil && goalPredicate(state) {
			result.Targets = []*State{state}
		}

		state = state.RandomNextState()
	}

	result.Success = true
	result.N = state.Depth
	return
}

func BatchRandomWalkFind(initState *State, validate, goalPredicate func(*State) bool,
	depth, batch int) (result SearchResult) {

	for i := 0; i < batch; i++ {
		res := RandomWalkFind(initState, validate, goalPredicate, depth)
		if res.Success {
			res.N = i + 1
			return res
		}
	}
	result.N = batch
	result.Success = false
	return
}

func BatchRandomWalkValidate(initState *State, validate, goalPredicate func(*State) bool,
	depth, batch int) (result SearchResult) {

	for i := 0; i < batch; i++ {
		res := RandomWalkValidate(initState, validate, goalPredicate, depth)
		if !res.Success {
			return res
		}

		if len(res.Targets) > 0 {
			result.Targets = append(result.Targets, res.Targets[0])
		}
		result.N += res.N
	}

	result.Success = true
	return
}

type StateEdge struct {
	From  *State
	Event Event
	To    *State
}

func FindPath(final *State) (*State, []StateEdge) {
	edges := make([]StateEdge, 0, 16)
	for final.Prev != nil {
		edge := StateEdge{
			From:  final.Prev,
			Event: final.Event,
			To:    final,
		}
		edges = append(edges, edge)
		final = final.Prev
	}
	reversePath(edges)
	return final, edges
}

func reversePath(path []StateEdge) {
	for i, j := 0, len(path)-1; i < len(path)/2; i, j = i+1, j-1 {
		path[i], path[j] = path[j], path[i]
	}
}

func PrintPath(path []StateEdge) {
	for _, edge := range path {
		instanceClass := reflect.TypeOf(edge.Event.Instance)
		fmt.Printf("Event: %s %s %v\n", edge.Event.Action, instanceClass, edge.Event.Instance)
	}
}
