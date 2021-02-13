package paxos

import (
	"coms4113/hw5/pkg/base"
	"fmt"
	"testing"
)

func globalAgreedValue(state *base.State) (int, interface{}) {
	var v interface{}

	for _, node := range state.Nodes() {
		server := node.(*Server)

		if !base.IsNil(server.agreedValue) {
			if base.IsNil(v) {
				v = server.agreedValue
			} else if v != server.agreedValue {
				return -1, nil
			}
		}
	}

	if v == nil {
		return 0, nil
	}

	return 1, v
}

func validate(state *base.State) bool {
	flag, v := globalAgreedValue(state)

	if flag == -1 {
		return false
	}

	old := state.Prev

	if old != nil {
		_, oldV := globalAgreedValue(old)

		if !base.IsNil(oldV) && oldV != v {
			return false
		}
	}

	return true
}

func goal(state *base.State) bool {
	for _, node := range state.Nodes() {
		server := node.(*Server)
		if !base.IsNil(server.agreedValue) {
			return true
		}
	}

	return false
}

func TestBasic(t *testing.T) {
	peers := []base.Address{"s1", "s2", "s3"}
	s := base.NewState(0, false, false)

	server := NewServer(peers, 0, "v1")
	s.AddNode(peers[0], server, nil)

	server = NewServer(peers, 1, nil)
	s.AddNode(peers[1], server, nil)

	server = NewServer(peers, 2, nil)
	s.AddNode(peers[2], server, nil)

	result := base.RandomWalkFind(s, validate, goal, 10000)

	if result.Success {
		_, path := base.FindPath(result.Targets[0])
		base.PrintPath(path)
	}
}

func TestBasic2(t *testing.T) {
	peers := []base.Address{"s1", "s2", "s3"}

	s := base.NewState(0, false, false)

	server := NewServer(peers, 0, "v1")
	s.AddNode(peers[0], server, nil)

	server = NewServer(peers, 1, "v2")
	s.AddNode(peers[1], server, nil)

	server = NewServer(peers, 2, nil)
	s.AddNode(peers[2], server, nil)

	result := base.RandomWalkFind(s, validate, goal, 10000)

	if result.Success {
		_, path := base.FindPath(result.Targets[0])
		base.PrintPath(path)
	}
}

func TestBfs1(t *testing.T) {
	peers := []base.Address{"s1", "s2", "s3"}

	s := base.NewState(0, false, false)

	server := NewServer(peers, 0, "v1")
	s.AddNode(peers[0], server, nil)

	server = NewServer(peers, 1, nil)
	s.AddNode(peers[1], server, nil)

	server = NewServer(peers, 2, nil)
	s.AddNode(peers[2], server, nil)

	result := base.BfsFind(s, validate, goal, 10)

	if !result.Success {
		t.Fail()
		return
	}

	_, path := base.FindPath(result.Targets[0])
	base.PrintPath(path)
}

func TestBfs2(t *testing.T) {
	peers := []base.Address{"s1", "s2", "s3"}

	s := base.NewState(0, false, false)

	server := NewServer(peers, 0, "v1")
	server.proposer.N = 1
	server.proposer.V = "v1"
	server.proposer.InitialValue = "v1"
	server.proposer.Responses = []bool{true, false, false}
	server.proposer.SuccessCount = 1
	server.proposer.ResponseCount = 1
	server.proposer.Phase = Accept
	server.n_p = 1
	server.n_a = 1
	server.v_a = "v1"
	s.AddNode(peers[0], server, nil)

	server = NewServer(peers, 1, nil)
	server.n_p = 1
	s.AddNode(peers[1], server, nil)

	server = NewServer(peers, 2, nil)
	server.n_p = 1
	s.AddNode(peers[2], server, nil)

	s.Network = append(s.Network, &AcceptRequest{
		CoreMessage: base.MakeCoreMessage("s1", "s2"),
		N:           1,
		V:           "v1",
		SessionId:   0,
	})

	s.Network = append(s.Network, &AcceptRequest{
		CoreMessage: base.MakeCoreMessage("s1", "s3"),
		N:           1,
		V:           "v1",
		SessionId:   0,
	})

	result := base.BfsFind(s, validate, goal, 5)

	if !result.Success {
		t.Fail()
		return
	}

	_, path := base.FindPath(result.Targets[0])
	base.PrintPath(path)
}

func TestBfs3(t *testing.T) {
	peers := []base.Address{"s1", "s2", "s3"}

	s := base.NewState(0, false, false)

	server := NewServer(peers, 0, "v1")
	server.proposer.N = 1
	server.proposer.V = "v1"
	server.proposer.InitialValue = "v1"
	server.proposer.Responses = []bool{true, false, false}
	server.proposer.SuccessCount = 1
	server.proposer.ResponseCount = 1
	server.proposer.Phase = Accept
	server.n_p = 1
	server.n_a = 1
	server.v_a = "v1"
	s.AddNode(peers[0], server, nil)

	server = NewServer(peers, 1, nil)
	server.n_p = 1
	s.AddNode(peers[1], server, nil)

	server = NewServer(peers, 2, nil)
	server.n_p = 1
	s.AddNode(peers[2], server, nil)

	s.Network = append(s.Network, &AcceptRequest{
		CoreMessage: base.MakeCoreMessage("s1", "s2"),
		N:           1,
		V:           "v1",
		SessionId:   0,
	})

	s.Network = append(s.Network, &AcceptRequest{
		CoreMessage: base.MakeCoreMessage("s1", "s3"),
		N:           1,
		V:           "v1",
		SessionId:   0,
	})

	result := base.BfsFindAll(s, validate, goal, 8)

	if !result.Success {
		t.Fail()
		return
	}

	fmt.Printf("Number of Explored States: %d\n", result.N)
	fmt.Printf("Number of Success State: %d\n", len(result.Targets))

	maxDepth := 0
	var target *base.State

	// Find the goal state whose n_a is largest among the goal states
	for _, state := range result.Targets {
		if state.Depth > maxDepth {
			maxDepth = state.Depth
			target = state
		}
	}

	if target == nil {
		t.Fail()
		return
	}

	fmt.Printf("Depth of Targets State: %d\n", target.Depth)
	_, path := base.FindPath(target)
	base.PrintPath(path)
}

func consensusState() *base.State {
	peers := []base.Address{"s1", "s2", "s3"}

	s := base.NewState(0, true, true)

	server := NewServer(peers, 0, "v1")
	s.AddNode(peers[0], server, nil)

	server = NewServer(peers, 1, nil)
	server.n_a = 1
	server.v_a = "v3"
	s.AddNode(peers[1], server, nil)

	server = NewServer(peers, 2, nil)
	server.n_a = 1
	server.v_a = "v3"
	s.AddNode(peers[2], server, nil)
	return s
}

func invariantValidate(state *base.State) bool {
	flag, v := globalAgreedValue(state)
	if flag == -1 {
		return false
	}
	if flag == 1 && v != "v3" {
		return false
	}

	for _, node := range state.Nodes() {
		server := node.(*Server)
		if server.v_a != nil && server.v_a != "v3" {
			_, path := base.FindPath(state)
			base.PrintPath(path)
			return false
		}
	}

	return true
}

// Once it reaches consensus, it should never change.
func TestInvariant(t *testing.T) {
	s := consensusState()
	result := base.BfsFindAll(s, invariantValidate, nil, 7)
	if !result.Success {
		t.Fatal("consensus has been broken")
	}
	fmt.Printf("Number of Explored States: %d\n", result.N)

	s = consensusState()
	result = base.BatchRandomWalkValidate(s, invariantValidate, nil, 100, 5000)
	if !result.Success {
		t.Fatal("consensus has been broken")
	}
}

func TestPartition1(t *testing.T) {
	peers := []base.Address{"s1", "s2", "s3"}

	s := base.NewState(0, false, false)

	server := NewServer(peers, 0, nil)
	server.n_p = 1
	server.n_a = 1
	server.v_a = "v1"
	s.AddNode(peers[0], server, []base.Address{"s3"})

	server = NewServer(peers, 1, nil)
	server.n_p = 1
	s.AddNode(peers[1], server, nil)

	server = NewServer(peers, 2, "v3")
	server.n_p = 2
	server.proposer.Phase = Propose
	server.proposer.N = 2
	server.proposer.V = "v3"
	server.proposer.InitialValue = "v3"
	server.proposer.SuccessCount = 1
	server.proposer.ResponseCount = 1
	server.proposer.Responses = []bool{false, false, true}
	s.AddNode(peers[2], server, []base.Address{"s1"})

	s.Network = append(s.Network, &ProposeRequest{
		CoreMessage: base.MakeCoreMessage("s3", "s2"),
		N:           2,
		SessionId:   0,
	})

	s.Network = append(s.Network, &ProposeRequest{
		CoreMessage: base.MakeCoreMessage("s3", "s1"),
		N:           2,
		SessionId:   0,
	})

	goal := func(state *base.State) bool {
		s1 := state.Nodes()["s1"].(*Server)
		s2 := state.Nodes()["s2"].(*Server)
		s3 := state.Nodes()["s3"].(*Server)

		return state.Depth >= 8 && s3.agreedValue == "v3" &&
			s1.v_a == "v1" && s2.v_a == "v3" && s3.v_a == "v3"
	}

	result := base.BfsFind(s, validate, goal, 8)

	if !result.Success {
		t.Fail()
		return
	}
	fmt.Printf("Number of Explored States: %d\n", result.N)
	fmt.Printf("Depth of Targets State: %d\n", result.Targets[0].Depth)

	_, path := base.FindPath(result.Targets[0])
	base.PrintPath(path)
}

func reachState(start *base.State,
	checks []func(s *base.State) bool,
	depthLimit int) *base.State {

	s := start
	for _, check := range checks {

		res := base.BfsFind(s, validate, check, s.Depth+depthLimit)
		if !res.Success {
			return nil
		}
		s = res.Targets[0]
	}

	return s
}

// Set up a few intermediate state predicates to lead the initial state to the final state
// where S1 reaches consensus first and then S3 reaches consensus on the same value.
func checksForPartition2() []func(s *base.State) bool {
	p1PreparePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose
	}

	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept
	}

	s1KnowConsensus := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.agreedValue == "v1"
	}

	p3PreparePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose
	}

	p3PrepareAgain := func(s *base.State) bool {
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose && s3.proposer.N > s2.n_p
	}

	p3AcceptPhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Accept
	}

	s3KnowConsensus := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.agreedValue == "v1"
	}

	return []func(s *base.State) bool{p1PreparePhase, p1AcceptPhase, s1KnowConsensus,
		p3PreparePhase, p3PrepareAgain, p3AcceptPhase, s3KnowConsensus}
}

// S1 and S3 cannot communicate among each other. However, S1 and S3 should be able to agree on the same value.
// This test requires S1 firsts reach consensus on v1, and then S3 also learns that v1 is the consensus value.
func TestPartition2(t *testing.T) {
	peers := []base.Address{"s1", "s2", "s3"}

	s := base.NewState(0, false, false)

	server := NewServer(peers, 0, "v1")
	s.AddNode(peers[0], server, []base.Address{"s3"})

	server = NewServer(peers, 1, nil)
	s.AddNode(peers[1], server, nil)

	server = NewServer(peers, 2, "v3")
	s.AddNode(peers[2], server, []base.Address{"s1"})

	s = reachState(s, checksForPartition2(), 5)
	if s == nil {
		t.Fatal("cannot find a path where S1 reaches consensus first and S3 agrees on the same value, while S1 and" +
			"S3 are isolated")
	}
}

// This check functions requires the program: (1) A2 rejects P1 and (2) S3 is the first server to know a consensus
// is reached.
func checksForCase5Failures() []func(s *base.State) bool {
	checks := ToA2RejectP1()

	checks = append(checks, func(s *base.State) bool {
		event := s.Event
		if event.Action != base.Handle && event.Action != base.HandleDuplicate {
			return false
		}
		m, ok := event.Instance.(*AcceptResponse)
		return ok && !m.Ok && m.From() == "s2" && m.To() == "s1"
	})

	checks = append(checks, ToConsensusCase5()...)

	checks = append(checks, func(s *base.State) bool {
		for addr, node := range s.Nodes() {
			server := node.(*Server)
			if addr == "s3" && server.agreedValue != "v3" {
				return false
			}

			if addr != "s3" && server.agreedValue != nil {
				return false
			}
		}
		return true
	})
	return checks
}

// https://columbia.github.io/ds1-class/lectures/07-paxos-functioning-slides.pdf
// 5. With Failures. P2 in the slide becomes P3 here.
func TestCase5Failures(t *testing.T) {
	peers := []base.Address{"s1", "s2", "s3"}

	s := base.NewState(0, false, false)

	server := NewServer(peers, 0, "v1")
	s.AddNode(peers[0], server, []base.Address{"s3"})

	server = NewServer(peers, 1, nil)
	s.AddNode(peers[1], server, nil)

	server = NewServer(peers, 2, "v3")
	s.AddNode(peers[2], server, []base.Address{"s1"})

	s = reachState(s, checksForCase5Failures(), 5)
	if s == nil {
		t.Fatal("cannot find a path where an acceptor rejects a Accept Request and then the system " +
			"reaches consensus")
	}
}

func noConsensus(s *base.State) bool {
	for _, node := range s.Nodes() {
		server := node.(*Server)
		if server.agreedValue != nil {
			return false
		}
	}

	return true
}

func s1GetAllAcceptRejects(s *base.State) bool {
	if !noConsensus(s) {
		return false
	}
	server := s.Nodes()["s1"].(*Server)
	return server.proposer.Phase == Accept && server.proposer.ResponseCount == 3 && server.proposer.SuccessCount == 0
}

func s3GetAllAcceptRejects(s *base.State) bool {
	if !noConsensus(s) {
		return false
	}
	server := s.Nodes()["s3"].(*Server)
	return server.proposer.Phase == Accept && server.proposer.ResponseCount == 3 && server.proposer.SuccessCount == 0
}

// Guide the program to (1) reject all the Accept requests from S1, (2) reject all the Accept requests from S3, and
// (3) reject all the Accept requests from S1 again.
func checksForNotTerminate() []func(s *base.State) bool {
	checks := NotTerminate1()

	checks = append(checks, s1GetAllAcceptRejects)

	checks = append(checks, NotTerminate2()...)

	checks = append(checks, s3GetAllAcceptRejects)

	checks = append(checks, NotTerminate3()...)

	checks = append(checks, s1GetAllAcceptRejects)
	return checks
}

// https://columbia.github.io/ds1-class/lectures/07-paxos-functioning-slides.pdf
// last slide. P2 in the slides becomes P3 here.
func TestNotTerminate(t *testing.T) {
	peers := []base.Address{"s1", "s2", "s3"}

	s := base.NewState(0, false, false)

	server := NewServer(peers, 0, "v1")
	s.AddNode(peers[0], server, nil)

	server = NewServer(peers, 1, nil)
	s.AddNode(peers[1], server, nil)

	server = NewServer(peers, 2, "v3")
	s.AddNode(peers[2], server, nil)

	s = reachState(s, checksForNotTerminate(), 5)

	if s == nil {
		t.Fatal("cannot find a path where consensus not reached")
	}
}

func concurrentProposerChecks() []func(s *base.State) bool {
	p1GetsAllRejects := func(s *base.State) bool {
		if !noConsensus(s) {
			return false
		}
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Accept && s1.proposer.ResponseCount == 3 && s1.proposer.SuccessCount == 0
		if valid {
			fmt.Println("... p1 received rejects from all peers during Accept phase")
		}
		return valid
	}

	allKnowConsensus := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := s1.agreedValue == "v3" && s2.agreedValue == "v3" && s3.agreedValue == "v3"
		if valid {
			fmt.Println("... all peers agreed on v3")
		}
		return valid
	}

	checks := concurrentProposer1()
	checks = append(checks, p1GetsAllRejects)
	checks = append(checks, concurrentProposer2()...)
	checks = append(checks, allKnowConsensus)
	return checks
}

// https://docs.google.com/presentation/d/1ESICVkGl0zNY-95bTCGoJhbcYeiKGUQAuepUaITvJhg/edit#slide=id.g9f0e2b3fae_0_180
func TestConcurrentProposer(t *testing.T) {
	fmt.Printf("Test: Concurrent proposers...\n")
	peers := []base.Address{"s1", "s2", "s3"}
	s := base.NewState(0, false, false)
	server := NewServer(peers, 0, "v1")
	s.AddNode(peers[0], server, nil)

	server = NewServer(peers, 1, nil)
	s.AddNode(peers[1], server, nil)

	server = NewServer(peers, 2, "v3")
	s.AddNode(peers[2], server, nil)

	if nil == reachState(s, concurrentProposerChecks(), 5) {
		t.Fatal("cannot find a path where we reach consensus on v3")
	}
	fmt.Printf("... Passed\n")
}

// https://docs.google.com/presentation/d/1ESICVkGl0zNY-95bTCGoJhbcYeiKGUQAuepUaITvJhg/edit#slide=id.g9f109140a1_1_8
// Reference code for implementing predicates, not graded.
func TestFailChecks(t *testing.T) {
	fmt.Printf("Test: Decides fail ...\n")
	peers := []base.Address{"s1", "s2", "s3"}
	s := base.NewState(0, false, false)
	server := NewServer(peers, 0, "v1")
	s.AddNode(peers[0], server, nil)

	server = NewServer(peers, 1, nil)
	s.AddNode(peers[1], server, nil)

	server = NewServer(peers, 2, "v2")
	s.AddNode(peers[2], server, nil)

	if nil == reachState(s, decidesFailChecks(), 5) {
		t.Fatal("cannot find a path where we reach consensus on v1")
	}
	fmt.Printf("... Passed\n")
}

func decidesFailChecks() []func(s *base.State) bool {
	p1PreparePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Propose
		if valid {
			fmt.Println("... p1 entered Propose phase")
		}
		return valid
	}
	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := false
		otheroks := 0
		if s1.proposer.Phase == Accept && s1.proposer.V == "v1" {
			for _, m := range s.Network {
				resp, ok := m.(*ProposeResponse)
				if ok && resp.Ok && m.To() == s1.Address() {
					otheroks++
				}
			}
			if otheroks == 1 {
				valid = true
			}
		}
		if valid {
			fmt.Println("... p1 entered Accept phase with proposed value v1")
		}
		return valid
	}
	p1DecidePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := false
		otheroks := 0
		if s1.proposer.Phase == Decide && s1.proposer.V == "v1" {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && resp.Ok && m.To() == s1.Address() {
					otheroks++
				}
			}
			if otheroks == 1 {
				valid = true
			}
		}
		if valid {
			fmt.Println("... p1 entered Decide phase with proposed value v1")
		}
		return valid
	}
	p3PreparePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Propose && s3.proposer.V == "v2"
		if valid {
			fmt.Println("... p3 entered Propose phase with proposed value v2")
		}
		return valid
	}
	p3AcceptPhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		valid := false
		otheroks := 0
		if s3.proposer.Phase == Accept && s3.proposer.V == "v1" {
			for _, m := range s.Network {
				resp, ok := m.(*ProposeResponse)
				if ok && resp.Ok && m.To() == s3.Address() {
					otheroks++
				}
			}
			if otheroks == 1 {
				valid = true
			}
		}
		if valid {
			fmt.Println("... p3 entered Accept phase with proposed value v1")
		}
		return valid
	}
	p3DecidePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		valid := false
		otheroks := 0
		if s3.proposer.Phase == Decide && s3.proposer.V == "v1" {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && resp.Ok && m.To() == s3.Address() {
					otheroks++
				}
			}
			if otheroks == 1 {
				valid = true
			}
		}
		if valid {
			fmt.Println("... p3 entered Decide phase with proposed value v1")
		}
		return valid
	}
	allKnowConsensus := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := s1.agreedValue == "v1" && s2.agreedValue == "v1" && s3.agreedValue == "v1"
		if valid {
			fmt.Println("... all peers agreed on v1")
		}
		return valid
	}
	return []func(s *base.State) bool{
		p1PreparePhase,
		p1AcceptPhase,
		p1DecidePhase,
		p3PreparePhase,
		p3AcceptPhase,
		p3DecidePhase,
		allKnowConsensus}
}
