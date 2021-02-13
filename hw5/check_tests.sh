cd pkg/pingpong

go test -run 'TestBasic'
go test -run 'TestBfsFind'
go test -run 'TestBfsFindAll1'
go test -run 'TestBfsFindAll2'
go test -run 'TestBfsFindAll2'
go test -run 'TestBfsFindAll3'
go test -run 'TestRandomWalkFindAll'
go test -run 'TestRandomWalkFind'

go test -run 'TestHashAndEqual'
go test -run 'TestStateInherit'
go test -run 'TestNextStates'
go test -run 'TestPartition'

cd ../paxos

go test -run 'TestUnit'

go test -run 'TestBasic'
go test -run 'TestBasic2'
go test -run 'TestBfs1'
go test -run 'TestBfs2'
go test -run 'TestBfs3'
go test -run 'TestInvariant'
go test -run 'TestPartition1'
go test -run 'TestPartition2'
go test -run 'TestCase5Failures'
go test -run 'TestNotTerminate'
go test -run 'TestConcurrentProposer'
# TestFailChecks not graded
go test -run 'TestFailChecks'


