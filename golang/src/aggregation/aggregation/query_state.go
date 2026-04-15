package aggregation

type QueryState struct {
	ReceivedCounts int
	TargetCounts   int
	EOFcount       int
}
