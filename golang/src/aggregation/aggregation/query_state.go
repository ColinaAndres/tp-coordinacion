package aggregation

type QueryState struct {
	ReceivedCount int
	TargetCounts  int
	EOFcount      int //Posible parametro no necesario
}
