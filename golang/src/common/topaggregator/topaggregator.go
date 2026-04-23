package topaggregator

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruitmap"
)

// Clase auxiliar para evitar logica repetida en Join y Aggregation,
// Encargada de mantener los mapas de frutas y los EOFs por cliente.
type TopAggregator struct {
	fruitMaps    map[string]*fruitmap.FruitMap
	clientsEOF   map[string]int
	expectedEOFs int
	topSize      int
}

func NewTopAggregator(expectedEOFs int, topSize int) *TopAggregator {
	return &TopAggregator{
		fruitMaps:    map[string]*fruitmap.FruitMap{},
		clientsEOF:   map[string]int{},
		expectedEOFs: expectedEOFs,
		topSize:      topSize,
	}
}

// Add agrega registros para un cliente.
func (ta *TopAggregator) Add(clientId string, records []fruititem.FruitItem) {
	fruitMap, ok := ta.fruitMaps[clientId]
	if !ok {
		fruitMap = fruitmap.NewFruitMap()
		ta.fruitMaps[clientId] = fruitMap
	}
	fruitMap.Add(records)
}

// RegisterEOF incrementa el contador de EOFs para un cliente.
// Devuelve el top si se alcanzó el total esperado, nil en caso contrario.
func (ta *TopAggregator) RegisterEOF(clientId string) []fruititem.FruitItem {
	ta.clientsEOF[clientId]++
	if ta.clientsEOF[clientId] < ta.expectedEOFs {
		return nil
	}
	fruitMap, ok := ta.fruitMaps[clientId]
	if !ok {
		return []fruititem.FruitItem{}
	}
	return fruitMap.Top(ta.topSize)
}

// Clean libera la memoria del cliente.
func (ta *TopAggregator) Clean(clientId string) {
	delete(ta.fruitMaps, clientId)
	delete(ta.clientsEOF, clientId)
}
