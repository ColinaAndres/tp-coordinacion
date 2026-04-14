package accumulator

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

type Accumulator struct {
	clientMaps map[string]map[string]fruititem.FruitItem
}

func NewAccumulator() *Accumulator {
	return &Accumulator{
		clientMaps: map[string]map[string]fruititem.FruitItem{},
	}
}

// Agrega los registros de un cliente al acumulador,
// sumando los registros de frutas repetidos, si no existia la
// entrada la crea.
func (accumulator *Accumulator) AddFruitItems(clientId string, fruitRecords []fruititem.FruitItem) error {
	if _, ok := accumulator.clientMaps[clientId]; !ok {
		accumulator.clientMaps[clientId] = map[string]fruititem.FruitItem{}
	}

	clientMap := accumulator.clientMaps[clientId]

	for _, fruitRecord := range fruitRecords {
		_, ok := clientMap[fruitRecord.Fruit]
		if ok {
			clientMap[fruitRecord.Fruit] = clientMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			clientMap[fruitRecord.Fruit] = fruitRecord
		}
	}
	return nil
}

// Elimina los registros de un cliente y devuelve la lista de items eliminados.
// Devuelve false si el cliente no existía
func (accumulator *Accumulator) RemoveClientFruitItems(clientId string) ([]fruititem.FruitItem, bool) {
	clientMap, ok := accumulator.clientMaps[clientId]
	if !ok {
		return nil, false
	}

	fruitItems := make([]fruititem.FruitItem, 0, len(clientMap))
	for _, item := range clientMap {
		fruitItems = append(fruitItems, item)
	}

	delete(accumulator.clientMaps, clientId)
	return fruitItems, true
}
