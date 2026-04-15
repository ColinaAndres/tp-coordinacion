package accumulator

import (
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

type Accumulator struct {
	clientMaps  map[string]map[string]fruititem.FruitItem
	doneClients map[string]bool
	mutex       sync.RWMutex
}

func NewAccumulator() *Accumulator {
	return &Accumulator{
		clientMaps:  map[string]map[string]fruititem.FruitItem{},
		doneClients: map[string]bool{},
		mutex:       sync.RWMutex{},
	}
}

// Agrega los registros de un cliente al acumulador,
// sumando los registros de frutas repetidos, si no existia la entrada la crea pero si
// el cliente ya habia enviado un EOF, no se agregan los registros y se devuelve false.
// Metodo thread safe
func (accumulator *Accumulator) AddFruitItems(clientId string, fruitRecords []fruititem.FruitItem) bool {
	accumulator.mutex.Lock()
	defer accumulator.mutex.Unlock()

	if accumulator.doneClients[clientId] {
		return false
	}

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
	return true
}

// Elimina los registros de un cliente y devuelve la lista de items eliminados.
// Devuelve false si el cliente no existía
// Metodo thread safe
func (accumulator *Accumulator) RemoveClientFruitItems(clientId string) ([]fruititem.FruitItem, bool) {
	accumulator.mutex.Lock()
	defer accumulator.mutex.Unlock()

	clientMap, ok := accumulator.clientMaps[clientId]
	if !ok {
		return nil, false
	}

	fruitItems := make([]fruititem.FruitItem, 0, len(clientMap))
	for _, item := range clientMap {
		fruitItems = append(fruitItems, item)
	}

	delete(accumulator.clientMaps, clientId)
	accumulator.doneClients[clientId] = true

	return fruitItems, true
}

// Devuelve la lista de items acumulados para un cliente sin eliminar los registros del acumulador.
// Devuelve false si el cliente no existía.
// Metodo thread safe, se pueden realizar lecturas concurrentes.
func (accumulator *Accumulator) GetClientFruitItems(clientId string) ([]fruititem.FruitItem, bool) {
	accumulator.mutex.RLock()
	defer accumulator.mutex.RUnlock()

	clientMap, ok := accumulator.clientMaps[clientId]
	if !ok {
		return nil, false
	}

	fruitItems := make([]fruititem.FruitItem, 0, len(clientMap))
	for _, item := range clientMap {
		fruitItems = append(fruitItems, item)
	}

	return fruitItems, true
}

// Limpia la lista de clientes que ya enviaron EOF, se puede usar para liberar memoria
// si se sabe que no van a volver a enviar registros
// Metodo thread safe
func (accumulator *Accumulator) CleanDoneClient(clientId string) {
	accumulator.mutex.Lock()
	defer accumulator.mutex.Unlock()
	delete(accumulator.doneClients, clientId)
}
