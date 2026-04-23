package accumulator

import (
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

type Accumulator struct {
	clients map[string]*clientState
	mutex   sync.Mutex
}

func NewAccumulator() *Accumulator {
	return &Accumulator{
		clients: map[string]*clientState{},
		mutex:   sync.Mutex{},
	}
}

// Crea un nuevo estado para el cliente si no existe o devuelve el estado existente.
func (accumulator *Accumulator) getClientState(clientId string) *clientState {
	if state, ok := accumulator.clients[clientId]; ok {
		return state
	}
	state := newClientState()
	accumulator.clients[clientId] = state
	return state
}

// Agrega los registros de un cliente al acumulador,
// sumando los registros de frutas repetidos, si no existia la entrada la crea pero.
// Devuelve:
// - closing: true si el cliente ya envió EOF, false en caso contrario
// - newCount: cantidad de registros agregados al acumulador
// - itemsToFlush: lista de items a enviar al peer, si el cliente ya envió EOF y se alcanzó el total esperado, nil en caso contrario
// Metodo thread safe
func (accumulator *Accumulator) AddFruitItems(clientId string, fruitItems []fruititem.FruitItem) (closing bool, newCount int, itemsToFlush []fruititem.FruitItem) {
	accumulator.mutex.Lock()
	defer accumulator.mutex.Unlock()

	state := accumulator.getClientState(clientId)
	newCount = len(fruitItems)
	state.ownCount += newCount
	state.addItems(fruitItems)
	closing = state.isClosing()

	if state.isReadyToFlush() {
		itemsToFlush = state.takeItems()
	}

	return closing, newCount, itemsToFlush
}

// Marca que el cliente ya envio EOF, y devuelve la cantidad de registros que se analizaron para el cliente,
// y los items a enviar al aggregador si se alcanzó el total esperado
func (accumulator *Accumulator) MarkClientAsDone(clientId string, totalExpected int) (ownCount int, itemsToFlush []fruititem.FruitItem) {
	accumulator.mutex.Lock()
	defer accumulator.mutex.Unlock()

	state := accumulator.getClientState(clientId)
	state.totalExpected = totalExpected
	ownCount = state.ownCount

	if state.isReadyToFlush() {
		itemsToFlush = state.takeItems()
	}

	return ownCount, itemsToFlush

}

// Suma a el conteo de peers para un cliente,
// y devuelve los items a enviar al aggregador si se alcanzó el total esperado
func (accumulator *Accumulator) AddPeerCount(clientId string, count int) []fruititem.FruitItem {
	accumulator.mutex.Lock()
	defer accumulator.mutex.Unlock()

	state := accumulator.getClientState(clientId)
	state.peerCount += count
	if state.isReadyToFlush() {
		return state.takeItems()
	}
	return nil
}

// Limpia la lista de clientes que ya enviaron EOF, se puede usar para liberar memoria
// si se sabe que no van a volver a enviar registros
// Metodo thread safe
func (accumulator *Accumulator) CleanClient(clientId string) {
	accumulator.mutex.Lock()
	defer accumulator.mutex.Unlock()
	delete(accumulator.clients, clientId)
}
