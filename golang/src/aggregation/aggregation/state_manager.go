package aggregation

import (
	"sync"
)

type QueryState struct {
	LocalCount  int
	PeersCount  int
	TargetCount int
	EOFcount    int
}

type StateManager struct {
	clientStates   map[string]*QueryState
	TargetEOFCount int
	mu             sync.RWMutex
}

func NewStateManager(targetEOFCount int) *StateManager {
	return &StateManager{
		clientStates:   make(map[string]*QueryState),
		TargetEOFCount: targetEOFCount,
	}
}

func (manager *StateManager) AddLocalCount(clientId string, count int) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	state, ok := manager.clientStates[clientId]
	if !ok {
		state = &QueryState{}
		manager.clientStates[clientId] = state
	}
	state.LocalCount += count
}

func (manager *StateManager) MarkEOF(clientId string, total_count int) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	state, ok := manager.clientStates[clientId]
	if !ok {
		state = &QueryState{}
		manager.clientStates[clientId] = state
	}
	state.EOFcount++
	state.TargetCount = total_count
}

func (manager *StateManager) DoneWaiting(clientId string) bool {
	manager.mu.RLock()
	defer manager.mu.RUnlock()

	state, ok := manager.clientStates[clientId]
	if !ok {
		return false
	}
	return state.EOFcount == manager.TargetEOFCount
}

func (manager *StateManager) DoneReceiving(clientId string) bool {
	manager.mu.RLock()
	defer manager.mu.RUnlock()

	state, ok := manager.clientStates[clientId]
	if !ok {
		return false
	}
	return (state.LocalCount+state.PeersCount) == state.TargetCount && state.EOFcount == manager.TargetEOFCount
}

func (manager *StateManager) GetReceivedCount(clientId string) int {
	manager.mu.RLock()
	defer manager.mu.RUnlock()

	state, ok := manager.clientStates[clientId]
	if !ok {
		return 0
	}
	return state.LocalCount
}

func (manager *StateManager) AddPeersCount(clientId string, count int) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	state, ok := manager.clientStates[clientId]
	if !ok {
		state = &QueryState{}
		manager.clientStates[clientId] = state
	}
	state.PeersCount += count
}
