package accumulator

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

type clientState struct {
	fruitItems    map[string]fruititem.FruitItem
	ownCount      int
	peerCount     int
	totalExpected int  // -1 = EOF aún no llegó
	flushed       bool //
}

func newClientState() *clientState {
	return &clientState{
		fruitItems:    map[string]fruititem.FruitItem{},
		totalExpected: -1,
	}
}

func (cs *clientState) addItems(fruitItems []fruititem.FruitItem) {
	for _, fruitItem := range fruitItems {
		_, ok := cs.fruitItems[fruitItem.Fruit]
		if ok {
			cs.fruitItems[fruitItem.Fruit] = cs.fruitItems[fruitItem.Fruit].Sum(fruitItem)
		} else {
			cs.fruitItems[fruitItem.Fruit] = fruitItem
		}
	}
}

func (cs *clientState) isReadyToFlush() bool {
	return !cs.flushed &&
		cs.totalExpected >= 0 &&
		cs.ownCount+cs.peerCount == cs.totalExpected
}

func (s *clientState) takeItems() []fruititem.FruitItem {
	items := make([]fruititem.FruitItem, 0, len(s.fruitItems))
	for _, item := range s.fruitItems {
		items = append(items, item)
	}
	s.fruitItems = nil
	s.flushed = true
	return items
}
