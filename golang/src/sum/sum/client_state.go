package sum

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruitmap"
)

const (
	defaultTotalExpected = -1
)

type clientState struct {
	fruitItems    *fruitmap.FruitMap
	ownCount      int
	peerCount     int
	totalExpected int  // -1 = EOF aún no llegó
	flushed       bool //
}

func newClientState() *clientState {
	return &clientState{
		fruitItems:    fruitmap.NewFruitMap(),
		ownCount:      0,
		peerCount:     0,
		totalExpected: defaultTotalExpected,
		flushed:       false,
	}
}

func (cs *clientState) addItems(fruitItems []fruititem.FruitItem) {
	cs.fruitItems.Add(fruitItems)
}

func (cs *clientState) isReadyToFlush() bool {
	return !cs.flushed &&
		cs.totalExpected > defaultTotalExpected &&
		cs.ownCount+cs.peerCount == cs.totalExpected
}

func (cs *clientState) takeItems() []fruititem.FruitItem {
	items := cs.fruitItems.Take()
	cs.fruitItems = nil
	cs.flushed = true
	return items
}

func (cs *clientState) isClosing() bool {
	return cs.totalExpected > defaultTotalExpected
}
