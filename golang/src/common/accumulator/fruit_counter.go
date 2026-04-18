package accumulator

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

type FruitCounter struct {
	fruitItem fruititem.FruitItem
	count     int
}

func NewFruitCounter(fruitItem fruititem.FruitItem) *FruitCounter {
	return &FruitCounter{fruitItem: fruitItem, count: 0}
}

func (counter *FruitCounter) AddFruitItem(fruitItem fruititem.FruitItem) {
	counter.fruitItem = counter.fruitItem.Sum(fruitItem)
	counter.count++
}
