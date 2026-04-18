package accumulator

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

type FruitCounter struct {
	FruitItem fruititem.FruitItem
	Count     int
}

func NewFruitCounter(fruitItem fruititem.FruitItem) *FruitCounter {
	return &FruitCounter{FruitItem: fruitItem, Count: 1}
}

func (counter *FruitCounter) AddFruitItem(fruitItem fruititem.FruitItem) {
	counter.FruitItem = counter.FruitItem.Sum(fruitItem)
	counter.Count++
}
