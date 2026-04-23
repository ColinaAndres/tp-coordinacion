package fruitmap

import (
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

type FruitMap struct {
	fruitItems map[string]fruititem.FruitItem
}

func NewFruitMap() *FruitMap {
	return &FruitMap{
		fruitItems: map[string]fruititem.FruitItem{},
	}
}

func (fm *FruitMap) Add(fruitItems []fruititem.FruitItem) {
	for _, fruitItem := range fruitItems {
		_, ok := fm.fruitItems[fruitItem.Fruit]
		if ok {
			fm.fruitItems[fruitItem.Fruit] = fm.fruitItems[fruitItem.Fruit].Sum(fruitItem)
		} else {
			fm.fruitItems[fruitItem.Fruit] = fruitItem
		}
	}
}

func (fm *FruitMap) Take() []fruititem.FruitItem {
	items := make([]fruititem.FruitItem, 0, len(fm.fruitItems))
	for _, item := range fm.fruitItems {
		items = append(items, item)
	}
	fm.fruitItems = map[string]fruititem.FruitItem{}
	return items
}

func (fm *FruitMap) Top(n int) []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(fm.fruitItems))
	for _, item := range fm.fruitItems {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	if n > len(fruitItems) {
		n = len(fruitItems)
	}
	return fruitItems[:n]
}
