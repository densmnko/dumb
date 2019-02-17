package hlc18

import (
	"math"
	"testing"
)

func TestIndexOrIterator(t *testing.T) {

	iterator := makeIndexOrIterator()

	iterator.push([]uint32{1})
	iterator.push([]uint32{1})
	iterator.push([]uint32{})
	iterator.push([]uint32{})

	if 1 != iterator.Next() {
		t.Error("failed")
	}
	if math.MaxUint32 != iterator.Next() {
		t.Error("failed")
	}

	iterator = makeIndexOrIterator()
	iterator.push([]uint32{1, 3})
	iterator.push([]uint32{2, 4})
	iterator.push([]uint32{2, 3})
	iterator.push([]uint32{1, 2, 3, 4})

	if 4 != iterator.Next() {
		t.Error("failed")
	}
	if 3 != iterator.Next() {
		t.Error("failed")
	}
	if 2 != iterator.Next() {
		t.Error("failed")
	}
	if 1 != iterator.Next() {
		t.Error("failed")
	}
	if math.MaxUint32 != iterator.Next() {
		t.Error("failed")
	}

}

func TestIndexAndIterator(t *testing.T) {

	iterator := makeIndexAndIterator()
	iterator.push([]uint32{1})
	iterator.push([]uint32{1})
	iterator.Prepare()
	if 1 != iterator.Next() {
		t.Error("failed")
	}
	if math.MaxUint32 != iterator.Next() {
		t.Error("failed")
	}

	iterator = makeIndexAndIterator()
	iterator.push([]uint32{1})
	iterator.push([]uint32{})
	iterator.Prepare()
	if math.MaxUint32 != iterator.Next() {
		t.Error("failed")
	}

	iterator = makeIndexAndIterator()
	iterator.push([]uint32{1, 3})
	iterator.push([]uint32{2, 4})
	iterator.push([]uint32{2, 3})
	iterator.push([]uint32{1, 2, 3, 4})
	iterator.Prepare()
	if math.MaxUint32 != iterator.Next() {
		t.Error("failed")
	}

	iterator = makeIndexAndIterator()
	iterator.push([]uint32{1, 2})
	iterator.push([]uint32{2, 4})
	iterator.push([]uint32{2, 3})
	iterator.push([]uint32{1, 2, 3, 4})
	iterator.Prepare()
	if 2 != iterator.Next() {
		t.Error("failed")
	}
	if math.MaxUint32 != iterator.Next() {
		t.Error("failed")
	}
}
