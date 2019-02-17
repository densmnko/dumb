package hlc18

import (
	"math/rand"
	"testing"
)

func TestVector(t *testing.T) {

	r := rand.New(rand.NewSource(99))

	vec := makeVector(8)

	var refSum int

	for i := 0; i < 642; i++ {
		v := r.Uint32()
		refSum += int(v)
		if i != vec.Push(v) {
			t.Errorf("push %d error %v", i, vec)
		}
	}

	var vecSum int

	sum := func(i int, v uint32) bool {
		vecSum += int(v)
		return true
	}

	vec.Iterate(sum)

	if vecSum != refSum {
		t.Errorf("summs diff")
	}

}
