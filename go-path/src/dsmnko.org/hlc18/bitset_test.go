package hlc18

import (
	"testing"
)

func TestBitSet(t *testing.T) {

	var bitset Bitset256

	if bitset.IsEmpty() != true {
		t.Errorf("IsEmpty failed, %v", bitset)
	}

	bitset.Set(0)
	bitset.Set(1)

	bitset.Set(127)
	bitset.Set(64)
	bitset.Set(63)

	if bitset.Get(0) != true {
		t.Errorf("0 failed, %v", bitset)
	}
	if bitset.Get(1) != true {
		t.Errorf("1 failed, %v", bitset)
	}
	if bitset.Get(63) != true {
		t.Errorf("63 failed, %v", bitset)
	}
	if bitset.Get(64) != true {
		t.Errorf("64 failed, %v", bitset)
	}
	if bitset.Get(127) != true {
		t.Errorf("127 failed, %v", bitset)
	}

	bitset.Set(127) // have to be idempotent
	if bitset.Get(127) != true {
		t.Errorf("127 failed, %v", bitset)
	}

	if bitset.Get(2) == true {
		t.Errorf("2 failed, %v", bitset)
	}
	if bitset.Get(255) == true {
		t.Errorf("255 failed, %v", bitset)
	}

	var bitset2 Bitset256

	bitset2.Set(0)
	bitset2.Set(1)
	bitset2.Set(64)
	bitset2.Set(63)

	if bitset.Contains(&bitset2) != true {
		t.Errorf("Contains failed, %v, %v", bitset, bitset2)
	}

	if bitset.Any(&bitset2) != true {
		t.Errorf("Any failed, %v, %v", bitset, bitset2)
	}

	bitset2.Set(42)

	if bitset.Any(&bitset2) != true {
		t.Errorf("Any (42) failed, %v, %v", bitset, bitset2)
	}
	if bitset.Contains(&bitset2) == true {
		t.Errorf("non Contains failed, %v, %v", bitset, bitset2)
	}

	var bitset3 Bitset256

	bitset3.Set(42)

	if bitset.Any(&bitset3) == true {
		t.Errorf("non Any failed, %v, %v", bitset, bitset3)
	}

}
