package hlc18

type Bitset256 [4]uint64
type Bitset128 [2]uint64

func (p *Bitset256) Set(bit byte) {
	p[bit/64] = p[bit/64] | (1 << (bit % 64))
}

func (p *Bitset256) Get(bit byte) bool {
	return 1 == ((p[bit/64] >> (bit % 64)) & 0x01)
}

func (p *Bitset256) Contains(p2 *Bitset256) bool {
	return (p[0]&p2[0] == p2[0]) &&
		(p[1]&p2[1] == p2[1]) &&
		(p[2]&p2[2] == p2[2]) &&
		(p[3]&p2[3] == p2[3])
}

func (p *Bitset256) Any(p2 *Bitset256) bool {
	return (p[0]&p2[0] != 0) ||
		(p[1]&p2[1] != 0) ||
		(p[2]&p2[2] != 0) ||
		(p[3]&p2[3] != 0)
}

func (p *Bitset256) IsNotEmpty() bool {
	return p[0] != 0 || p[1] != 0 || p[2] != 0 || p[3] != 0
}

func (p *Bitset256) IsEmpty() bool {
	return !p.IsNotEmpty()
}

func (p *Bitset128) Set(bit byte) {
	p[bit/64] = p[bit/64] | (1 << (bit % 64))
}

func (p *Bitset128) Get(bit byte) bool {
	return 1 == ((p[bit/64] >> (bit % 64)) & 0x01)
}

func (p *Bitset128) Contains(p2 *Bitset128) bool {
	return (p[0]&p2[0] == p2[0]) && (p[1]&p2[1] == p2[1])
}

func (p *Bitset128) Any(p2 *Bitset128) bool {
	return (p[0]&p2[0] != 0) || (p[1]&p2[1] != 0)
}

func (p *Bitset128) IsNotEmpty() bool {
	return p[0] != 0 || p[1] != 0
}

func (p *Bitset128) IsEmpty() bool {
	return !p.IsNotEmpty()
}

func (p *Bitset128) Count() (count int) {
	n := p[0]
	for n != 0 {
		count++
		n &= n - 1 // Zero the lowest-order one-bit
	}
	n = p[1]
	for n != 0 {
		count++
		n &= n - 1 // Zero the lowest-order one-bit
	}
	return count
}

func (p *Bitset128) Reset() {
	p[0] = 0
	p[1] = 0
}

func Bitset128Set(bit byte, t0 *uint64, t1 *uint64) {
	//p[bit / 64] = p[bit / 64] | (1 << (bit % 64))
	if 0 == bit/64 {
		*t0 = *t0 | (1 << (bit % 64))
	} else {
		*t1 = *t1 | (1 << (bit % 64))
	}

}

func Bitset128Get(bit byte, t0 *uint64, t1 *uint64) bool {
	if 0 == bit/64 {
		return 1 == ((*t0 >> (bit % 64)) & 0x01)
	} else {
		return 1 == ((*t1 >> (bit % 64)) & 0x01)
	}
}

func Bitset128Contains(t0 *uint64, t1 *uint64, p2 *Bitset128) bool {
	return (*t0&p2[0] == p2[0]) && (*t1&p2[1] == p2[1])
}

func Bitset128Any(t0 *uint64, t1 *uint64, p2 *Bitset128) bool {
	return (*t0&p2[0] != 0) || (*t1&p2[1] != 0)
}
