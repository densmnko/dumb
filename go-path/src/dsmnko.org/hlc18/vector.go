package hlc18

type VectorUint32 struct {
	blockSize, len int
	blocks         [][]uint32
}

func makeVector(block int) (vect *VectorUint32) {
	return &VectorUint32{
		blockSize: block,
		len:       0,
		blocks:    make([][]uint32, 0)}
}

func makeInitialVector(block int, value uint32) (vect *VectorUint32) {
	vector := &VectorUint32{
		blockSize: block,
		len:       0,
		blocks:    make([][]uint32, 0)}
	vector.Push(value)
	return vector
}

func (p *VectorUint32) Push(value uint32) int {
	b := p.len / p.blockSize
	if b >= len(p.blocks) {
		// expand, copy, swap
		tmp := make([][]uint32, b+1)
		tmp[b] = make([]uint32, p.blockSize)
		copy(tmp, p.blocks)
		p.blocks = tmp
	}
	p.blocks[b][p.len%p.blockSize] = value
	p.len++
	return p.len - 1
}

func (p *VectorUint32) Get(index int) uint32 {
	return p.blocks[index/p.blockSize][index%p.blockSize]
}

// возвращаем предыдущее знаение
func (p *VectorUint32) Len() int {
	return p.len
}

// iter func(i int,v uint32) bool - return true for continue, false to exit Iteration

/**
Iterate() returns last index iterated
*/
func (p *VectorUint32) Iterate(iter func(i int, v uint32) bool) int {
	var index = 0
	for ib := 0; ib < len(p.blocks) && index < p.len; ib++ {
		for ii := 0; ii < p.blockSize && index < p.len; ii++ {
			index = (ib * p.blockSize) + ii
			if index < p.len {
				if !iter(index, p.blocks[ib][ii]) {
					return index
				}
			}
		}
	}
	return index
}

func (p *VectorUint32) CopyTo(out []uint32) {
	for i, b := range p.blocks {
		if i+1 < len(p.blocks) {
			copy(out[i*p.blockSize:], b)
		} else {
			copy(out[i*p.blockSize:], b[:(p.len-1)%p.blockSize+1])
		}
	}
}
