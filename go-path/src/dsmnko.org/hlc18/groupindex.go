package hlc18

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"tidwall/evio"
	"unsafe"
)

const (
	BIRTH_YEARS  = 56
	JOINED_YEARS = 8
	STATUSES     = 3
)

var _aggregates []Aggregate

type Aggregate interface {
	Index(id uint32, account *Account)
	Aggregate(group *Group) (map[uint64]int32, bool)
	GroupKey(x int, y int, groups []bool) uint64
}

func capacityOf(name string) int {
	switch name {
	case "status":
		return STATUSES
	case "country":
		return len(CountryDict.values) + 1
	case "city":
		return len(CityDict.values) + 1
	case "sex":
		return 2
	case "joined":
		return JOINED_YEARS
	case "birth":
		return BIRTH_YEARS

	default:
		log.Fatal("capacityOf " + name)
		return -1
	}
}

func offsetOf(name string) uint {
	switch name {
	case "status":
		return STATUS_OFFSET
	case "country":
		return COUNTRY_OFFSET
	case "city":
		return CITY_OFFSET
	case "sex":
		return 0
	case "joined":
		return JOINED_Y_OFFSET
	case "birth":
		return BIRTH_Y_OFFSET

	default:
		log.Fatal("offsetOf " + name)
		return 0
	}
}

func filterValueOf(name string, group *Group) int {
	switch name {
	case "status":
		return int(group.Filter.status_eq) - 1
	case "country":
		return int(group.Filter.country_eq)
	case "city":
		return int(group.Filter.city_eq)
	case "sex":
		return int(group.Filter.sex_eq) - 1
	case "joined":
		return int(group.Filter.joined_year) - 2011
	case "birth":
		return int(group.Filter.birth_year) - 1950
	case "interests":
		return int(group.interestKeyCode) - 1
	default:
		log.Fatal("filterValueOf " + name)
		return -1
	}
}

func sexIndexFunc(a *Account) uint { return uint(a.getSex() - 1) }

func indexFuncOf(name string) func(*Account) uint {
	switch name {
	case "status":
		return func(a *Account) uint { return uint(a.getStatus() - 1) }
	case "country":
		return func(a *Account) uint { return uint(a.getCountry()) }
	case "city":
		return func(a *Account) uint { return uint(a.getCity()) }
	case "sex":
		return sexIndexFunc
	case "joined":
		return func(a *Account) uint { return uint(a.getJoinedYear() - 2011) }
	case "birth":
		return func(a *Account) uint { return uint(a.getBirthYear() - 1950) }
	default:
		log.Fatal("indexFuncOf " + name)
		return nil
	}
}

var rangeBackend []int

func RebuildAggregates(accounts *[]Account, maxId uint32) {
	rangeBackend = make([]int, 8192*2)
	for i := range rangeBackend {
		rangeBackend[i] = i
	}

	_aggregates = []Aggregate{

		makeInterestBy("country", "sex"),
		makeInterestBy("country", "status"),
		makeInterestBy("country", "joined"),
		makeInterestBy("country", "birth"),

		makeInterestBy("city", "sex"),
		makeInterestBy("city", "status"),
		makeInterestBy("city", "birth"),
		makeInterestBy("city", "joined"),

		makeTrippleAggregate("birth", "sex", "status"),

		makeTrippleAggregate("country", "sex", "status"),
		makeTrippleAggregate("country", "sex", "joined"),
		makeTrippleAggregate("country", "sex", "birth"),
		makeTrippleAggregate("country", "status", "joined"),
		makeTrippleAggregate("country", "status", "birth"),

		makeTrippleAggregate("city", "sex", "status"),
		makeTrippleAggregate("city", "sex", "joined"),
		makeTrippleAggregate("city", "sex", "birth"),
		makeTrippleAggregate("city", "status", "joined"),
		makeTrippleAggregate("city", "status", "birth"),

		makeQuadAggregate("country", "sex", "status", "joined"),
		makeQuadAggregate("country", "sex", "status", "birth"),

		makeQuadAggregate("city", "sex", "status", "birth"),
		makeQuadAggregate("city", "sex", "status", "joined"),

		makeInterestBy4("country", "sex", "joined"),
		makeInterestBy4("country", "sex", "birth"),
		makeInterestBy4("country", "status", "joined"),
		makeInterestBy4("country", "status", "birth"),

		makeInterestBy4("city", "sex", "joined"),
		makeInterestBy4("city", "sex", "birth"),
		makeInterestBy4("city", "status", "joined"),
		makeInterestBy4("city", "status", "birth"),
	}
	for i := uint32(0); i < maxId; i++ {
		account := &((*accounts)[i])
		for _, aggregate := range _aggregates {
			aggregate.Index(i+1, account)
		}
	}
}

/** interestBy */
type interestBy struct {
	data                 [][]int32
	first, second, third string

	stripe                                 int
	firstOffset, secondOffset, thirdOffset uint
	firstIndex, secondIndex, thirdIndex    func(*Account) uint
	names                                  map[string]int
	backData                               []int32
}

func makeInterestBy(second, third string) *interestBy {
	p := interestBy{
		first:        "interests",
		firstOffset:  SNAME_OFFSET,
		second:       second,
		secondOffset: offsetOf(second),
		secondIndex:  indexFuncOf(second),
		stripe:       1,
		names:        map[string]int{"interests": 0, second: 1},
	}

	p.stripe = capacityOf(third)
	p.third = third
	p.thirdOffset = offsetOf(third)
	p.thirdIndex = indexFuncOf(third)
	p.names[third] = 2

	p.data = make([][]int32, capacityOf(second)*p.stripe)

	columnCapacity := len(InterestDict.values)
	p.backData = make([]int32, len(p.data)*columnCapacity)

	for i := 0; i < len(p.data); i++ {
		p.data[i] = p.backData[i*columnCapacity : (i+1)*columnCapacity]
		//p.data[i] = make([]int32, columnCapacity)
	}

	return &p
}

func (p *interestBy) Index(id uint32, account *Account) {
	j := p.secondIndex(account)*uint(p.stripe) + p.thirdIndex(account)

	pos := (id - 1) * 2
	interest0 := Store.interests[pos] >> 1
	interest1 := Store.interests[pos+1]

	xs := p.data[j]
	pxs0 := uintptr(unsafe.Pointer(&xs[0]))

	// + unroll interests0
	for i := uintptr(0); i < 63 && interest0 != 0; i++ {
		if 1 == interest0&1 {
			//xs[i] += 1
			*(*uint32)(unsafe.Pointer(pxs0 + i*4)) += 1

		}
		interest0 >>= 1
	}
	// + unroll interests1
	for i := uintptr(63); i < 127 && interest1 != 0; i++ {
		if 1 == interest1&1 {
			//xs[i] += 1
			*(*uint32)(unsafe.Pointer(pxs0 + i*4)) += 1
		}
		interest1 >>= 1
	}
}

func (p *interestBy) GroupKey(i, j int, groups []bool) uint64 {
	var key uint64
	if groups[0] {
		key |= keyBits(i+1, p.firstOffset)
	}
	if groups[1] {
		key |= keyBits(j/p.stripe, p.secondOffset)
	}
	if groups[2] {
		key |= keyBits(j%p.stripe, p.thirdOffset)
	}
	return key
}

func (p *interestBy) Aggregate(group *Group) (map[uint64]int32, bool) {
	if len(group.GroupBy) > 3 || len(group.FilterBy) > 3 {
		return nil, false
	}

	groups := []bool{false, false, false}
	for _, n := range group.GroupBy {
		if v, ok := p.names[n]; ok {
			groups[v] = true
		} else {
			return nil, false
		}
	}

	filters := []int{-1, -1, -1}
	for n, _ := range group.FilterBy {
		if v, ok := p.names[n]; ok {
			filters[v] = filterValueOf(n, group)
		} else {
			return nil, false
		}
	}

	// interestBy используем только если в условиях были интересы
	if groups[0] == false && filters[0] == -1 {
		return nil, false
	}
	counters := countersBorrow() // make(map[uint64]int32)

	var listJ []int
	if filters[1] != -1 && filters[2] != -1 {
		listJ = []int{filters[1]*p.stripe + filters[2]}
	} else if filters[1] != -1 {
		jLen := capacityOf(p.third)
		listJ = make([]int, jLen)
		f1 := filters[1] * p.stripe
		for jk := 0; jk < jLen; jk++ {
			listJ[jk] = f1 + jk
		}
	} else if filters[2] != -1 {
		jLen := capacityOf(p.second)
		listJ = make([]int, jLen)
		for jk := 0; jk < jLen; jk++ {
			listJ[jk] = jk*p.stripe + filters[2]
		}
	}

	if len(listJ) == 0 && filters[0] == -1 {
		for j, ds := range p.data {
			for i, val := range ds {
				if val > 0 {
					counters[p.GroupKey(i, j, groups)] += val
				}
			}
		}
	} else if len(listJ) == 0 && filters[0] != -1 {
		i := filters[0]
		for j, ds := range p.data {
			if val := ds[i]; val > 0 {
				counters[p.GroupKey(i, j, groups)] += val
			}
		}
	} else if len(listJ) != 0 && filters[0] == -1 {
		for _, j := range listJ {
			ds := p.data[j]
			for i, val := range ds {
				if val > 0 {
					counters[p.GroupKey(i, j, groups)] += val
				}
			}
		}
	} else if len(listJ) != 0 && filters[0] != -1 {
		i := filters[0]
		for _, j := range listJ {
			if val := p.data[j][i]; val > 0 {
				counters[p.GroupKey(i, j, groups)] += val
			}
		}
	}
	//println(len(counters))
	return counters, true
}

/* quadAggregate */
type quadAggregate struct {
	data                                                [][]int32
	first, second, third, forth                         string
	cap2, stripe, stripe2                               int
	firstOffset, secondOffset, thirdOffset, forthOffset uint
	firstIndex, secondIndex, thirdIndex, forthIndex     func(*Account) uint
	names                                               map[string]int
}

func makeQuadAggregate(first, second, third, forth string) *quadAggregate {
	p := quadAggregate{
		data: make([][]int32, capacityOf(first)),

		first:  first,
		second: second,
		third:  third,
		forth:  forth,

		firstOffset:  offsetOf(first),
		secondOffset: offsetOf(second),
		thirdOffset:  offsetOf(third),
		forthOffset:  offsetOf(forth),

		//secondCapacity: capacityOf(second),
		cap2:    capacityOf(second),
		stripe:  capacityOf(third),
		stripe2: capacityOf(forth),

		firstIndex:  indexFuncOf(first),
		secondIndex: indexFuncOf(second),
		thirdIndex:  indexFuncOf(third),
		forthIndex:  indexFuncOf(forth),

		names: map[string]int{first: 0, second: 1, third: 2, forth: 3}}

	columnCapacity := p.cap2 * p.stripe * p.stripe2
	for i := 0; i < len(p.data); i++ {
		p.data[i] = make([]int32, columnCapacity)
	}
	return &p
}

func (p *quadAggregate) Index(id uint32, account *Account) {
	p.data[p.firstIndex(account)][(uint(p.stripe)*p.secondIndex(account)+p.thirdIndex(account))*uint(p.stripe2)+p.forthIndex(account)] += 1
}

func (p *quadAggregate) GroupKey(i int, j int, groups []bool) uint64 {
	var key uint64
	if groups[0] {
		key |= keyBits(i, p.firstOffset)
	}
	if groups[1] {
		key |= keyBits(j/p.stripe/p.stripe2, p.secondOffset)
	}
	if groups[2] {
		key |= keyBits((j/p.stripe2)%p.stripe, p.thirdOffset)
	}
	if groups[3] {
		key |= keyBits(j%p.stripe2, p.forthOffset)
	}
	return key
}

var countersPool = sync.Pool{
	New: func() interface{} {
		return make(map[uint64]int32, 2048)
	},
}

func countersBorrow() map[uint64]int32 {
	return countersPool.Get().(map[uint64]int32)
}

func countersRelease(buffer map[uint64]int32) {
	for key := range buffer {
		delete(buffer, key)
	}
	countersPool.Put(buffer)
}

func (p *quadAggregate) Aggregate(group *Group) (map[uint64]int32, bool) {

	if len(group.GroupBy) > 3 || len(group.FilterBy) > 3 {
		return nil, false
	}

	groups := []bool{false, false, false, false}
	for _, n := range group.GroupBy {
		if v, ok := p.names[n]; ok {
			groups[v] = true
		} else {
			return nil, false
		}
	}
	filters := []int{-1, -1, -1, -1}
	for n, _ := range group.FilterBy {
		if v, ok := p.names[n]; ok {
			filters[v] = filterValueOf(n, group)
		} else {
			return nil, false
		}
	}

	counters := countersBorrow() // make(map[uint64]int32)

	var listJ []int
	if filters[1] != -1 || filters[2] != -1 || filters[3] != -1 {
		xs := filters[1:2]
		if xs[0] == -1 {
			xs = rangeBackend[0:p.cap2]
		}
		ys := filters[2:3]
		if ys[0] == -1 {
			ys = rangeBackend[0:p.stripe]
		}
		zs := filters[3:4]
		if zs[0] == -1 {
			zs = rangeBackend[0:p.stripe2]
		}
		listJ = make([]int, len(xs)*len(ys)*len(zs))[:0]
		for _, x := range xs {
			for _, y := range ys {
				for _, z := range zs {
					listJ = append(listJ, (x*p.stripe+y)*p.stripe2+z)
				}
			}
		}
	}

	if filters[0] == -1 {
		for i := 0; i < len(p.data); i++ {
			if len(listJ) == 0 {
				for j := 0; j < len(p.data[i]); j++ {
					if val := p.data[i][j]; val > 0 {
						counters[p.GroupKey(i, j, groups)] += val
					}
				}
			} else {
				for _, j := range listJ {
					if val := p.data[i][j]; val > 0 {
						counters[p.GroupKey(i, j, groups)] += val
					}
				}
			}
		}
	} else {
		i := filters[0]
		if len(listJ) == 0 {
			for j := 0; j < len(p.data[i]); j++ {
				if val := p.data[i][j]; val > 0 {
					counters[p.GroupKey(i, j, groups)] += val
				}
			}
		} else {
			for _, j := range listJ {
				if val := p.data[i][j]; val > 0 {
					counters[p.GroupKey(i, j, groups)] += val
				}
			}
		}

	}
	//println(len(counters))
	return counters, true
}

/* tippleAggregate */
type trippleAggregate struct {
	data                 [][]int32
	first, second, third string
	//secondCapacity,
	stripe                                 int
	firstOffset, secondOffset, thirdOffset uint
	firstIndex, secondIndex, thirdIndex    func(*Account) uint
	names                                  map[string]int
}

func makeTrippleAggregate(first, second, third string) *trippleAggregate {
	p := trippleAggregate{
		data: make([][]int32, capacityOf(first)),

		first:  first,
		second: second,
		third:  third,

		firstOffset:  offsetOf(first),
		secondOffset: offsetOf(second),
		thirdOffset:  offsetOf(third),

		//secondCapacity: capacityOf(second),
		stripe: capacityOf(third),

		firstIndex:  indexFuncOf(first),
		secondIndex: indexFuncOf(second),
		thirdIndex:  indexFuncOf(third),

		names: map[string]int{first: 0, second: 1, third: 2}}

	columnCapacity := capacityOf(second) * p.stripe
	for i := 0; i < len(p.data); i++ {
		p.data[i] = make([]int32, columnCapacity)
	}
	return &p
}

func (p *trippleAggregate) Index(id uint32, account *Account) {
	p.data[p.firstIndex(account)][uint(p.stripe)*p.secondIndex(account)+p.thirdIndex(account)] += 1
}

func (p *trippleAggregate) GroupKey(i int, j int, groups []bool) uint64 {
	var key uint64
	if groups[0] {
		key |= keyBits(i, p.firstOffset)
	}
	if groups[1] {
		key |= keyBits(j/p.stripe, p.secondOffset)
	}
	if groups[2] {
		key |= keyBits(j%p.stripe, p.thirdOffset)
	}
	return key
}

func (p *trippleAggregate) Aggregate(group *Group) (map[uint64]int32, bool) {

	if len(group.GroupBy) > 3 || len(group.FilterBy) > 3 {
		return nil, false
	}

	groups := []bool{false, false, false}
	for _, n := range group.GroupBy {
		if v, ok := p.names[n]; ok {
			groups[v] = true
		} else {
			return nil, false
		}
	}
	filters := []int{-1, -1, -1}
	for n, _ := range group.FilterBy {
		if v, ok := p.names[n]; ok {
			filters[v] = filterValueOf(n, group)
		} else {
			return nil, false
		}
	}
	counters := countersBorrow() // make(map[uint64]int32)

	var listJ []int
	if filters[1] != -1 && filters[2] != -1 {
		listJ = []int{filters[1]*p.stripe + filters[2]}
	} else if filters[1] != -1 {
		jLen := capacityOf(p.third)
		listJ = make([]int, jLen)
		f1 := filters[1] * p.stripe
		for jk := 0; jk < jLen; jk++ {
			listJ[jk] = f1 + jk
		}
	} else if filters[2] != -1 {
		jLen := capacityOf(p.second)
		listJ = make([]int, jLen)
		for jk := 0; jk < jLen; jk++ {
			listJ[jk] = jk*p.stripe + filters[2]
		}
	}

	if filters[0] == -1 {
		for i := 0; i < len(p.data); i++ {
			if len(listJ) == 0 {
				for j := 0; j < len(p.data[i]); j++ {
					if val := p.data[i][j]; val > 0 {
						counters[p.GroupKey(i, j, groups)] += val
					}
				}
			} else {
				for _, j := range listJ {
					if val := p.data[i][j]; val > 0 {
						counters[p.GroupKey(i, j, groups)] += val
					}
				}
			}
		}
	} else {
		i := filters[0]
		if len(listJ) == 0 {
			for j := 0; j < len(p.data[i]); j++ {
				if val := p.data[i][j]; val > 0 {
					counters[p.GroupKey(i, j, groups)] += val
				}
			}
		} else {
			for _, j := range listJ {
				if val := p.data[i][j]; val > 0 {
					counters[p.GroupKey(i, j, groups)] += val
				}
			}
		}

	}
	//println(len(counters))
	return counters, true
}

/* doubleAggregate */
/*
type doubleAggregate struct {
	data                      [][]int32
	first, second             string
	firstOffset, secondOffset uint
	firstIndex, secondIndex   func(*Account) uint
}

func makeDoubleAggregate(first, second string) *doubleAggregate {
	aggregate := doubleAggregate{
		data:         make([][]int32, capacityOf(first)),
		first:        first,
		second:       second,
		firstOffset:  offsetOf(first),
		secondOffset: offsetOf(second),
		firstIndex:   indexFuncOf(first),
		secondIndex:  indexFuncOf(second)}

	columnCapacity := capacityOf(second)
	for i := 0; i < len(aggregate.data); i++ {
		aggregate.data[i] = make([]int32, columnCapacity)
	}
	return &aggregate
}

func (p *doubleAggregate) Index(id uint32, account *Account) {
	p.data[p.firstIndex(account)][p.secondIndex(account)] += 1
}

func (p *doubleAggregate) GroupKey(x int, y int, groups []bool) uint64 {
	if groups[0] && groups[1] {
		return keyBits(x, p.firstOffset) | keyBits(y, p.secondOffset)
	} else if groups[0] {
		return keyBits(x, p.firstOffset)
	} else {
		return keyBits(y, p.secondOffset)
	}
}

func (p *doubleAggregate) Aggregate(group *Group) (map[uint64]int32, bool) {

	xFilter := -1
	yFilter := -1
	var firstGroup, secondGroup bool

	if len(group.GroupBy) == 1 && p.first == group.GroupBy[0] {
		// x -> ..
		firstGroup = true
		if len(group.FilterBy) == 1 && group.FilterBy[p.second] {
			yFilter = filterValueOf(p.second, group)
		} else if len(group.FilterBy) != 0 {
			return nil, false
		}
	} else if len(group.GroupBy) == 1 && p.second == group.GroupBy[0] {
		// y -> ..
		secondGroup = true
		if len(group.FilterBy) == 1 && group.FilterBy[p.first] {
			xFilter = filterValueOf(p.first, group)
		} else if len(group.FilterBy) != 0 {
			return nil, false
		}
	} else if len(group.GroupBy) == 2 && ((p.second == group.GroupBy[0] && p.first == group.GroupBy[1]) || (p.second == group.GroupBy[1] && p.first == group.GroupBy[0])) {
		// x,y -> ..
		firstGroup = true
		secondGroup = true
		if len(group.FilterBy) == 1 && group.FilterBy[p.first] {
			xFilter = filterValueOf(p.first, group)
		} else if len(group.FilterBy) == 1 && group.FilterBy[p.second] {
			yFilter = filterValueOf(p.second, group)
		} else if len(group.FilterBy) != 0 {
			return nil, false
		}
	} else {
		return nil, false
	}

	counters := make(map[uint64]int32)
	// todo: написать 3 цикла в зависимости от x_filter/y_filter
	for i := 0; i < len(p.data); i++ {
		if xFilter == -1 || xFilter == i {
			for j := 0; j < len(p.data[i]); j++ {
				if yFilter == -1 || yFilter == j {
					if val := p.data[i][j]; val > 0 {
						counters[p.GroupKey(i, j, []bool{firstGroup, secondGroup})] += val
					}
				}
			}
		}
	}

	return counters, true
}
*/

func keyBits(value int, offset uint) uint64 {
	if offset == 0 {
		return uint64(value)
	} else {
		return uint64(value) << offset
	}
}

/** interestBy4 */
type interestBy4 struct {
	data                        [][]int32
	first, second, third, forth string

	cap2, stripe, stripe2, fullStripe int

	firstOffset, secondOffset, thirdOffset, forthOffset uint
	firstIndex, secondIndex, thirdIndex, forthIndex     func(*Account) uint
	names                                               map[string]int
	backData                                            []int32
}

func makeInterestBy4(second, third, forth string) *interestBy4 {
	p := interestBy4{
		first:        "interests",
		firstOffset:  SNAME_OFFSET,
		second:       second,
		secondOffset: offsetOf(second),
		secondIndex:  indexFuncOf(second),
		stripe:       1,
		names:        map[string]int{"interests": 0, second: 1},
	}

	p.stripe = capacityOf(third)
	p.third = third
	p.thirdOffset = offsetOf(third)
	p.thirdIndex = indexFuncOf(third)
	p.names[third] = 2

	p.cap2 = capacityOf(second)
	p.stripe2 = capacityOf(forth)
	p.forth = forth
	p.forthOffset = offsetOf(forth)
	p.forthIndex = indexFuncOf(forth)
	p.names[forth] = 3

	p.fullStripe = p.stripe * p.stripe2

	columnCapacity := len(InterestDict.values)
	p.data = make([][]int32, p.cap2*p.fullStripe)
	p.backData = make([]int32, len(p.data)*columnCapacity)
	for i := 0; i < len(p.data); i++ {
		p.data[i] = p.backData[i*columnCapacity : (i+1)*columnCapacity]
		//p.data[i] = make([]int32, columnCapacity)
	}
	return &p
}

func (p *interestBy4) Index(id uint32, account *Account) {
	j := p.secondIndex(account)*uint(p.fullStripe) + p.thirdIndex(account)*uint(p.stripe2) + p.forthIndex(account)

	pos := (id - 1) * 2
	interest0 := Store.interests[pos] >> 1
	interest1 := Store.interests[pos+1]

	xs := p.data[j]
	pxs0 := uintptr(unsafe.Pointer(&xs[0]))

	// + unroll interests0
	for i := uintptr(0); i < 63 && interest0 != 0; i++ {
		if 1 == interest0&1 {
			//xs[i] += 1
			*(*uint32)(unsafe.Pointer(pxs0 + i*4)) += 1

		}
		interest0 >>= 1
	}
	// + unroll interests1
	for i := uintptr(63); i < 127 && interest1 != 0; i++ {
		if 1 == interest1&1 {
			//xs[i] += 1
			*(*uint32)(unsafe.Pointer(pxs0 + i*4)) += 1
		}
		interest1 >>= 1
	}
}

func (p *interestBy4) GroupKey(i, j int, groups []bool) uint64 {
	var key uint64
	if groups[0] {
		key |= keyBits(i+1, p.firstOffset)
	}
	if groups[1] {
		key |= keyBits(j/p.fullStripe, p.secondOffset)
	}
	if groups[2] {
		key |= keyBits((j/p.stripe2)%p.stripe, p.thirdOffset)
	}
	if groups[3] {
		key |= keyBits(j%p.stripe, p.thirdOffset)
	}
	return key
}

func (p *interestBy4) Aggregate(group *Group) (map[uint64]int32, bool) {
	if len(group.GroupBy) > 3 || len(group.FilterBy) > 3 {
		return nil, false
	}

	groups := []bool{false, false, false, false}
	for _, n := range group.GroupBy {
		if v, ok := p.names[n]; ok {
			groups[v] = true
		} else {
			return nil, false
		}
	}

	filters := []int{-1, -1, -1, -1}
	for n, _ := range group.FilterBy {
		if v, ok := p.names[n]; ok {
			filters[v] = filterValueOf(n, group)
		} else {
			return nil, false
		}
	}

	// interestBy* агрегат используем только если в условиях или ключе группы были интересы
	if groups[0] == false && filters[0] == -1 {
		return nil, false
	}

	counters := countersBorrow() // make(map[uint64]int32)

	var listJ []int
	if filters[1] != -1 || filters[2] != -1 || filters[3] != -1 {
		xs := filters[1:2]
		if xs[0] == -1 {
			xs = rangeBackend[0:p.cap2]
		}
		ys := filters[2:3]
		if ys[0] == -1 {
			ys = rangeBackend[0:p.stripe]
		}
		zs := filters[3:4]
		if zs[0] == -1 {
			zs = rangeBackend[0:p.stripe2]
		}
		listJ = make([]int, 0, len(xs)*len(ys)*len(zs))
		for _, x := range xs {
			for _, y := range ys {
				for _, z := range zs {
					listJ = append(listJ, (x*p.stripe+y)*p.stripe2+z)
				}
			}
		}
	}
	/*
	   // (3) Conversion of a Pointer to a uintptr and back, with arithmetic.
	   //
	   // If p points into an allocated object, it can be advanced through the object
	   // by conversion to uintptr, addition of an offset, and conversion back to Pointer.
	   //
	   //	p = unsafe.Pointer(uintptr(p) + offset)
	   //
	   // The most common use of this pattern is to access fields in a struct
	   // or elements of an array:
	   //
	   //	// equivalent to f := unsafe.Pointer(&s.f)
	   //	f := unsafe.Pointer(uintptr(unsafe.Pointer(&s)) + unsafe.Offsetof(s.f))
	   //
	   //	// equivalent to e := unsafe.Pointer(&x[i])
	   //	e := unsafe.Pointer(uintptr(unsafe.Pointer(&x[0])) + i*unsafe.Sizeof(x[0]))
	*/
	if len(listJ) == 0 && filters[0] == -1 {
		for j, ds := range p.data {
			for i, val := range ds {
				if val > 0 {
					counters[p.GroupKey(i, j, groups)] += val
				}
			}
		}
	} else if len(listJ) == 0 && filters[0] != -1 {
		i := filters[0]
		for j, ds := range p.data {
			if val := ds[i]; val > 0 {
				counters[p.GroupKey(i, j, groups)] += val
			}
		}
	} else if len(listJ) != 0 && filters[0] == -1 {
		for _, j := range listJ {
			ds := p.data[j]
			for i, val := range ds {
				if val > 0 {
					counters[p.GroupKey(i, j, groups)] += val
				}
			}
		}
	} else if len(listJ) != 0 && filters[0] != -1 {
		i := filters[0]
		for _, j := range listJ {
			if val := p.data[j][i]; val > 0 {
				counters[p.GroupKey(i, j, groups)] += val
			}
		}
	}
	//println(len(counters))
	return counters, true
}

// birthYear  uint16 // Ограничено снизу 01.01.1950 и сверху 01.01.2005-ым.
// joinedYear uint16 // 2011-01-01 : 2018-01-01

func CalculateGroups() {
	for _, key := range []string{
		"city,status",
		"interests",
		"country,sex",
		"city,sex",
	} {
		groupOf("keys", key).aggregate(true)
	}

	groupOf("keys", "city,status", "sex", "m").aggregate(true)
	groupOf("keys", "city,status", "sex", "f").aggregate(true)

	groupOf("keys", "country,sex", "sex", "m").aggregate(true)
	groupOf("keys", "country,sex", "sex", "f").aggregate(true)

	//fmt.Printf("%v\tCalculateGroups 1\n", Timenow())

	for joined := 2011; joined <= 2018; joined++ {
		if evio.GetEpollWait() == 0 {
			fmt.Printf("%v\tCalculateGroups stopped\n", Timenow())
			return
		}
		joinedStr := strconv.Itoa(joined)
		groupOf("keys", "city", "joined", joinedStr).aggregate(true)
		groupOf("keys", "city,status", "joined", joinedStr).aggregate(true)
		groupOf("keys", "city,status", "joined", joinedStr, "sex", "m").aggregate(true)
		groupOf("keys", "city,status", "joined", joinedStr, "sex", "f").aggregate(true)
		groupOf("keys", "city,sex", "joined", joinedStr).aggregate(true)
		for interest := range InterestDict.ids {
			if evio.GetEpollWait() == 0 {
				fmt.Printf("%v\tCalculateGroups stopped\n", Timenow())
				return
			}
			if interest != "" {
				groupOf("keys", "country,sex", "joined", joinedStr, "interests", interest, "joined", joinedStr).aggregate(true)
				groupOf("keys", "city,status", "joined", joinedStr, "interests", interest).aggregate(true)
				groupOf("keys", "city,sex", "joined", joinedStr, "interests", interest).aggregate(true)
				groupOf("keys", "country,status", "joined", joinedStr, "interests", interest).aggregate(true)
			}
		}
		//group;[city]/1/joined,status,;50;1;39;998
	}
	//fmt.Printf("%v\tCalculateGroups 2\n", Timenow())

	for interest := range InterestDict.ids {
		if evio.GetEpollWait() == 0 {
			fmt.Printf("%v\tCalculateGroups stopped\n", Timenow())
			return
		}
		if interest != "" {
			groupOf("keys", "country,sex", "interests", interest).aggregate(true)
			groupOf("keys", "city,status", "interests", interest).aggregate(true)
			groupOf("keys", "city", "interests", interest).aggregate(true)
			groupOf("keys", "city,sex", "interests", interest).aggregate(true)
		}
	}
	//fmt.Printf("%v\tCalculateGroups 3\n", Timenow())

	for birth := 1950; birth <= 2005; birth++ {
		if evio.GetEpollWait() == 0 {
			fmt.Printf("%v\tCalculateGroups stopped\n", Timenow())
			return
		}
		birthStr := strconv.Itoa(birth)

		groupOf("keys", "country,status", "birth", birthStr).aggregate(true)
		groupOf("keys", "city,status", "birth", birthStr).aggregate(true)
		groupOf("keys", "city,status", "birth", birthStr, "sex", "m").aggregate(true)
		groupOf("keys", "city,status", "birth", birthStr, "sex", "f").aggregate(true)

		groupOf("keys", "city", "birth", birthStr).aggregate(true)
		groupOf("keys", "interests", "birth", birthStr).aggregate(true)
		groupOf("keys", "city", "birth", birthStr, "status", "свободны").aggregate(true)
		groupOf("keys", "city", "birth", birthStr, "status", "заняты").aggregate(true)
		groupOf("keys", "city", "birth", birthStr, "status", "всё сложно").aggregate(true)

	}

	fmt.Printf("%v\tCalculateGroups done\n", Timenow())

	// [city status]/1/joined,sex,	16	38	2378	32207
	// [city status]/joined,	6	2	488	979
	// [city sex]/1/interests,joined,	17	10	632	994
	// [city status]/interests,	16	13	854	1952
	// [city status]/birth,sex,	15	3	261	997
	// [city]/birth,sex,	33	7	236	977
	// [city]/interests,joined,	27	7	289	977
	// [city]/interests,	44	7	177	997

	// [city]/1/birth,sex,	25	6	273	977
	// [city status]/-1/birth,status,	29	4	168	977
	// [city status]/-1/birth,	13	4	375	977
	// [status]/-1/likes,	90	2	32	977
	// [country status]/-1/joined,status,	24	2	122	977
	// [city sex]/1/joined,status,	11	2	266	977
	// [city]/-1/joined,status,	33	2	88	977
	// [city]/1/sex,	43	2	68	977
	// [country]/-1/joined,status,	33	1	59	977
	// [city status]/1/birth,	8	1	244	977
	// [interests]/1/	28	1	69	977
	// [city status]/-1/interests,	21	14	697	976
	// [city]/1/interests,	35	10	306	976
	// [city status]/1/interests,joined,	14	8	627	976
	// [city sex]/-1/interests,	13	8	675	976
	// [city]/1/birth,status,	25	7	312	976
	// [city sex]/1/interests,	17	6	402	976
	// [city sex]/1/birth,status,	15	5	390	976
	// [city status]/-1/birth,interests,	20	5	292	976
	// [city status]/1/birth,sex,	13	5	450	976
	// [city]/-1/interests,joined,	30	5	194	976
	// [city sex]/-1/joined,status,	12	4	406	976
	// [city]/-1/birth,status,	38	4	128	976

}

func CalculateGroupsParallel(wg *sync.WaitGroup) {
	for birth := 1950; birth <= 2005; birth++ {
		birthStr := strconv.Itoa(birth)
		for interest := range InterestDict.ids {
			if evio.GetEpollWait() == 0 {
				fmt.Printf("%v\tCalculateGroupsParallel stopped\n", Timenow())
				return
			}
			if interest != "" {
				groupOf("keys", "city,status", "birth", birthStr, "interests", interest).aggregate(true)
			}
		}
	}

	wg.Done()
	fmt.Printf("%v\tCalculateGroupsParallel done\n", Timenow())
}

func groupOf(conditions ...string) *Group {
	var params = make(map[string]string)
	for i, cond := range conditions {
		if i%2 == 0 {
			params[cond] = conditions[i+1]
		}
	}
	params["order"] = "1"

	var group Group
	if err := group.FromParams(params); err != nil {
		log.Printf("FromParams failed, %v, %v\n", params, err)
		return nil
	}
	return &group
}
