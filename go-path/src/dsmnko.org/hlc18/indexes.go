package hlc18

import (
	"fmt"
	"math"
)

const (
	HUGE_BLOCK_SIZE   = 32 * 1024 // для очень неселективных индексов (пол)
	LARGE_BLOCK_SIZE  = 4 * 1024  // для средне-селективных индексов (десятки-сотни бакетов)
	MEDIUM_BLOCK_SIZE = 1024      // для высоко-средне-селективных
	SMALL_BLOCK_SIZE  = 32        // для высоко-селективных

	LIKES_INDEX_VECTOR_CAPACITY = 24
)

var (
	//var LikesIndex []*VectorUint32

	LikesIndexCompact [][]uint32

	InterestsIndexCompact    [][]uint32
	InterestsSexIndexCompact [][]uint32
	Interests2xIndexCompact  [][]uint32

	BirthYearIndexCompact [][]uint32
	CityIndexCompact      [][]uint32
	CountryIndexCompact   [][]uint32
	FnameIndexCompact     [][]uint32
	SexIndexCompact       [][]uint32
	StatusIndexCompact    [][]uint32

	//sex, premium, status, interest [,country|city]
	RecommendIndexCompact        map[int][]uint32
	RecommendIndexCountryCompact map[int][]uint32
	RecommendIndexCityCompact    map[int][]uint32

	// имена которые мы видели для полов [пол][имя] - 1:0
	SexNames [2][]byte

	BirthYearCityIndexCompact [][]uint32
	EmailPrefixIndexCompact   [][]uint32

	CountryPhoneCodeSexIndex map[int][]uint32
	CityPhoneCodeSexIndex    map[int][]uint32
)

var interestsIndexContainer = make([][]uint32, 90)
var interestsSexIndexContainer = make([][]uint32, 90*2)
var interests2xIndexContainer = make([][]uint32, (90+1)*90)

func MakeIndexes(capacity int) {

	SexNames[0] = make([]byte, 256)
	SexNames[1] = make([]byte, 256)

	for x := range interestsIndexContainer {
		interestsIndexContainer[x] = make([]uint32, 44000)
	}
	for x := range interestsSexIndexContainer {
		interestsSexIndexContainer[x] = make([]uint32, 22000)
	}

	for x := range interests2xIndexContainer {
		interests2xIndexContainer[x] = make([]uint32, 1300) //1430 max, 1300 med
	}

	LikesIndexCompact = make([][]uint32, capacity)
	for x := range LikesIndexCompact {
		LikesIndexCompact[x] = make([]uint32, 0, 32)
	}

}

func ResetIndexes() {

	// update all counters
	InterestsCount = len(InterestDict.ids)
	CountryCount = len(CountryDict.ids) + 1
	CityCount = len(CityDict.ids) + 1

	// -------------- RebuildIndexes()

	BirthYearIndexCompact = nil
	BirthYearCityIndexCompact = nil
	CityIndexCompact = nil
	CountryIndexCompact = nil
	FnameIndexCompact = nil
	SexIndexCompact = nil
	StatusIndexCompact = nil
	EmailPrefixIndexCompact = nil

	CountryPhoneCodeSexIndex = nil
	CityPhoneCodeSexIndex = nil

	// ------------- RebuildRecommendIndexes()

	resetRecommendIndex(&RecommendIndexCompact)
	resetRecommendIndex(&RecommendIndexCityCompact)
	resetRecommendIndex(&RecommendIndexCountryCompact)

	resetWithContainer(&InterestsIndexCompact, &interestsIndexContainer)
	resetWithContainer(&InterestsSexIndexCompact, &interestsSexIndexContainer)
	resetWithContainer(&Interests2xIndexCompact, &interests2xIndexContainer)

}

func resetRecommendIndex(index *map[int][]uint32) {

	if len(*index) == 0 {
		*index = make(map[int][]uint32)
	} else {
		for i, xs := range *index {
			(*index)[i] = xs[:0] //index[i][:0]
		}
	}
}

const BIRTH_YEARS_LEN = 56

func RebuildRecommendIndexes(accounts *[]Account, maxId uint32) {

	//recommendTemp := make(map[int]*VectorUint32/*, 2*2*3*InterestsCount*/)
	//recommendTempCity := make(map[int]*VectorUint32 /*, 2*2*3*InterestsCount*CityCount*/)
	//recommendTempCountry := make(map[int]*VectorUint32 /*, 2*2*3*InterestsCount*CountryCount*/)

	//interests2xIndexTemp := make([]*VectorUint32, (90+1)*90)
	interestsBuffer := make([]byte, 32) //
	for i := uint32(0); i < maxId; i++ {
		accPtr := &((*accounts)[i])
		id := i + 1
		appendRecommendAndInterestsIndex(id, accPtr, interestsBuffer)
	}

	//RecommendIndexCityCompact = make(map[int][]uint32, len(recommendTempCity))
	//compact_map(recommendTempCity, RecommendIndexCityCompact)
	//recommendTempCity = nil
	//indexMapStat("RecommendIndexCityCompact", RecommendIndexCityCompact)

	//RecommendIndexCountryCompact = make(map[int][]uint32, len(recommendTempCountry))
	//compact_map(recommendTempCountry, RecommendIndexCountryCompact)
	//recommendTempCountry = nil
	//indexMapStat("RecommendIndexCountryCompact", RecommendIndexCountryCompact)

	//RecommendIndexCompact = make(map[int][]uint32, len(recommendTemp))
	//compact_map(recommendTemp, RecommendIndexCompact)
	//recommendTemp = nil
	//indexMapStat("RecommendIndexCompact", RecommendIndexCompact)

}

// sex -1|2
func countryPhoneCodeSexKey(country byte, phoneCode uint16, sex byte) int {
	return (int(sex-1)*1000+int(phoneCode))*CountryCount + int(country)
}

func cityPhoneCodeSexKey(city uint16, phoneCode uint16, sex byte) int {
	return (int(sex-1)*1000+int(phoneCode))*CityCount + int(city)
}

func RebuildIndexes(accounts *[]Account, maxId uint32) {

	birthYearTemp := make([]*VectorUint32, BIRTH_YEARS_LEN)
	birthYearCityTemp := make([]*VectorUint32, BIRTH_YEARS_LEN*(CityCount+1))
	cityTemp := make([]*VectorUint32, CityCount)
	sexTemp := make([]*VectorUint32, 2)
	statusTemp := make([]*VectorUint32, 3)

	countryIndices := make([]*VectorUint32, (CountryCount + 1))
	fnameIndices := make([]*VectorUint32, len(FnameDict.values)+1)
	emailPrefixTemp := make([]*VectorUint32, 26*(26+1))

	CountryPhoneCodeSexIndex = make(map[int][]uint32, CountryCount*100*2)
	CityPhoneCodeSexIndex = make(map[int][]uint32, CityCount*100*2)

	for i := uint32(0); i < maxId; i++ {

		accPtr := &((*accounts)[i])
		id := i + 1

		//compactLikes(id)

		appendBirthYearCityIndex(id, accPtr, birthYearTemp, cityTemp, birthYearCityTemp)

		appendCountryIndex(id, accPtr, countryIndices)
		appendFnameIndex(id, accPtr, fnameIndices)
		appendSexIndex(id, accPtr, sexTemp)
		appendStatusIndex(id, accPtr, statusTemp)

		appendEmailIndex(id, accPtr, emailPrefixTemp)

		phoneCode := accPtr.getPhoneCode()
		if phoneCode != 0 {
			key := countryPhoneCodeSexKey(accPtr.getCountry(), phoneCode, accPtr.getSex())
			CountryPhoneCodeSexIndex[key] = append(CountryPhoneCodeSexIndex[key], id)

			key = cityPhoneCodeSexKey(accPtr.getCity(), phoneCode, accPtr.getSex())
			CityPhoneCodeSexIndex[key] = append(CityPhoneCodeSexIndex[key], id)
		}

	}

	//indexMapStat("CountryPhoneCodeSexIndex",CountryPhoneCodeSexIndex)
	//indexMapStat("CityPhoneCodeSexIndex",CityPhoneCodeSexIndex)

	BirthYearIndexCompact = make([][]uint32, len(birthYearTemp))
	compact(birthYearTemp, BirthYearIndexCompact, false)
	birthYearTemp = nil

	CityIndexCompact = make([][]uint32, len(cityTemp))
	compact(cityTemp, CityIndexCompact, false)
	cityTemp = nil
	//indexStat("CityIndexCompact",BirthYearIndexCompact)

	BirthYearCityIndexCompact = make([][]uint32, len(birthYearCityTemp))
	compact(birthYearCityTemp, BirthYearCityIndexCompact, false)
	birthYearCityTemp = nil
	//indexStat("BirthYearCityIndexCompact",BirthYearCityIndexCompact)

	CountryIndexCompact = make([][]uint32, len(countryIndices))
	compact(countryIndices, CountryIndexCompact, false)
	countryIndices = nil

	FnameIndexCompact = make([][]uint32, len(fnameIndices))
	compact(fnameIndices, FnameIndexCompact, false)
	fnameIndices = nil

	SexIndexCompact = make([][]uint32, len(sexTemp))
	compact(sexTemp, SexIndexCompact, false)
	sexTemp = nil

	StatusIndexCompact = make([][]uint32, len(statusTemp))
	compact(statusTemp, StatusIndexCompact, false)
	statusTemp = nil

	EmailPrefixIndexCompact = make([][]uint32, len(emailPrefixTemp))
	compact(emailPrefixTemp, EmailPrefixIndexCompact, false)
	emailPrefixTemp = nil

}

func resetWithContainer(index, cont *[][]uint32) {
	*index = make([][]uint32, len(*cont))
	for x := range *cont {
		(*index)[x] = (*cont)[x][:0]
	}
}

/*
func compactLikes(id uint32 ) {
	account := &Store.Accounts2[id-1]
	if account.updatedLikes != nil {
		added := len(*account.updatedLikes) / 2 //account.updatedLikes.Len() / 2
		current := account.likesId.Len()
		newLikesId := makeIdArray(current + added)
		copy(newLikesId, account.likesId)
		account.likesId = newLikesId
		//	stat[added] += 1
		//}
		newLikeTs := make([]uint32, current+added)
		copy(newLikeTs, account.likesTs)
		account.likesTs = newLikeTs
		for i := 0; i < added; i++ {
			account.likesId.Put(current+i, (*account.updatedLikes)[i*2])
			account.likesTs[current+i] = (*account.updatedLikes)[i*2+1]
		}
		account.updatedLikes = nil
	}
}
*/

func compact_map(src map[int]*VectorUint32, dest map[int][]uint32) {
	for key, val := range src {
		array := make([]uint32, val.Len(), val.Len())
		val.CopyTo(array)
		dest[key] = array
		delete(src, key)
	}
}

func compact(src []*VectorUint32, dest [][]uint32, sort bool) {
	for i := 0; i < len(src); i++ {
		if (src)[i] != nil {
			dest[i] = make([]uint32, src[i].len, src[i].len)
			src[i].CopyTo(dest[i])
			if sort {
				Uint32Slice(dest[i]).Sort()
			}
			src[i] = nil
		} else {
			dest[i] = make([]uint32, 0)
		}
	}
}

const LIKE_MOD_FLAG = 2000000

func AppendAccountLikesIndex(id uint32, likes []Like) {
	for _, like := range likes {
		LikesIndexCompact[like.Id-1] = append(LikesIndexCompact[like.Id-1], id)
		if len(LikesIndexCompact[like.Id-1]) > 1 && LikesIndexCompact[like.Id-1][0] < LIKE_MOD_FLAG {
			LikesIndexCompact[like.Id-1][0] += LIKE_MOD_FLAG
		}
	}
}

/*
func appendInterestsIndex(id uint32, account *Account) {
	// + unroll interests0
	pos := (id - 1) * 2
	interest := Store.interests[pos] >> 1
	for i := 0; i < 63 && interest != 0; i++ {
		if interest&1 == 1 {
			InterestsIndexCompact[i] = append(InterestsIndexCompact[i],id)
		}
		interest >>= 1
	}
	// + unroll interests1
	interest = Store.interests[pos+1]
	for i := byte(0); i < 64 && interest != 0; i++ {
		if interest&1 == 1 {
			InterestsIndexCompact[i+63] = append(InterestsIndexCompact[i+63],id)
		}
		interest >>= 1
	}
}
*/

func appendBirthYearCityIndex(id uint32, account *Account, dstBirth, dstCity, dstBirthCity []*VectorUint32) {
	year := account.getBirthYear() - 1950
	{
		m := dstBirth[year]
		if m == nil {
			m = makeVector(LARGE_BLOCK_SIZE)
			dstBirth[year] = m
		}
		m.Push(id)
	}
	city := account.getCity()
	{
		m := dstCity[city]
		if m == nil {
			m = makeVector(LARGE_BLOCK_SIZE)
			dstCity[city] = m
		}
		m.Push(id)
	}
	birthCity := city*BIRTH_YEARS_LEN + year
	{
		m := dstBirthCity[birthCity]
		if m == nil {
			m = makeVector(512)
			dstBirthCity[birthCity] = m
		}
		m.Push(id)
	}

}

func appendCountryIndex(id uint32, account *Account, dst []*VectorUint32) {
	city := account.getCountry()
	m := dst[city]
	if m == nil {
		m = makeVector(MEDIUM_BLOCK_SIZE)
		dst[city] = m
	}
	m.Push(id)
}

func appendFnameIndex(id uint32, account *Account, dst []*VectorUint32) {
	city := account.getFname()
	m := dst[city]
	if m == nil {
		m = makeVector(MEDIUM_BLOCK_SIZE)
		dst[city] = m
	}
	m.Push(id)
}

func appendSexIndex(id uint32, account *Account, dst []*VectorUint32) {
	sex := account.data & 1
	m := dst[sex]
	if m == nil {
		m = makeVector(HUGE_BLOCK_SIZE)
		dst[sex] = m
	}
	m.Push(id)
}

func appendEmailIndex(id uint32, account *Account, dst []*VectorUint32) {
	i := emailIndex(Store.Accounts2[id-1].email)
	m := dst[i]
	if m == nil {
		m = makeVector(HUGE_BLOCK_SIZE)
		dst[i] = m
	}
	m.Push(id)
}

func emailIndex(s string) int {
	bytes := S2b(s)
	return int(bytes[0]-'a')*26 + int(bytes[1]-'a')
}

func appendStatusIndex(id uint32, account *Account, dst []*VectorUint32) {
	status := account.getStatus() - 1
	m := dst[status]
	if m == nil {
		m = makeVector(HUGE_BLOCK_SIZE)
		dst[status] = m
	}
	m.Push(id)
}

func appendRecommendAndInterestsIndex(id uint32, account *Account, buffer []byte) {
	pos := (id - 1) * 2
	sex := byte(account.data & 1)
	// + unroll interests0
	interests := interestsToArray(Store.interests[pos], Store.interests[pos+1], buffer)

	for i, code := range interests {
		addRecommendKeys(id, code, account)
		InterestsIndexCompact[code] = append(InterestsIndexCompact[code], id)
		InterestsSexIndexCompact[code*2+sex] = append(InterestsSexIndexCompact[code*2+sex], id)

		for j := i + 1; j < len(interests); j++ {
			code2x := int(code)*90 + int(interests[j])
			Interests2xIndexCompact[code2x] = append(Interests2xIndexCompact[code2x], id)

			//if m, ok := inter2x[key]; ok {
			//	m.Push(id)
			//} else {
			//	inter2x[key] = makeInitialVector(MEDIUM_BLOCK_SIZE, id)
			//}
		}
	}

	/*
		interest := Store.interests[pos] >> 1
		for i := byte(0); i < 63 && interest != 0; i++ {
			if interest&1 == 1 {
				addRecommendKeys(id, i, account, dst, dstCountry, dstCity)
				InterestsIndexCompact[i] = append(InterestsIndexCompact[i], id)
				InterestsSexIndexCompact[i*2+sex] = append(InterestsSexIndexCompact[i*2+sex], id)
			}
			interest >>= 1
		}
		// + unroll interests1
		interest = Store.interests[pos+1]
		for i := byte(0); i < 64 && interest != 0; i++ {
			if interest&1 == 1 {
				addRecommendKeys(id, i+63, account, dst, dstCountry, dstCity)
				InterestsIndexCompact[i+63] = append(InterestsIndexCompact[i+63], id)
				InterestsSexIndexCompact[(i+63)*2+sex] = append(InterestsSexIndexCompact[(i+63)*2+sex], id)
			}
			interest >>= 1
		}
	*/

}

func addRecommendKeys(id uint32, interest byte, account *Account) {

	key := recommendKey(byte(account.data&1), account.IsPremium(), account.getStatus()-1, interest)
	if xs, ok := RecommendIndexCompact[key]; ok {
		RecommendIndexCompact[key] = append(xs, id)
	} else {
		RecommendIndexCompact[key] = append(make([]uint32, 0, 9500), id)
	}

	keyCity := recommendKeyCity(key, account.getCity())

	if xs, ok := RecommendIndexCityCompact[keyCity]; ok {
		RecommendIndexCityCompact[keyCity] = append(xs, id)
	} else {
		RecommendIndexCityCompact[keyCity] = append([]uint32{}, id)
	}

	keyCountry := recommendKeyCountry(key, account.getCountry())
	if xs, ok := RecommendIndexCountryCompact[keyCountry]; ok {
		RecommendIndexCountryCompact[keyCountry] = append(xs, id)
	} else {
		RecommendIndexCountryCompact[keyCountry] = append([]uint32{}, id)
	}

}

type IndexIterator interface {
	Next() uint32
	// возвращает индекс если он один или nil если индексов нет или их несколько
	ToSingle() []uint32
	Len() int
}

const DEFAULT_ITER_CAPACITY = 16

type IndexOrIterator struct {
	//current   int
	indexes   [][]uint32
	positions []int
}

func makeIndexOrIterator() *IndexOrIterator {
	return &IndexOrIterator{
		indexes:   make([][]uint32, 0, DEFAULT_ITER_CAPACITY),
		positions: make([]int, 0, DEFAULT_ITER_CAPACITY),
	}
}

func (p *IndexOrIterator) push(index []uint32) {
	if len(index) != 0 {
		p.indexes = append(p.indexes, index)
		p.positions = append(p.positions, len(index)-1)
	}
}
func (p *IndexOrIterator) ToSingle() (index []uint32) {
	if len(p.indexes) == 1 {
		return p.indexes[0]
	} else if len(p.indexes) == 0 {
		return emptyIndex
	}
	return nil
}

func (p *IndexOrIterator) Len() (l int) {
	for _, i := range p.indexes {
		l += len(i)
	}
	return l
}

func (p *IndexOrIterator) Next() uint32 {

	nextVal := uint32(0)
	nextFound := false

	// найдем след значение для возврата
	for i := range p.indexes {
		pos := p.positions[i]
		if pos >= 0 && nextVal <= p.indexes[i][pos] {
			nextVal = p.indexes[i][pos]
			nextFound = true
		}
	}

	if !nextFound {
		return math.MaxUint32
	}

	// сдвинемся в позициях индексов
	for i := range p.indexes {
		pos := p.positions[i]
		if pos >= 0 && nextVal == p.indexes[i][pos] {
			p.positions[i] = p.positions[i] - 1
		}
	}

	return nextVal
}

type IndexAndIterator struct {
	base      []uint32
	basePos   int
	indexes   [][]uint32
	positions []int
}

func makeIndexAndIterator() *IndexAndIterator {
	return &IndexAndIterator{
		indexes:   make([][]uint32, 0, DEFAULT_ITER_CAPACITY),
		positions: make([]int, 0, DEFAULT_ITER_CAPACITY),
	}
}

func (p *IndexAndIterator) push(index []uint32) {
	p.indexes = append(p.indexes, index)
	p.positions = append(p.positions, len(index)-1)
}

func (p *IndexAndIterator) ToSingle() (index []uint32) {
	if len(p.indexes) == 1 {
		return p.indexes[0]
	} else if len(p.indexes) == 0 {
		return emptyIndex
	}
	return nil
}

func (p *IndexAndIterator) Prepare() *IndexAndIterator {
	// найдем мин индекс
	ml := math.MaxInt32
	i := 0
	for x, xs := range p.indexes {
		if l := len(xs); l < ml {
			ml = l
			i = x
		}
	}
	p.base = p.indexes[i]
	p.basePos = len(p.base) - 1
	// delete trick, no leaks, https://github.com/golang/go/wiki/SliceTricks
	copy(p.indexes[i:], p.indexes[i+1:])
	p.indexes[len(p.indexes)-1] = nil // or the zero value of T
	p.indexes = p.indexes[:len(p.indexes)-1]
	p.positions = append(p.positions[:i], p.positions[i+1:]...)
	return p
}

// todo: брать самый короткий индекс и пятится по нему назад
func (p *IndexAndIterator) Next() uint32 {
	for b := p.basePos; b >= 0; b-- {
		max := p.base[b]
		nextFound := true
		for i, xs := range p.indexes {
			//var found = p.moveTo(i, max)
			//index := p.indexes[i]
			pos := p.positions[i]
			found := false
			for pos >= 0 && xs[pos] >= max {
				pos--
				p.positions[i] = pos
				found = xs[pos+1] == max // можно nextFound здесь считать
			}
			nextFound = nextFound && found
		}
		if nextFound {
			p.basePos = b - 1
			return max
		}
	}
	return math.MaxUint32
}

func (p *IndexAndIterator) Len() (l int) {
	for _, i := range p.indexes {
		l += len(i)
	}
	return l
}

func indexVectorStat(name string, index []*VectorUint32) {

	stat := make(map[int]int)
	for _, li := range index {
		if li != nil {
			stat[li.Len()] += 1
		}
	}
	fmt.Printf("\nindex stat for '%s'\nbucket;size\n", name)
	for k, v := range stat {
		fmt.Printf("%d;%d\n", k, v)
	}

}

func indexStat(name string, index [][]uint32) {
	stat := make(map[int]int)
	for _, li := range index {
		if li != nil {
			stat[len(li)] += 1
		}
	}
	fmt.Printf("\nindex stat for '%s'\nbucket;size\n", name)
	for k, v := range stat {
		fmt.Printf("%d;%d\n", k, v)
	}

}

func indexMapStat(name string, index map[int][]uint32) {

	stat := make(map[int]int)
	for _, lk := range index {
		stat[len(lk)] += 1
	}
	fmt.Printf("\nindex stat for '%s'\nbucket;size\n", name)
	for k, v := range stat {
		fmt.Printf("%d;%d\n", k, v)
	}

}
