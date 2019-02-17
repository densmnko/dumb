package hlc18

import (
	"math"
	"sort"
	"sync"
)

/*
Решение должно проверять совместимость только с противоположным полом
Если в GET-запросе передана страна или город с ключами country и city соответственно,
то нужно искать только среди живущих в указанном месте.

В ответе ожидается код 200 и структура {"accounts": [ ... ]} либо код 404 ,
если пользователя с искомым id не обнаружено в хранимых данных. По ключу "accounts" должны быть N пользователей,
сортированных по убыванию их совместимости с обозначенным id.
Число N задаётся в запросе GET-параметром limit и не бывает больше 20.

1. 	Наибольший вклад в совместимость даёт наличие статуса "свободны".
	Те кто "всё сложно" идут во вторую очередь, а "занятые" в третью и последнюю
	(очень вероятно их вообще не будет в ответе).

2. 	Далее идёт совместимость по интересам.
	Чем больше совпавших интересов у пользователей, тем более они совместимы.

3. 	Третий по значению параметр - различие в возрасте. Чем больше разница, тем меньше совместимость.

4. 	Те, у кого активирован премиум-аккаунт, пропихиваются в самый верх, вперёд обычных пользователей.
	Если таких несколько, то они сортируются по совместимости между собой.

5. 	Если общих интересов нет, то стоит считать пользователей абсолютно несовместимыми с compatibility = 0.

Число N задаётся в запросе GET-параметром limit и не бывает больше 20.

*/

func abs(n int32) int32 {
	y := n >> 31
	return (n ^ y) - y
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func commonInterests2(me0, me1 uint64, somebodyId uint32) (count uint64) {

	pos := (somebodyId - 1) * 2
	n := me0 & Store.interests[pos] //somebody.interests0
	for n != 0 {
		count++
		n &= n - 1 // Zero the lowest-order one-bit
	}
	n = me1 & Store.interests[pos+1]
	for n != 0 {
		count++
		n &= n - 1 // Zero the lowest-order one-bit
	}
	return count
}

func commonInterests2_new(me0, me1 uint64, somebodyId uint32) byte {
	var count uint64
	pos := (somebodyId - 1) * 2
	n := me0 & Store.interests[pos]

	if n != 0 {
		n -= (n >> 1) & m1             //put count of each 2 bits into those 2 bits
		n = (n & m2) + ((n >> 2) & m2) //put count of each 4 bits into those 4 bits
		n = (n + (n >> 4)) & m4        //put count of each 8 bits into those 8 bits
		count += (n * h01) >> (TEST_BITS_64 - 8)
	}

	n = me1 & Store.interests[pos+1]
	if n != 0 {
		n -= (n >> 1) & m1             //put count of each 2 bits into those 2 bits
		n = (n & m2) + ((n >> 2) & m2) //put count of each 4 bits into those 4 bits
		n = (n + (n >> 4)) & m4        //put count of each 8 bits into those 8 bits
		count += (n * h01) >> (TEST_BITS_64 - 8)
	}

	return byte(count)
}

func containsSorted(array []uint32, id uint32) bool {
	i := sort.Search(len(array), func(i int) bool { return array[i] >= id })
	return i < len(array) && array[i] == id
}

func contains(array []uint32, id uint32) bool {
	for i := 0; i < len(array); i++ {
		if id == array[i] {
			return true
		}
	}
	return false
}

func containsLike(array []Like, id uint32) bool {
	for i := 0; i < len(array); i++ {
		if id == array[i].Id {
			return true
		} else if array[i].Id > id { // т.к. теперь сортированные
			return false
		}
	}
	return false
}

func recommendKey(sex, premium, status, interest byte) int {
	return int(sex)*6*InterestsCount + int(premium)*3*InterestsCount + int(status)*InterestsCount + int(interest)
}

var CountryCount int
var CityCount int
var InterestsCount int

func recommendKeyCountry(key int, country byte) int {
	return key*CountryCount + int(country)
}

func recommendKeyCity(key int, city uint16) int {
	return key*CityCount + int(city)
}

var selectionPool = sync.Pool{
	New: func() interface{} { return make([]uint64, 1024*16) },
}

func token(id uint32, common uint64, myBirth int32) uint64 {
	return uint64(id) | (common << 24) | (uint64(abs(myBirth-Store.Accounts[id-1].birth)) << 32)
}

func tokenCommon(token uint64) uint32 {
	return uint32((token << 32) >> (24 + 32))
}

var TOKEN_MASK = uint64(mask(0, 24))

func tokenId(token uint64) uint32 {
	return uint32(token & TOKEN_MASK)
}

func tokenBirthDiff(token uint64) uint64 {
	return token >> 32
}

func Recommend(myId uint32, limit int, params map[string]string) []uint32 {

	var cityCode uint16
	var countryCode byte

	if v, _ := params["city"]; v != "" {
		if cityCode = CityDict.ids[v]; cityCode == 0 {
			return []uint32{}
		}
	} else if v, _ := params["country"]; v != "" {
		if countryCode = CountryDict.ids[v]; countryCode == 0 {
			return []uint32{}
		}
	}

	cacheKey := CacheKey{myId, cityCode, countryCode}
	if cache, e := getRecommendL2Cache(cacheKey, limit); e == nil {
		return cache
	}

	interests0 := Store.interests[(myId-1)*2]
	interests1 := Store.interests[(myId-1)*2+1]
	hasInterests := interests0 != 0 || interests1 != 0

	if !hasInterests {
		return []uint32{}
	}

	me := &Store.Accounts[myId-1]

	// какой максимум может быть в принципе
	// maxInterests := int(bitcount(me.interests0) + bitcount(me.interests1))
	// какой максимум видели
	maxSeenInterests := uint64(0)
	// сколько таких максимумов видели
	maxSeenInterestsCount := 0

	sex := byte(0) // 0 - m, 1 - f
	if 1 == me.getSex() {
		sex = 1
	}

	resultBuff := [20]uint32{}
	result := resultBuff[:]
	foundCount := 0

	poolBytes := selectionPool.Get().([]uint64)

	interestsBuffer := make([]byte, 16) // todo: ? sync.Pool
	myInterests := interestsToArray(interests0, interests1, interestsBuffer)
	for _, premium := range []byte{1, 0} {
		for _, status := range []byte{0, 1, 2} {
			var selected = poolBytes[:0]
			iter := makeIndexOrIterator()
			for _, anInterest := range myInterests {
				key := recommendKey(sex, premium, status, anInterest)
				if countryCode != 0 {
					iter.push(RecommendIndexCountryCompact[recommendKeyCountry(int(key), countryCode)])
				} else if cityCode != 0 {
					iter.push(RecommendIndexCityCompact[recommendKeyCity(int(key), cityCode)])
				} else {
					iter.push(RecommendIndexCompact[key])
				}
			}

			for id := iter.Next(); id != math.MaxUint32 && limit > 0; id = iter.Next() {
				var common uint64

				pos := (id - 1) * 2
				n := interests0 & Store.interests[pos]
				n -= (n >> 1) & m1             //put count of each 2 bits into those 2 bits
				n = (n & m2) + ((n >> 2) & m2) //put count of each 4 bits into those 4 bits
				n = (n + (n >> 4)) & m4        //put count of each 8 bits into those 8 bits
				common += (n * h01) >> (TEST_BITS_64 - 8)
				n = interests1 & Store.interests[pos+1]
				n -= (n >> 1) & m1             //put count of each 2 bits into those 2 bits
				n = (n & m2) + ((n >> 2) & m2) //put count of each 4 bits into those 4 bits
				n = (n + (n >> 4)) & m4        //put count of each 8 bits into those 8 bits
				common += (n * h01) >> (TEST_BITS_64 - 8)

				if common == 0 {
					continue

				} else if common > maxSeenInterests {
					maxSeenInterests = common
					maxSeenInterestsCount = 1
					selected = append(selected, token(id, common, me.birth))
				} else if common == maxSeenInterests {
					maxSeenInterestsCount++
					selected = append(selected, token(id, common, me.birth))
				} else if common > 0 && maxSeenInterestsCount < limit {
					// с 0 совпавшими интересами нас не интересуют совсем
					selected = append(selected, token(id, common, me.birth))
				}
			}

			if len(selected) > 0 {
				var order = func(i int, j int) bool {
					// обратная сортировка, в начале массива будут "лучшие" записи
					// Далее идёт совместимость по интересам.
					// Чем больше совпавших интересов у пользователей, тем более они совместимы
					ti := selected[i]
					tj := selected[j]
					commonI := tokenCommon(ti)
					commonJ := tokenCommon(tj)
					if commonI > commonJ {
						return true
					} else if commonI == commonJ {
						// Третий по значению параметр - различие в возрасте. Чем больше разница, тем меньше совместимость
						diffI := tokenBirthDiff(ti)
						diffJ := tokenBirthDiff(tj)
						if diffI < diffJ {
							return true
						} else if diffI == diffJ {
							return tokenId(ti) < tokenId(tj)
						}
						/*
							iid := tokenId(selected[i])
							jid := tokenId(selected[j])
							ai := &Store.Accounts[iid-1]
							aj := &Store.Accounts[jid-1]
							diffI := abs(me.birth - ai.birth)
							diffJ := abs(me.birth - aj.birth)
							if diffI < diffJ {
								return true
							} else if diffI == diffJ {
								return iid < jid
							}
						*/
						return false
					}
					return false
				}
				sort.Slice(selected, order)

				for i := 0; i < len(result)-foundCount && i < len(selected); i++ {
					result[i+foundCount] = tokenId(selected[i])
				}
				//copy(result[foundCount:], selected)
				foundCount += len(selected)
			}
			if foundCount >= limit {
				selectionPool.Put(poolBytes)
				l := min(limit, len(result))
				//for i := 0; i < l; i++ {
				//	result[i] = tokenId(result[i])
				//}
				putRecommendL2Cache(cacheKey, result[:l])
				return result[:l]
			}
		}
	}

	// todo: сюда мы попадем только если нашли меньше чеm limit?
	selectionPool.Put(poolBytes)
	l := min(limit, foundCount)
	//for i := 0; i < l; i++ {
	//	result[i] = tokenId(result[i])
	//}
	putRecommendL2Cache(cacheKey, result[:l])
	return result[:l]
}

func interestsToArray(interests0 uint64, interests1 uint64, buffer []byte) []byte {

	var myInterests = buffer[:0]

	// + unroll interests0
	interest := interests0 >> 1
	for i := byte(0); i < 63 && interest != 0; i++ {
		if interest&1 == 1 {
			myInterests = append(myInterests, i)
		}
		interest >>= 1
	}
	// + unroll interests1
	interest = interests1
	for i := byte(0); i < 64 && interest != 0; i++ {
		if interest&1 == 1 {
			myInterests = append(myInterests, i+63)
		}
		interest >>= 1
	}
	return myInterests
}

/*
func commonInterests(meId, somebodyId uint32) byte {
	pos := (meId-1) * 2
	return commonInterests2(Store.interests[pos], Store.interests[pos+1], somebodyId )
}
*/

func bitcountNaive(n uint64) (count byte) {
	count = 0
	for n != 0 {
		count += byte(n & 1)
		n >>= 1
	}
	return count
}

// Sparse-ones: Only iterates as many times as there are 1-bits in the integer.
func bitcountSparse(n uint64) (count byte) {
	for n != 0 {
		count++
		n &= n - 1 // Zero the lowest-order one-bit
	}
	return count
}

const TEST_BITS_64 = 8 * 8
const TEST_BITS_32 = 4 * 8

const m1 = ^uint64(0) / 3
const m2 = ^uint64(0) / 5
const m4 = ^uint64(0) / 17
const h01 = ^uint64(0) / 255

func bitcountWP3(n uint64) (count byte) {
	n -= (n >> 1) & m1             //put count of each 2 bits into those 2 bits
	n = (n & m2) + ((n >> 2) & m2) //put count of each 4 bits into those 4 bits
	n = (n + (n >> 4)) & m4        //put count of each 8 bits into those 8 bits
	return byte((n * h01) >> (TEST_BITS_64 - 8))
}

/*
// https://bisqwit.iki.fi/source/misc/bitcounting/#Wp3NiftyRevised
const unsigned TEST_BITS = sizeof(TestType) * CHAR_BITS;

//This uses fewer arithmetic operations than any other known
//implementation on machines with fast multiplication.
//It uses 12 arithmetic operations, one of which is a multiply.
static unsigned bitcount (TestType n)
{
TestType m1 = (~(TestType)0) / 3;
TestType m2 = (~(TestType)0) / 5;
TestType m4 = (~(TestType)0) / 17;
TestType h01 = (~(TestType)0) / 255;

n -= (n >> 1) & m1;             //put count of each 2 bits into those 2 bits
n = (n & m2) + ((n >> 2) & m2); //put count of each 4 bits into those 4 bits
n = (n + (n >> 4)) & m4;        //put count of each 8 bits into those 8 bits

return (n * h01) >> (TEST_BITS-8);
// returns left 8 bits of x + (x<<8) + (x<<16) + (x<<24) + ...
}
*/
