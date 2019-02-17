package hlc18

import (
	"sort"
	"sync"
)

/*
Подбор по похожим симпатиям: /accounts/<id>/suggest/

Этот тип запросов похож на предыдущий тем, что он тоже про поиск "вторых половинок".
Аналогично пересылается id пользователя, для которого мы ищем вторую половинку и аналогично используется GET-параметер limit.
Различия в реализации. Теперь мы ищем, кого лайкают пользователи того же пола с похожими "симпатиями" и предлагаем тех,
кого они недавно лайкали сами. В случае, если в запросе передан GET-параметр country или city,
то искать "похожие симпатии" нужно только в определённой локации.

Похожесть симпатий определим как функцию: similarity = f (me, account),
которая вычисляется однозначно как сумма из дробей 1 / abs(my_like['ts'] - like['ts']),
где my_like и like - это симпатии к одному и тому же пользователю.
Для дроби, где my_like['ts'] == like['ts'], заменяем дробь на 1.
Если общих лайков нет, то стоит считать пользователей абсолютно непохожими с similarity = 0.
Если у одного аккаунта есть несколько лайков на одного и того же пользователя с разными датами,
то в формуле используется среднее арифметическое их дат.

В ответе возвращается список тех, кого ещё не лайкал пользователь с указанным id,
но кого лайкали пользователи с самыми похожими симпатиями.
Сортировка по убыванию похожести,
а между лайками одного такого пользователя - по убыванию id лайка.

*/

// todo: попробовать float32 ?
func similarity(likeId, likeTs uint32, other []Like) float32 {
	if otherTs := likeTsOf(other, likeId); otherTs != likeTs {
		return float32(1.0) / float32(abs(int32(likeTs)-int32(otherTs)))
	}
	return 1.0
}

type Other struct {
	id     uint32
	weight float32
	//index  int
}

var othersMapPool = sync.Pool{
	New: func() interface{} {
		return make(map[uint32]float32, 3200)
	},
}

func othersMapBorrow() map[uint32]float32 {
	return othersMapPool.Get().(map[uint32]float32)
}

func othersMapRelease(buffer map[uint32]float32) {
	for key := range buffer {
		delete(buffer, key)
	}
	othersMapPool.Put(buffer)
}

// todo: !!! "утоптать" лайки после фазы-2
func Suggest(myId uint32, limit int, params map[string]string) []uint32 {

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
	if cache, e := getSuggestL2Cache(cacheKey, limit); e == nil {
		return cache
	}

	me := Store.Likes[myId-1]

	if len(me) == 0 {
		putSuggestL2Cache(cacheKey, []uint32{})
		return []uint32{}
	}

	// найдем всех с кем одинаково лакайли
	// кто -> вес
	othersMap := othersMapBorrow() // make(map[uint32]float32, 3200)

	// likeid -> ts (unique)
	mylikes := make(map[uint32]uint32, len(me))

	//index := 0
	//othersMapIndex := make(map[uint32]int)

	for i := 0; i < len(me); i++ {
		likeId := me[i].Id
		mylikes[likeId] = likeTsOf(me, likeId)
	}

	for likeId, likeTs := range mylikes { //i := 0; i < me.likesId.Len(); i++ {
		// if vector := LikesIndex[likeId-1]; vector != nil {
		// 	vector.Iterate(func(_ int, otherId uint32) bool {
		// 		otherAccount := &Store.Accounts[otherId-1]
		// 		if otherId != myId &&
		// 			(countryCode == 0 || countryCode == otherAccount.getCountry()) &&
		// 			(cityCode == 0 || cityCode == otherAccount.getCity()) {
		// 			othersMap[otherId] += similarity(likeId, likeTs, &Store.Accounts2[otherId-1])
		// 		}
		// 		return true
		// 	})
		// }

		var prevOther uint32
		for _, otherId := range LikesIndexCompact[likeId-1] {
			if otherId != myId && prevOther != otherId {
				prevOther = otherId
				otherAccount := &Store.Accounts[otherId-1]
				if (countryCode == 0 || countryCode == otherAccount.getCountry()) &&
					(cityCode == 0 || cityCode == otherAccount.getCity()) {
					othersMap[otherId] += similarity(likeId, likeTs, Store.Likes[otherId-1])
					//if _, found := othersMapIndex[otherId]; !found {
					//	othersMapIndex[otherId] = index
					//	index++
					//}
				}
			}
		}

	}

	if len(othersMap) == 0 {
		othersMapRelease(othersMap)
		putSuggestL2Cache(cacheKey, []uint32{})
		return []uint32{}
	}

	others := make([]Other, 0, len(othersMap))

	for k, v := range othersMap {
		others = append(others, Other{k, v})
	}

	othersMapRelease(othersMap)

	sort.Slice(others, func(i, j int) bool {
		//return others[i].weight > others[j].weight
		if diff := others[i].weight - others[j].weight; diff > 0 {
			return true
		} else if diff < 0 {
			return false
		} else {
			return others[i].id < others[j].id
		}
	})

	//fmt.Printf("%v\n", others)

	result := make([]uint32, 20)[:0]

	//seen := make(map[uint32]bool)

	suggestedBuffer := make([]uint32, 0, 128)

	//println(len(others)) 3200 max

	for _, otherId := range others {
		suggested := suggestedBuffer
		other := Store.Likes[otherId.id-1]
		for i := 0; i < len(other); i++ {
			if likeId := other[i].Id; !containsLike(me, likeId) {
				suggested = append(suggested, likeId)
			}
		}
		// В ответе возвращается список тех, кого ещё не лайкал пользователь с указанным id,
		// но кого лайкали пользователи с самыми похожими симпатиями.
		// Сортировка по убыванию похожести, а между лайками одного такого пользователя - по убыванию id лайка.
		sort.Slice(suggested, func(i, j int) bool {
			return suggested[i] > suggested[j]
		})

		for _, s := range suggested {
			if !seen(result, s) { // защита от дублей
				result = append(result, s)
				if rlen := len(result); rlen > 19 || rlen >= limit {
					putSuggestL2Cache(cacheKey, result)
					return result
				}
			}
		}
	}

	finalResult := result[:min(limit, len(result))]
	putSuggestL2Cache(cacheKey, finalResult)
	return finalResult
}

func seen(array []uint32, value uint32) bool {
	for _, v := range array {
		if v == value {
			return true
		}
	}
	return false
}

// todo: вычислять при переиндексации и т.д.
func likeTsOf(likes []Like, likeId uint32) uint32 {
	count := int64(0)
	sum := int64(0)

	// for i := 0; i < len(likes); i++ {
	// 	if likeId == likes[i].Id {
	// 		count++
	// 		sum += int64(likes[i].Ts)
	// 	} else if likes[i].Id > likeId {
	// 		break
	// 	}
	// }

	for i := 0; i < len(likes) && likes[i].Id <= likeId; i++ {
		if likeId == likes[i].Id {
			count++
			sum += int64(likes[i].Ts)
		}
	}

	if count == 0 {
		println("todo: likeTsOf:count == 0") // todo
		return 0
	}
	return uint32(sum / count)
}
