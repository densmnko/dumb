package hlc18

import (
	"errors"
	"fmt"
	"github.com/valyala/fasthttp"
	"io"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
)

// Полей для группировки всего пять - sex, status, interests, country, city.
type Group struct {
	Limit           int
	GroupBy         []string
	FilterBy        map[string]bool
	Filter          Filter
	dataMask        uint64
	Order           int8
	interestGroup   bool // надо ли группировать по интересам
	interestKeyCode byte // значение кода интереса если его надо учитывать в группах
	likes           bool // группа с лайками

}

func unpackSex(data uint64) byte {
	return byte(1&data) + 1
}

func unpackInterest(data uint64) byte {
	return byte((data & SNAME_MASK) >> SNAME_OFFSET)
}

func packInterest(data *uint64, code byte) {
	if code > 127 {
		log.Fatalf("invalid interest '%d'", code)
	}
	*data = (*data & ^SNAME_MASK) | (uint64(code) << SNAME_OFFSET)
}

func calcInterestKeyPart(code byte) uint64 {
	if code > 127 {
		log.Fatalf("invalid interest '%d'", code)
	}
	return (uint64(code) << SNAME_OFFSET)
}

func (g *Group) FromParams(params map[string]string) error {

	g.Filter.birth_gt = math.MaxInt32
	g.Filter.birth_lt = math.MaxInt32

	g.FilterBy = make(map[string]bool)

	var found = false

	for key, val := range params {
		switch key {
		case "order":
			if "1" == val {
				g.Order = 1
			} else if "-1" == val {
				g.Order = -1
			} else {
				return fmt.Errorf("invalid %s=%s", key, val)
			}
		case "keys":
			g.GroupBy = strings.Split(val, ",")
			for _, group := range g.GroupBy {
				switch group {
				case "sex":
					g.dataMask |= 1
				case "status":
					g.dataMask |= STATUS_MASK
				case "country":
					g.dataMask |= COUNTRY_MASK
				case "city":
					g.dataMask |= CITY_MASK
				case "interests":
					g.interestGroup = true

				default:
					return fmt.Errorf("invalid %s=%s", key, val)
				}
			}

		case "fname":
			g.FilterBy[key] = true
			if g.Filter.fname_eq, found = FnameDict.ids[val]; !found {
				g.Filter.ShortCircuit = true
				return nil
			}

		case "sname":
			g.FilterBy[key] = true
			if g.Filter.sname_eq, found = SnameDict.ids[val]; !found {
				g.Filter.ShortCircuit = true
				return nil
			}
		case "status":
			g.FilterBy[key] = true
			if g.Filter.status_eq = statusCode(val); g.Filter.status_eq == 0 {
				return fmt.Errorf("invalid %s=%s", key, val)
			}

		case "sex":
			g.FilterBy[key] = true
			if g.Filter.sex_eq = sexCode(val); g.Filter.sex_eq == 0 {
				return fmt.Errorf("invalid %s=%s", key, val)
			}
		case "city":
			g.FilterBy[key] = true
			if g.Filter.city_eq, found = CityDict.ids[val]; !found {
				g.Filter.ShortCircuit = true
				return nil
			}
		case "country":
			g.FilterBy[key] = true
			if g.Filter.country_eq, found = CountryDict.ids[val]; !found {
				g.Filter.ShortCircuit = true
				return nil
			}

		//для likes будет только один id, для interests только одна строка, для birth и joined - будет одно число - год).
		case "likes":
			g.likes = true
			g.FilterBy[key] = true
			if i, e := strconv.Atoi(val); e == nil {
				if uint32(i) > Store.MaxId || i == 0 {
					g.Filter.ShortCircuit = true
					return nil
				}
				g.Filter.likes_contains = []uint32{uint32(i)}
			} else {
				return fmt.Errorf("invalid %s=%s, %v\n", key, val, e)
			}

		case "interests":
			g.FilterBy[key] = true
			if code, ok := InterestDict.ids[val]; ok && code > 0 {
				g.Filter.interests_any.Set(code)
				g.interestKeyCode = code
			} else {
				g.Filter.ShortCircuit = true
				return nil
			}

		case "birth":
			g.FilterBy[key] = true
			if year, e := strconv.Atoi(val); e == nil {
				if year < 1950 || year > 2005 {
					g.Filter.ShortCircuit = true
					return nil
				}
				g.Filter.birth_year = uint16(year)
			} else {
				return fmt.Errorf("invalid %s=%s", key, val)
			}

		case "joined":
			g.FilterBy[key] = true
			if year, e := strconv.Atoi(val); e == nil {
				if year < 2011 || year > 2018 {
					g.Filter.ShortCircuit = true
					return nil
				}
				g.Filter.joined_year = uint16(year)
			} else {
				return fmt.Errorf("invalid %s=%s", key, val)
			}

		// что это за щщит ?
		case "phone":
		case "email":
		case "premium":
			return fmt.Errorf("ORLY? %s=%s", key, val)
		default:
			return fmt.Errorf("unknown param %s=%s", key, val)
		}
	}

	if len(g.GroupBy) == 0 || g.Order == 0 {
		return errors.New("empty keys or order")
	}

	g.Filter.updateCheckFlags()
	return nil
}

var errorInvalidParam = errors.New("invalid param")

func (p *Group) FromArgs(args *fasthttp.Args) error {

	p.Filter.birth_gt = math.MaxInt32
	p.Filter.birth_lt = math.MaxInt32
	p.FilterBy = make(map[string]bool)

	var found = false

	var err error

	args.VisitAll(func(keyBytes, valueBytes []byte) {
		if err != nil {
			return
		}
		val := B2s(valueBytes)
		key := B2s(keyBytes)
		switch key {
		case "limit":
			if p.Limit, err = fasthttp.ParseUint(valueBytes); err != nil || p.Limit < 1 {
				err = errorInvalidParam
				return
			}
		case "query_id":
			return // ignored

		case "order":
			if "1" == val {
				p.Order = 1
			} else if "-1" == val {
				p.Order = -1
			} else {
				err = errorInvalidParam
				return
			}
		case "keys":
			p.GroupBy = strings.Split(val, ",")
			for _, group := range p.GroupBy {
				switch group {
				case "sex":
					p.dataMask |= 1
				case "status":
					p.dataMask |= STATUS_MASK
				case "country":
					p.dataMask |= COUNTRY_MASK
				case "city":
					p.dataMask |= CITY_MASK
				case "interests":
					p.interestGroup = true
				default:
					err = errorInvalidParam
					return
				}
			}

		case "fname":
			p.FilterBy[key] = true
			if p.Filter.fname_eq, found = FnameDict.ids[val]; !found {
				p.Filter.ShortCircuit = true
				err = errorInvalidParam
				return
			}

		case "sname":
			p.FilterBy[key] = true
			if p.Filter.sname_eq, found = SnameDict.ids[val]; !found {
				p.Filter.ShortCircuit = true
				err = errorInvalidParam
				return
			}
		case "status":
			p.FilterBy[key] = true
			if p.Filter.status_eq = statusCode(val); p.Filter.status_eq == 0 {
				err = errorInvalidParam
				return
			}

		case "sex":
			p.FilterBy[key] = true
			if p.Filter.sex_eq = sexCode(val); p.Filter.sex_eq == 0 {
				err = errorInvalidParam
				return
			}
		case "city":
			p.FilterBy[key] = true
			if p.Filter.city_eq, found = CityDict.ids[val]; !found {
				p.Filter.ShortCircuit = true
				//return nil
			}
		case "country":
			p.FilterBy[key] = true
			if p.Filter.country_eq, found = CountryDict.ids[val]; !found {
				p.Filter.ShortCircuit = true
				//return nil
			}

		//для likes будет только один id, для interests только одна строка, для birth и joined - будет одно число - год).
		case "likes":
			p.FilterBy[key] = true
			p.likes = true
			if i, e := fasthttp.ParseUint(valueBytes); e == nil {
				if uint32(i) > Store.MaxId || i == 0 {
					p.Filter.ShortCircuit = true
					//return nil
				}
				p.Filter.likes_contains = []uint32{uint32(i)}
			} else {
				err = errorInvalidParam
				return
			}

		case "interests":
			p.FilterBy[key] = true
			if code, ok := InterestDict.ids[val]; ok && code > 0 {
				p.Filter.interests_any.Set(code)
				p.interestKeyCode = code
			} else {
				p.Filter.ShortCircuit = true
				//return nil
			}

		case "birth":
			p.FilterBy[key] = true
			if year, e := strconv.Atoi(val); e == nil {
				if year < 1950 || year > 2005 {
					p.Filter.ShortCircuit = true
					//return nil
				}
				p.Filter.birth_year = uint16(year)
			} else {
				err = errorInvalidParam
				return
			}

		case "joined":
			p.FilterBy[key] = true
			if year, e := strconv.Atoi(val); e == nil {
				if year < 2011 || year > 2018 {
					p.Filter.ShortCircuit = true
					//return nil
				}
				p.Filter.joined_year = uint16(year)
			} else {
				err = errorInvalidParam
				return
			}

		default:
			//err = fmt.Errorf("unknown param %s=%s", key, val) // что это за щщит?
			err = errorInvalidParam
			return
		}
	})

	if len(p.GroupBy) == 0 {
		return errorInvalidParam
	}

	p.Filter.updateCheckFlags()
	return err
}

/**
передается функция для учета ключа классификации
*/
func (g *Group) ClassifyAccount(id uint32, collect func(uint64)) {
	account := &Store.Accounts[id-1]
	if g.Filter.Test(id, account) {
		// todo - unroll всех или учет единичного интереса, может вернуть список ключей
		key := g.dataMask & account.data
		if g.interestGroup {
			if g.interestKeyCode == 0 {
				// + unroll interests0
				pos := (id - 1) * 2
				interest := Store.interests[pos] >> 1
				for i := byte(0); i < 63 && interest != 0; i++ {
					if interest&1 == 1 {
						collect(key | calcInterestKeyPart(i+1))
					}
					interest >>= 1
				}
				// + unroll interests1
				interest = Store.interests[pos+1]
				for i := byte(0); i < 64 && interest != 0; i++ {
					if interest&1 == 1 {
						collect(key | calcInterestKeyPart(i+64))
					}
					interest >>= 1
				}
				// todo ? надо ли считать "пустой интерес"
				// if account.interests0 == 0 && account.interests1 == 0 { }
				return
			} else {
				packInterest(&key, g.interestKeyCode)
			}
		}
		collect(key)
	}
}

func (p *Group) tails(counters map[uint64]int32, maxLimit int) ([]GroupItem, []GroupItem) {
	items := make([]GroupItem, len(counters))[:]
	var index = 0
	for k, c := range counters {
		items[index] = GroupItem{Key: k, Count: c}
		index++
	}

	less := func(i, j int) bool {
		if items[i].Count < items[j].Count {
			return true
		} else if items[i].Count == items[j].Count {
			// если одинаковые то сортируем по ключам группировки
			//return p.decodeWithCache(items[i].Key) < p.decodeWithCache(items[j].Key)
			for _, v := range p.GroupBy {
				if cmp := strings.Compare(decode(v, items[i].Key), decode(v, items[j].Key)); cmp == -1 {
					return true
				} else if cmp == 1 {
					return false
				}
			}
		}
		return false
	}
	sort.Slice(items[:], less)
	var head []GroupItem
	var tail []GroupItem
	if maxLimit < len(items) {
		head = append([]GroupItem{}, items[:maxLimit]...)
		tail = append([]GroupItem{}, items[len(items)-maxLimit:]...)
	} else {
		head = items
		tail = append([]GroupItem{}, head...)
	}
	for i, j := 0, len(tail)-1; i < j; i, j = i+1, j-1 {
		tail[i], tail[j] = tail[j], tail[i]
	}
	return head, tail
}

type GroupCacheKey struct {
	sex, status, interests, country, city byte // для сортировки важен порядок задания ключей

	order int8

	sex_eq       byte
	status_eq    byte
	country_eq   byte
	interests_eq byte
	city_eq      uint16
	birth_year   uint16
	joined_year  uint16
}

func (p *Group) getCachedGroup(key GroupCacheKey) []GroupItem {
	groupL2CacheMutex.RLock()
	if res, found := groupL2Cache[key]; found {
		groupL2CacheMutex.RUnlock()
		return res
	}
	groupL2CacheMutex.RUnlock()
	return nil
}

func (p *Group) toCachedItems(key GroupCacheKey, counters map[uint64]int32) (xs []GroupItem) {
	head, tail := p.tails(counters, 50)
	countersRelease(counters)
	if key.order == 1 {
		if p.Limit < len(head) {
			xs = head[:p.Limit]
		} else {
			xs = head
		}
		groupL2CacheMutex.Lock()
		groupL2Cache[key] = head
		key.order = 0 - key.order
		groupL2Cache[key] = tail
		groupL2CacheMutex.Unlock()
	} else {
		if p.Limit < len(tail) {
			xs = tail[:p.Limit]
		} else {
			xs = tail
		}
		groupL2CacheMutex.Lock()
		groupL2Cache[key] = tail
		key.order = 0 - key.order
		groupL2Cache[key] = head
		groupL2CacheMutex.Unlock()
	}
	return xs
}

func (p *Group) CacheKey() GroupCacheKey {
	key := GroupCacheKey{order: p.Order,
		sex_eq:       p.Filter.sex_eq,
		status_eq:    p.Filter.status_eq,
		country_eq:   p.Filter.country_eq,
		interests_eq: p.interestKeyCode,
		city_eq:      p.Filter.city_eq,
		birth_year:   p.Filter.birth_year,
		joined_year:  p.Filter.joined_year,
	}

	for i, s := range p.GroupBy {
		switch s {
		case "sex":
			key.sex = byte(i + 1)
		case "status":
			key.status = byte(i + 1)
		case "country":
			key.country = byte(i + 1)
		case "city":
			key.city = byte(i + 1)
		case "interests":
			key.interests = byte(i + 1)
		}
	}
	return key
}

func (p *Group) aggregate(updateCache bool) ([]GroupItem, bool) {
	key := p.CacheKey()
	if xs := p.getCachedGroup(key); xs != nil {
		return xs, true
	}
	for _, aggregate := range _aggregates {
		if counters, ok := aggregate.Aggregate(p); ok {
			if updateCache {
				return p.toCachedItems(key, counters), ok
			} else {
				return p.Sort(counters, p.Limit), ok
			}
		}
	}
	return nil, false
}

func (g *Group) Classify(accounts *[]Account, updateCache bool) []GroupItem {

	if !g.likes {
		if items, ok := g.aggregate(updateCache); ok {
			if len(items) <= g.Limit {
				return items
			} else {
				return items[:g.Limit]
			}
		}
		log.Printf("wtf?: %v\n", g)
	}

	//  brute force fallback, более-менее приемлем для индексированных фильтров
	counters := countersBorrow() // make(map[uint64]int32)

	collect := func(groupKey uint64) { counters[groupKey] += 1 }
	// идем в обычном порядке (хм, а так будет быстрее или пофиг?)
	if index, iter := g.Filter.Index(false); index != nil || iter != nil {
		if iter != nil {
			for id := iter.Next(); id != math.MaxUint32; id = iter.Next() {
				g.ClassifyAccount(id, collect)
			}
		} else {
			for _, id := range index {
				g.ClassifyAccount(id, collect)
			}
		}
	} else {
		log.Printf("orly?: %v\n", g)
		// жестокий харкор: тупо идем снизу и набираем данные пока не упремся в лимит, в финале сюда не должны попадать
		for id := uint32(0); id < Store.MaxId; id++ {
			g.ClassifyAccount(id+1, collect)
		}
	}
	// todo: likes не кешируем, а остальной fallback?
	return g.Sort(counters, g.Limit)
}

type GroupItem struct {
	Key   uint64
	Count int32
}

func (g *Group) Sort(counters map[uint64]int32, limit int) []GroupItem {
	items := make([]GroupItem, len(counters))[:]
	var index = 0
	for k, c := range counters {
		items[index] = GroupItem{k, c}
		index++
	}
	countersRelease(counters)

	less := func(i, j int) bool {
		if items[i].Count < items[j].Count {
			return true
		} else if items[i].Count == items[j].Count {
			// если одинаковые то сортируем по ключам группировки
			//return g.decodeWithCache(items[i].Key) < g.decodeWithCache(items[j].Key)
			for _, v := range g.GroupBy {
				if cmp := strings.Compare(decode(v, items[i].Key), decode(v, items[j].Key)); cmp == -1 {
					return true
				} else if cmp == 1 {
					return false
				}
			}
		}
		return false
	}

	if g.Order == 1 {
		sort.Slice(items[:], less)
	} else {
		sort.Slice(items[:], func(i, j int) bool { return less(j, i) })
	}

	if limit < len(items) {
		return items[:limit]
	}

	return items
}

func decode(field string, groupKey uint64) string {
	switch field {
	case "sex":
		return sexOf(unpackSex(groupKey))
	case "status":
		return statusOf(unpackStatus(groupKey))
	case "country":
		return CountryDict.values[unpackCountry(groupKey)]
	case "city":
		return CityDict.values[unpackCity(groupKey)]
	case "interests":
		return InterestDict.values[unpackInterest(groupKey)]
	default:
		return ""
	}
}

func (g *Group) WriteJsonGroupItemOut(out []byte, groupKey uint64, count int32) []byte {
	out = append(out, '{')
	for _, name := range g.GroupBy {
		if str := decode(name, groupKey); str != "" { //todo: ? unroll to switch
			out = append(out, '"')
			out = append(out, name...)
			out = append(out, "\":\""...)
			out = append(out, str...)
			out = append(out, "\","...)
		}
	}
	out = append(out, "\"count\":"...)
	out = fasthttp.AppendUint(out, int(count))
	out = append(out, '}')
	return out
}

func (g *Group) DebugWriteJsonGroupItem(w io.Writer, groupKey uint64, count int32) {
	_, _ = fmt.Fprintf(w, "{")
	for _, name := range g.GroupBy {
		if str := decode(name, groupKey); str != "" {
			_, _ = fmt.Fprintf(w, "\"%s\":%s,", name, str)
		}
	}
	_, _ = fmt.Fprintf(w, "\"count\":%d}", count)
}
