package hlc18

import (
	"fmt"
	"github.com/valyala/fasthttp"
	"math"
	"strconv"
	"strings"
)

type NullOp uint8

const (
	Null_empty NullOp = iota
	Null_not
	Null_yes
)

// todo: ? для полей data и единичных|eq условий подготовить маску и сравнивать все в один заход

type Filter struct {
	Limit int
	//QueryId int

	ShortCircuit       bool
	sex_eq             uint8             //  - соответствие конкретному полу - "m" или "f";
	email_domain       uint8             // - выбрать всех, чьи email-ы имеют указанный домен;
	email_lt           string            // - выбрать всех, чьи email-ы лексикографически раньше;
	email_gt           string            // -  то же, но лексикографически позже
	status_eq          uint8             //- соответствие конкретному статусу;
	status_neq         uint8             //- выбрать всех, чей статус не равен указанному;
	fname_eq           uint8             //  - соответствие конкретному имени;
	fname_any          map[uint8]string  // - соответствие любому имени из перечисленных через запятую;
	fname_null         NullOp            //- выбрать всех, у кого указано имя (если 0) или не указано (если 1);
	sname_eq           uint16            // - соответствие конкретной фамилии;
	sname_starts       map[uint16]string // - выбрать всех, чьи фамилии начинаются с переданного префикса;
	sname_null         NullOp            // - выбрать всех, у кого указана фамилия (если 0) или не указана (если 1);
	phone_code         uint16            // - выбрать всех, у кого в телефоне конкретный код (три цифры в скобках);
	phone_null         NullOp            // - аналогично остальным полям;
	country_eq         uint8             // - всех, кто живёт в конкретной стране;
	country_null       NullOp            // - аналогично;
	city_eq            uint16            // - всех, кто живёт в конкретном городе;
	city_any           map[uint16]string // - в любом из перечисленных через запятую городов;
	city_null          NullOp            // - аналогично;
	birth_lt           int32             // - выбрать всех, кто родился до указанной даты;
	birth_gt           int32             // - после указанной даты;
	birth_year         uint16            // - кто родился в указанном году;
	interests_contains Bitset128         // - выбрать всех, у кого есть все перечисленные интересы;
	interest_eq        byte
	interests_any      Bitset128 // - выбрать всех, у кого есть любой из перечисленных интересов;
	likes_contains     []uint32  // - выбрать всех, кто лайкал всех перечисленных пользователей (в значении - перечисленные через запятые id);
	premium_now        int32     // - все у кого есть премиум на текущую дату;
	premium_null       NullOp    // - аналогично остальным;

	joined_year uint16 // 2011-01-01 : 2018-01-01

	// список полей фильтра
	Fields map[string]bool

	check_city    bool
	check_country bool
	check_status  bool
	check_fname   bool
	check_sname   bool
	check_phone   bool

	Len int
}

func (f *Filter) updateCheckFlags() {
	f.check_city = f.city_eq != 0 || f.city_any != nil || f.city_null != Null_empty
	f.check_country = f.country_eq != 0 || f.country_null != Null_empty
	f.check_status = f.status_eq != 0 || f.status_neq != 0
	f.check_fname = f.fname_eq != 0 || f.fname_any != nil || f.fname_null != Null_empty
	f.check_sname = f.sname_eq != 0 || f.sname_starts != nil || f.sname_null != Null_empty
	f.check_phone = f.phone_code != 0 || f.phone_null != Null_empty
}

func (f *Filter) Test(id uint32, a *Account) bool {

	if f.ShortCircuit {
		//log.Printf("filter is a short circuited %v\n", f)
		return false
	}

	if f.sex_eq != 0 && f.sex_eq != a.getSex() {
		//fmt.Printf("\nbad sex\n")
		return false
	}
	if f.email_domain != 0 && f.email_domain != a.getDomain() {
		return false
	}
	if f.email_lt != "" && -1 != strings.Compare(Store.Accounts2[id-1].email, f.email_lt) {
		return false
	}
	if f.email_gt != "" && 1 != strings.Compare(Store.Accounts2[id-1].email, f.email_gt) {
		return false
	}

	if f.check_status {
		status := a.getStatus()
		if f.status_eq != 0 && f.status_eq != status {
			return false
		}
		if f.status_neq != 0 && f.status_neq == status {
			return false
		}
	}

	if f.check_fname {
		fname := a.getFname()
		if f.fname_eq != 0 && f.fname_eq != fname {
			return false
		}
		if f.fname_any != nil {
			if 0 == fname {
				return false
			}
			if _, ok := f.fname_any[fname]; !ok {
				return false
			}
		}
		if f.fname_null != Null_empty {
			if val := fname; !((f.fname_null == Null_not && val != 0) || (f.fname_null == Null_yes && val == 0)) {
				return false
			}
		}
	}

	if f.check_sname {
		sname := a.getSname()
		if f.sname_eq != 0 && f.sname_eq != sname {
			return false
		}
		if f.sname_starts != nil {
			if 0 == sname {
				return false
			}
			if _, ok := f.sname_starts[sname]; !ok {
				return false
			}
		}
		if f.sname_null != Null_empty {
			if !((f.sname_null == Null_not && sname != 0) || (f.sname_null == Null_yes && sname == 0)) {
				return false
			}
		}
	}

	if f.check_phone {
		phone_code := a.getPhoneCode()
		if f.phone_code != 0 && f.phone_code != phone_code {
			return false
		}
		if f.phone_null != Null_empty {
			if !((f.phone_null == Null_not && phone_code != 0) || (f.phone_null == Null_yes && phone_code == 0)) {
				return false
			}
		}
	}

	if f.check_country {
		country := a.getCountry()
		if f.country_eq != 0 && f.country_eq != country {
			return false
		}
		if f.country_null != Null_empty {
			if !((f.country_null == Null_not && country != 0) || (f.country_null == Null_yes && country == 0)) {
				return false
			}
		}
	}

	if f.check_city {
		city := a.getCity()
		if f.city_eq != 0 && f.city_eq != city {
			return false
		}
		if f.city_any != nil {
			if _, ok := f.city_any[city]; !ok {
				return false
			}
		}
		if f.city_null != Null_empty {
			if !((f.city_null == Null_not && city != 0) || (f.city_null == Null_yes && city == 0)) {
				return false
			}
		}
	}

	if f.birth_lt != math.MaxInt32 && f.birth_lt <= a.birth {
		return false
	}
	if f.birth_gt != math.MaxInt32 && f.birth_gt >= a.birth {
		return false
	}
	if f.birth_year != 0 && f.birth_year != a.getBirthYear() {
		return false
	}
	if f.interests_contains.IsNotEmpty() {
		i := (id - 1) * 2
		if !Bitset128Contains(&Store.interests[i], &Store.interests[i+1], &f.interests_contains) {
			return false
		}
	}
	if f.interests_any.IsNotEmpty() {
		i := (id - 1) * 2
		if !Bitset128Any(&Store.interests[i], &Store.interests[i+1], &f.interests_any) {
			return false
		}
	}

	if f.premium_now != 0 && a.IsPremium() == 0 {
		return false
	}
	if !nullOkTimestamp(f.premium_null, Store.Accounts2[id-1].premiumStart) {
		return false
	}
	if f.joined_year != 0 && f.joined_year != a.getJoinedYear() {
		return false
	}
	return true
}

func (p *Filter) likesIndex(sorted bool) []uint32 {

	// при первом проходе добавляем ключи
	// при втором проходе только подтверждаем (но ничего не добляем)

	var tmp map[uint32]int
	scansCount := 0
	if p.likes_contains != nil {
		tmp = make(map[uint32]int)
		for _, likeid := range p.likes_contains {
			if likeid <= Store.MaxId {
				for _, k := range LikesIndexCompact[likeid-1] {
					//addKeyToIndex(scansCount, tmp, k)
					if scansCount == 0 {
						tmp[k] = 1
					} else {
						if cnt, ok := tmp[k]; ok {
							tmp[k] = cnt + 1
						}
					}
				}
			}
			scansCount++
		}
		p.likes_contains = nil
	}
	if tmp != nil && len(tmp) == 0 {
		return []uint32{}
	} else if tmp != nil {
		// отбираем ключи и упаковываем в массив
		arrSize := 0
		for _, cnt := range tmp {
			if cnt == scansCount {
				arrSize++
			}
		}
		arr := make([]uint32, arrSize)
		var i = 0
		for k, cnt := range tmp {
			if cnt == scansCount {
				arr[i] = k
				i++
			}
		}

		if sorted {
			Uint32Slice(arr).Sort()
		}

		return arr
	}
	return nil

}

var emptyIndex = make([]uint32, 0, 0)

// todo: для групп сортировать не нужно ?
func (f *Filter) Index(sorted bool) ([]uint32, IndexIterator) {

	likesIndex := f.likesIndex(sorted)
	if likesIndex != nil && len(likesIndex) < 24 {
		return likesIndex, nil
	}

	//if (f.QueryId == 2159) {
	//	println(f.QueryId)
	//}

	if f.interests_contains.IsNotEmpty() {
		icount := f.interests_contains.Count()
		if icount > 1 {

			buffer := make([]byte, 16) // todo: ? sync.Pool
			array := interestsToArray(f.interests_contains[0], f.interests_contains[1], buffer)

			if icount == 2 {
				f.interests_contains[0] = 0
				f.interests_contains[1] = 0 // в Test() уже можно не проверять, trust me...
			}
			// todo: пока берем первый подвернувшийся, даже не паримся с поиском минимального или and итератором
			//for i, code := range interests {
			//	addRecommendKeys(id, code, account)
			//	InterestsIndexCompact[code] = append(InterestsIndexCompact[code], id)
			//	InterestsSexIndexCompact[code*2+sex] = append(InterestsSexIndexCompact[code*2+sex], id)
			//
			//	for j := i + 1; j < len(interests); j++ {

			// todo: стоит попариться наверное...
			index2x := Interests2xIndexCompact[int(array[0])*90+int(array[1])]

			for j := 2; j < len(array); j++ {
				idxTmp := Interests2xIndexCompact[int(array[0])*90+int(array[j])]
				if len(idxTmp) < len(index2x) {
					index2x = idxTmp
				}
			}

			if likesIndex != nil {
				var iter = makeIndexAndIterator()
				iter.push(likesIndex)
				iter.push(index2x)
				return nil, iter.Prepare()
			} else {
				return index2x, nil
			}
		}

		useNaive := 1 == f.Len
		useNaive = useNaive || (2 == f.Len && (f.sex_eq != 0 || f.premium_null != 0 || f.status_neq != 0))
		useNaive = useNaive || (3 == f.Len && (f.sex_eq != 0 && (f.status_eq != 0 || f.status_neq != 0 || f.premium_null != 0 || len(f.city_any) != 0)))

		if useNaive {
			f.interests_contains.Reset()
			sex := f.sex_eq
			if f.sex_eq != 0 {
				f.sex_eq = 0
			}
			var iter = makeIndexAndIterator()
			if sex != 0 {
				iter.push(InterestsSexIndexCompact[(f.interest_eq-1)*2+sex-1])
				//return , nil
			} else {
				iter.push(InterestsIndexCompact[f.interest_eq-1])
				//return , nil
			}
			if len(likesIndex) > 0 {
				iter.push(likesIndex)
			}
			if single := iter.ToSingle(); single != nil {
				return single, nil
			}
			return nil, iter.Prepare()
		}

		// если есть индекс по лайкам, сразу вернем его - пока не умеем кобинировать and-or индексы
		if likesIndex != nil {
			return likesIndex, nil
		}

		sexList := []byte{0, 1}
		premiumList := []byte{0, 1}
		statusList := []byte{0, 1, 2}
		if f.sex_eq != 0 {
			sexList = []byte{f.sex_eq - 1}
			f.sex_eq = 0
		}
		if f.premium_now != 0 {
			premiumList = []byte{1}
			f.premium_now = 0
		}
		if f.status_eq != 0 {
			statusList = []byte{f.status_eq - 1}
			f.status_eq = 0
		} else if f.status_neq != 0 {
			statusList = append([]byte{0, 1, 2}[:f.status_neq-1], []byte{0, 1, 2}[f.status_neq:]...)
			f.status_neq = 0
		}

		var shortest IndexIterator
		seen := 0
		for interest := byte(0); interest < 100 && seen < icount; interest++ { // todo: 90?! 100, Карл!
			if f.interests_contains.Get(interest + 1) {
				seen++
				var iter = makeIndexOrIterator()
				for _, sex := range sexList {
					for _, prem := range premiumList {
						for _, status := range statusList {
							key := recommendKey(sex, prem, status, interest)
							var index []uint32
							if f.country_null == Null_yes {
								index = RecommendIndexCountryCompact[recommendKeyCountry(key, 0)]
							} else if f.country_eq != 0 {
								index = RecommendIndexCountryCompact[recommendKeyCountry(key, f.country_eq)]
							} else if f.city_null == Null_yes {
								index = RecommendIndexCityCompact[recommendKeyCity(key, 0)]
							} else if f.city_eq != 0 {
								index = RecommendIndexCityCompact[recommendKeyCity(key, f.city_eq)]
							} else {
								index = RecommendIndexCompact[key]
							}
							iter.push(index)
						}
					}
				}
				if shortest == nil || iter.Len() < shortest.Len() {
					shortest = iter
				}
			}
		}
		if f.country_null == Null_yes {
			f.country_null = Null_empty
		}
		if f.country_eq != 0 {
			f.country_eq = 0
		}
		if f.city_null == Null_yes {
			f.city_null = Null_empty
		}
		if f.city_eq != 0 {
			f.city_eq = 0
		}

		if single := shortest.ToSingle(); len(single) == 1 {
			return single, nil
		}
		return nil, shortest
	}

	if likesIndex != nil {
		return likesIndex, nil
	}

	if city := f.city_eq; (city != 0 || f.city_null == Null_yes || len(f.city_any) == 1) && f.birth_year > 0 {
		i := city*BIRTH_YEARS + (f.birth_year - 1950)
		if city == 0 && len(f.city_any) == 1 {
			for code := range f.city_any {
				i = code*BIRTH_YEARS + (f.birth_year - 1950)
			}
		}
		f.city_eq = 0
		f.city_null = Null_empty
		f.city_any = nil
		f.birth_year = 0
		index := BirthYearCityIndexCompact[i]
		if len(index) == 0 {
			return emptyIndex, nil
		}
		return index, nil
	} else if city := f.city_eq; city != 0 || f.city_null == Null_yes || len(f.city_any) == 1 {
		index := CityIndexCompact[city]
		if city == 0 && len(f.city_any) == 1 {
			for city := range f.city_any {
				index = CityIndexCompact[city]
				//city++
			}
		}

		f.city_eq = 0
		f.city_null = Null_empty
		f.city_any = nil

		if f.phone_code != 0 {
			if f.sex_eq != 0 {
				tmp := CityPhoneCodeSexIndex[cityPhoneCodeSexKey(city, f.phone_code, f.sex_eq)]
				if len(tmp) < len(index) {
					f.phone_code = 0
					f.sex_eq = 0
					index = tmp
				}
			} else {
				iter := makeIndexOrIterator()
				iter.push(CityPhoneCodeSexIndex[cityPhoneCodeSexKey(city, f.phone_code, 1)])
				iter.push(CityPhoneCodeSexIndex[cityPhoneCodeSexKey(city, f.phone_code, 2)])
				if tmp := iter.ToSingle(); tmp != nil {
					if len(tmp) < len(index) {
						f.phone_code = 0
						index = tmp
					}
				} else {
					f.phone_code = 0
					return nil, iter
				}
			}
		}

		if len(index) == 0 {
			return emptyIndex, nil
		}
		return index, nil
	} else if len(f.city_any) > 1 && f.birth_year > 0 {

		iter := makeIndexOrIterator()
		for code := range f.city_any {
			iter.push(BirthYearCityIndexCompact[code*BIRTH_YEARS+(f.birth_year-1950)])
		}
		f.city_any = nil
		f.birth_year = 0
		if single := iter.ToSingle(); single != nil {
			return single, nil
		}
		return nil, iter
	} else if len(f.city_any) > 1 {

		if f.phone_code != 0 {
			iter := makeIndexOrIterator()
			if f.sex_eq != 0 {
				for city := range f.city_any {
					iter.push(CityPhoneCodeSexIndex[cityPhoneCodeSexKey(city, f.phone_code, f.sex_eq)])
				}
			} else {
				for city := range f.city_any {
					iter.push(CityPhoneCodeSexIndex[cityPhoneCodeSexKey(city, f.phone_code, 1)])
					iter.push(CityPhoneCodeSexIndex[cityPhoneCodeSexKey(city, f.phone_code, 2)])
				}
			}
			f.phone_code = 0
			f.sex_eq = 0
			if single := iter.ToSingle(); single != nil {
				return single, nil
			}
			return nil, iter
		}

		iter := makeIndexOrIterator()
		for code := range f.city_any {
			iter.push(CityIndexCompact[code])
		}
		if f.email_lt != "" {
			l := emailIndex(f.email_lt)
			if l <= 104 {
				iterator := makeIndexOrIterator()
				for i := 0; i <= l; i++ {
					iterator.push(EmailPrefixIndexCompact[i])
				}
				if iterator.Len() < iter.Len() {
					if index := iterator.ToSingle(); index != nil {
						return index, nil
					}
					return nil, iterator
				}
			}
		}
		f.city_any = nil
		return nil, iter
	} else if fname := f.fname_eq; fname != 0 || len(f.fname_any) == 1 || f.fname_null == Null_yes {
		if fname == 0 && len(f.fname_any) == 1 {
			for k := range f.fname_any {
				fname = k
			}
		}
		index := FnameIndexCompact[fname]
		f.fname_eq = 0
		f.fname_null = Null_empty
		f.fname_any = nil
		if len(index) == 0 {
			return emptyIndex, nil
		}
		return index, nil
	} else if len(f.fname_any) > 1 {
		iterator := makeIndexOrIterator()
		for code := range f.fname_any {
			iterator.push(FnameIndexCompact[code])
		}
		f.fname_any = nil
		return nil, iterator
	} else if f.birth_year != 0 {
		index := BirthYearIndexCompact[f.birth_year-1950]
		if len(index) == 0 {
			return emptyIndex, nil
		}
		f.birth_year = 0
		return index, nil

	} else if country := f.country_eq; country != 0 || f.country_null == Null_yes {

		if f.phone_code != 0 {
			if f.sex_eq != 0 {
				index := CountryPhoneCodeSexIndex[countryPhoneCodeSexKey(country, f.phone_code, f.sex_eq)]
				f.phone_code = 0
				f.sex_eq = 0
				f.country_eq = 0
				f.country_null = Null_empty
				if len(index) == 0 {
					return emptyIndex, nil
				}
				return index, nil

			} else {
				iter := makeIndexOrIterator()
				iter.push(CountryPhoneCodeSexIndex[countryPhoneCodeSexKey(country, f.phone_code, 1)])
				iter.push(CountryPhoneCodeSexIndex[countryPhoneCodeSexKey(country, f.phone_code, 2)])

				f.phone_code = 0
				f.sex_eq = 0
				f.country_eq = 0
				f.country_null = Null_empty

				if index := iter.ToSingle(); index != nil {
					return nil, iter
				} else {

					return nil, iter
				}
			}
		}

		index := CountryIndexCompact[country]

		if f.email_gt != "" {
			l := emailIndex(f.email_gt)
			if l >= 572 {
				iterator := makeIndexOrIterator()
				for i := l; i < len(EmailPrefixIndexCompact); i++ {
					iterator.push(EmailPrefixIndexCompact[i])
				}
				if iterator.Len() < len(index) {
					if index := iterator.ToSingle(); index != nil {
						return index, nil
					}
					return nil, iterator
				}
			}
		}

		f.country_eq = 0
		f.country_null = Null_empty
		if len(index) == 0 {
			return emptyIndex, nil
		}
		return index, nil
	}

	if f.email_lt != "" {
		l := emailIndex(f.email_lt)
		if l <= 104 {
			iterator := makeIndexOrIterator()
			for i := 0; i <= l; i++ {
				iterator.push(EmailPrefixIndexCompact[i])
			}
			if index := iterator.ToSingle(); index != nil {
				return index, nil
			}
			return nil, iterator
		}
	}

	if f.email_gt != "" {
		l := emailIndex(f.email_gt)
		if l >= 572 {
			iterator := makeIndexOrIterator()
			for i := l; i < len(EmailPrefixIndexCompact); i++ {
				iterator.push(EmailPrefixIndexCompact[i])
			}
			if index := iterator.ToSingle(); index != nil {
				return index, nil
			}
			return nil, iterator
		}
	}

	if status := f.status_eq; status != 0 {
		f.status_eq = 0
		return StatusIndexCompact[status-1], nil
	} else if sex := f.sex_eq; sex != 0 {
		f.sex_eq = 0
		return SexIndexCompact[sex-1], nil
	}

	return nil, nil

}

func (p *Filter) processItem(consumer func(bool, map[string]bool, *Account, uint32), separate *bool, id uint32, limit int) (int, bool) {
	account := &Store.Accounts[id-1]
	if ok := p.Test(id, account); ok {
		consumer(*separate, p.Fields, account, id)
		limit--
		*separate = true
	}
	return limit, true
}

func (p *Filter) Process(limit int, consumer func(bool, map[string]bool, *Account, uint32)) {
	separate := false
	cont := true

	if index, iter := p.Index(true); index != nil || iter != nil {
		// идем по индексу
		if iter != nil {
			for id := iter.Next(); id != math.MaxUint32 && limit > 0 && cont; id = iter.Next() {
				limit, cont = p.processItem(consumer, &separate, id, limit)
			}
		} else {
			for idxi := len(index) - 1; idxi >= 0 && limit > 0 && cont; idxi-- {
				limit, cont = p.processItem(consumer, &separate, index[idxi], limit)
			}
		}
	} else {
		// харкор: тупо идем снизу и набираем данные пока не упремся в лимит
		//log.Printf("filter brut-force (%d): %v\n",query_id, params)
		for id := Store.MaxId; id > 0 && limit > 0; id-- {
			limit, cont = p.processItem(consumer, &separate, id, limit)
		}
	}
}

// todo: eliminate Atoi?
func MakeFilterArgs(args *fasthttp.Args) (*Filter, error) {

	var f = &Filter{Fields: make(map[string]bool), birth_gt: math.MaxInt32, birth_lt: math.MaxInt32}
	var err error
	var found bool

	args.VisitAll(func(keyBytes, valueBytes []byte) {
		if err != nil || f.ShortCircuit {
			return
		}
		f.Len++
		val := B2s(valueBytes)
		key := B2s(keyBytes)
		//for key, val := range params {
		switch key {
		case "limit":
			if f.Limit, err = fasthttp.ParseUint(valueBytes); err != nil || f.Limit < 1 {
				err = errorInvalidParam
				return
			}
		case "query_id":
			// fixme - ignore in prod
			//f.QueryId, _ = fasthttp.ParseUint(valueBytes)
			return // ignored

		case "sex_eq": //uint8 //  - соответствие конкретному полу - "m" или "f";
			f.Fields["sex"] = true
			if f.sex_eq = sexCode(val); f.sex_eq == 0 {
				err = errorInvalidParam
			}
		case "email_domain": //uint8 // - выбрать всех, чьи email-ы имеют указанный домен;
			if f.email_domain, found = DomainDict.ids[val]; !found {
				f.ShortCircuit = true
			}

		case "email_lt": //string // - выбрать всех, чьи email-ы лексикографически раньше;
			f.email_lt = val

		case "email_gt": //string // -  то же, но лексикографически позже
			f.email_gt = val

		case "status_eq": //uint8 //- соответствие конкретному статусу;
			f.Fields["status"] = true
			if f.status_eq = statusCode(val); f.status_eq == 0 {
				err = errorInvalidParam
			}
		case "status_neq": //uint8 //- выбрать всех, чей статус не равен указанному;
			f.Fields["status"] = true
			if f.status_neq = statusCode(val); f.status_neq == 0 {
				err = errorInvalidParam
			}

		case "fname_eq": //uint8 //  - соответствие конкретному имени;
			f.Fields["fname"] = true
			if f.fname_eq, found = FnameDict.ids[val]; !found {
				f.ShortCircuit = true
			}

		case "fname_any": //[]uint8 // - соответствие любому имени из перечисленных через запятую;
			f.Fields["fname"] = true
			f.fname_any = codesOf8(val, FnameDict)

		case "fname_null": //int8 //- выбрать всех, у кого указано имя (если 0) или не указано (если 1);
			f.Fields["fname"] = true
			if f.fname_null, err = nullOp(val); err != nil {
				err = errorInvalidParam
			}

		case "sname_eq": //uint16 // - соответствие конкретной фамилии;
			f.Fields["sname"] = true
			if f.sname_eq, found = SnameDict.ids[val]; !found {
				f.ShortCircuit = true
			}

		case "sname_starts": //[]uint16 // - выбрать всех, чьи фамилии начинаются с переданного префикса;
			f.Fields["sname"] = true
			f.sname_starts = make(map[uint16]string)
			for sname, code := range SnameDict.ids {
				if strings.HasPrefix(sname, val) {
					f.sname_starts[code] = sname
				}
			}

		case "sname_null": //int8 // - выбрать всех, у кого указана фамилия (если 0) или не указана (если 1);
			f.Fields["sname"] = true
			if f.sname_null, err = nullOp(val); err != nil {
				err = errorInvalidParam
			}

		case "phone_code": //uint16 // - выбрать всех, у кого в телефоне конкретный код (три цифры в скобках);
			f.Fields["phone"] = true
			if code, e := strconv.Atoi(val); e == nil {
				f.phone_code = uint16(code)
			} else {
				err = errorInvalidParam
			}

		case "phone_null": //int8 // - аналогично остальным полям;
			f.Fields["phone"] = true
			if f.phone_null, err = nullOp(val); err != nil {
				err = errorInvalidParam
			}

		case "country_eq": //uint8 // - всех, кто живёт в конкретной стране;
			f.Fields["country"] = true
			if f.country_eq, found = CountryDict.ids[val]; !found {
				f.ShortCircuit = true
			}

		case "country_null": //int8 // - аналогично;
			f.Fields["country"] = true
			if f.country_null, err = nullOp(val); err != nil {
				err = errorInvalidParam
			}

		case "city_eq": //uint16 // - всех, кто живёт в конкретном городе;
			f.Fields["city"] = true
			if f.city_eq, found = CityDict.ids[val]; !found {
				f.ShortCircuit = true
			}

		case "city_any": //[]uint16 // - в любом из перечисленных через запятую городов;
			f.Fields["city"] = true
			f.city_any = codesOf16(val, CityDict)

		case "city_null": //int8 // - аналогично;
			f.Fields["city"] = true
			if f.city_null, err = nullOp(val); err != nil {
				err = errorInvalidParam
			}

		case "birth_lt": //uint32 // - выбрать всех, кто родился до указанной даты;
			f.Fields["birth"] = true
			if ts, e := strconv.Atoi(val); e == nil {
				f.birth_lt = int32(ts)
			} else {
				err = errorInvalidParam
			}
		case "birth_gt": //uint32 // - после указанной даты;
			f.Fields["birth"] = true
			if ts, e := strconv.Atoi(val); e == nil {
				f.birth_gt = int32(ts)
			} else {
				err = errorInvalidParam
			}
		case "birth_year": //uint16 // - кто родился в указанном году;
			f.Fields["birth"] = true
			if year, e := strconv.Atoi(val); e == nil {
				if year < 1950 || year > 2005 {
					f.ShortCircuit = true
				}
				f.birth_year = uint16(year)
			} else {
				err = errorInvalidParam
			}
		case "interests_contains": //[]uint8 // - выбрать всех, у кого есть все перечисленные интересы;
			toBitset(val, true, InterestDict, &f.interests_contains, &f.interest_eq)
			if f.interests_contains.IsEmpty() {
				f.ShortCircuit = true
			}

		case "interests_any": //[]uint8 //- выбрать всех, у кого есть любой из перечисленных интересов;
			toBitset(val, false, InterestDict, &f.interests_any, &f.interest_eq)
			if f.interests_any.IsEmpty() {
				f.ShortCircuit = true
			}

		case "likes_contains": //[]uint32 // - выбрать всех, кто лайкал всех перечисленных пользователей (в значении - перечисленные через запятые id);
			valArray := strings.Split(val, ",")
			f.likes_contains = make([]uint32, len(valArray))
			for i, v := range valArray {
				if atoi, err := strconv.Atoi(v); err == nil {
					f.likes_contains[i] = uint32(atoi)
				} else {
					err = errorInvalidParam
				}
			}

		case "premium_now": //uint32 // - все у кого есть премиум на текущую дату;
			f.Fields["premium"] = true
			f.premium_now = Store.Now

		case "premium_null": //int8 // - аналогично остальным;
			f.Fields["premium"] = true
			if f.premium_null, err = nullOp(val); err != nil {
				err = errorInvalidParam
			}

		default:
			err = errorInvalidParam
		}
	})

	if f.ShortCircuit {
		f.Limit = 1
		return f, nil
	}

	f.Len -= 2

	if err != nil {
		return nil, err
	}

	// проверить на имя-пол
	if f.sex_eq != 0 && len(f.fname_any) > 0 {
		for code := range f.fname_any {
			if 0 == SexNames[f.sex_eq-1][code] {
				//log.Printf("%s is not a %d\n", name, f.sex_eq )
				delete(f.fname_any, code)
			}
		}
		if len(f.fname_any) == 0 {
			f.fname_any = nil
			//log.Printf("ShortCircuit: fname_any/sex\n")
			f.ShortCircuit = true
			return f, err
		}
	}

	f.updateCheckFlags()

	return f, err
}

func MakeFilter(params map[string]string) (*Filter, error) {

	var f = &Filter{Fields: make(map[string]bool), birth_gt: math.MaxInt32, birth_lt: math.MaxInt32, Len: len(params)}
	var err error
	var found bool

	for key, val := range params {
		switch key {
		case "sex_eq": //uint8 //  - соответствие конкретному полу - "m" или "f";
			f.Fields["sex"] = true
			if f.sex_eq = sexCode(val); f.sex_eq == 0 {
				return nil, fmt.Errorf("invalid sex_eq=%s", val)
			}
		case "email_domain": //uint8 // - выбрать всех, чьи email-ы имеют указанный домен;
			if f.email_domain, found = DomainDict.ids[val]; !found {
				f.ShortCircuit = true
				return f, nil
			}

		case "email_lt": //string // - выбрать всех, чьи email-ы лексикографически раньше;
			f.email_lt = val

		case "email_gt": //string // -  то же, но лексикографически позже
			f.email_gt = val

		case "status_eq": //uint8 //- соответствие конкретному статусу;
			f.Fields["status"] = true
			if f.status_eq = statusCode(val); f.status_eq == 0 {
				return nil, fmt.Errorf("invalid status_eq=%s", val)
			}
		case "status_neq": //uint8 //- выбрать всех, чей статус не равен указанному;
			f.Fields["status"] = true
			if f.status_neq = statusCode(val); f.status_neq == 0 {
				return nil, fmt.Errorf("invalid status_neq=%s", val)
			}

		case "fname_eq": //uint8 //  - соответствие конкретному имени;
			f.Fields["fname"] = true
			if f.fname_eq, found = FnameDict.ids[val]; !found {
				f.ShortCircuit = true
				return f, nil
			}

		case "fname_any": //[]uint8 // - соответствие любому имени из перечисленных через запятую;
			f.Fields["fname"] = true
			f.fname_any = codesOf8(val, FnameDict)

		case "fname_null": //int8 //- выбрать всех, у кого указано имя (если 0) или не указано (если 1);
			f.Fields["fname"] = true
			if f.fname_null, err = nullOp(val); err != nil {
				return nil, err
			}

		case "sname_eq": //uint16 // - соответствие конкретной фамилии;
			f.Fields["sname"] = true
			if f.sname_eq, found = SnameDict.ids[val]; !found {
				f.ShortCircuit = true
				return f, nil
			}

		case "sname_starts": //[]uint16 // - выбрать всех, чьи фамилии начинаются с переданного префикса;
			f.Fields["sname"] = true
			f.sname_starts = make(map[uint16]string)
			for sname, code := range SnameDict.ids {
				if strings.HasPrefix(sname, val) {
					f.sname_starts[code] = sname
				}
			}

		case "sname_null": //int8 // - выбрать всех, у кого указана фамилия (если 0) или не указана (если 1);
			f.Fields["sname"] = true
			if f.sname_null, err = nullOp(val); err != nil {
				return nil, err
			}

		case "phone_code": //uint16 // - выбрать всех, у кого в телефоне конкретный код (три цифры в скобках);
			f.Fields["phone"] = true
			if code, e := strconv.Atoi(val); e == nil {
				f.phone_code = uint16(code)
			} else {
				return nil, fmt.Errorf("invalid pred %s value '%s'", key, val)
			}
			continue

		case "phone_null": //int8 // - аналогично остальным полям;
			f.Fields["phone"] = true
			if f.phone_null, err = nullOp(val); err != nil {
				return nil, err
			}

		case "country_eq": //uint8 // - всех, кто живёт в конкретной стране;
			f.Fields["country"] = true
			if f.country_eq, found = CountryDict.ids[val]; !found {
				f.ShortCircuit = true
				return f, nil
			}

		case "country_null": //int8 // - аналогично;
			f.Fields["country"] = true
			if f.country_null, err = nullOp(val); err != nil {
				return nil, err
			}

		case "city_eq": //uint16 // - всех, кто живёт в конкретном городе;
			f.Fields["city"] = true
			if f.city_eq, found = CityDict.ids[val]; !found {
				f.ShortCircuit = true
				return f, nil
			}

		case "city_any": //[]uint16 // - в любом из перечисленных через запятую городов;
			f.Fields["city"] = true
			f.city_any = codesOf16(val, CityDict)

		case "city_null": //int8 // - аналогично;
			f.Fields["city"] = true
			if f.city_null, err = nullOp(val); err != nil {
				return nil, err
			}

		case "birth_lt": //uint32 // - выбрать всех, кто родился до указанной даты;
			f.Fields["birth"] = true
			if ts, e := strconv.Atoi(val); e == nil {
				f.birth_lt = int32(ts)
			} else {
				return nil, fmt.Errorf("invalid pred %s value '%s'", key, val)
			}
		case "birth_gt": //uint32 // - после указанной даты;
			f.Fields["birth"] = true
			if ts, e := strconv.Atoi(val); e == nil {
				f.birth_gt = int32(ts)
			} else {
				return nil, fmt.Errorf("invalid pred %s value '%s'", key, val)
			}
		case "birth_year": //uint16 // - кто родился в указанном году;
			f.Fields["birth"] = true
			if year, e := strconv.Atoi(val); e == nil {
				if year < 1950 || year > 2005 {
					f.ShortCircuit = true
					return f, nil
				}
				f.birth_year = uint16(year)
			} else {
				return nil, fmt.Errorf("invalid pred %s value '%s'", key, val)
			}
		case "interests_contains": //[]uint8 // - выбрать всех, у кого есть все перечисленные интересы;
			toBitset(val, true, InterestDict, &f.interests_contains, &f.interest_eq)
			if f.interests_contains.IsEmpty() {
				f.ShortCircuit = true
				return f, nil
			}

		case "interests_any": //[]uint8 //- выбрать всех, у кого есть любой из перечисленных интересов;
			toBitset(val, false, InterestDict, &f.interests_any, &f.interest_eq)
			if f.interests_any.IsEmpty() {
				f.ShortCircuit = true
				return f, nil
			}

		case "likes_contains": //[]uint32 // - выбрать всех, кто лайкал всех перечисленных пользователей (в значении - перечисленные через запятые id);
			valArray := strings.Split(val, ",")
			f.likes_contains = make([]uint32, len(valArray))
			for i, v := range valArray {
				if atoi, err := strconv.Atoi(v); err == nil {
					f.likes_contains[i] = uint32(atoi)
				} else {
					return nil, fmt.Errorf("likes_contains invalid value %s", v)
				}
			}

		case "premium_now": //uint32 // - все у кого есть премиум на текущую дату;
			f.Fields["premium"] = true
			f.premium_now = Store.Now

		case "premium_null": //int8 // - аналогично остальным;
			f.Fields["premium"] = true
			if f.premium_null, err = nullOp(val); err != nil {
				return nil, err
			}

		default:
			return nil, fmt.Errorf("invalid param %s", key)
		}
	}

	// проверить на имя-пол
	if f.sex_eq != 0 && len(f.fname_any) > 0 {
		for code := range f.fname_any {
			if 0 == SexNames[f.sex_eq-1][code] {
				//log.Printf("%s is not a %d\n", name, f.sex_eq )
				delete(f.fname_any, code)
			}
		}
		if len(f.fname_any) == 0 {
			f.fname_any = nil
			//log.Printf("ShortCircuit: fname_any/sex\n")
			f.ShortCircuit = true
			return f, nil
		}
	}

	f.updateCheckFlags()

	return f, nil
}

func toBitset(values string, strict bool, dict Dict8, bitset *Bitset128, single *byte) {
	for _, v := range strings.Split(values, ",") {
		if code, ok := dict.ids[v]; ok && code > 0 {
			bitset.Set(code)
			*single = code
		} else if strict && code == 0 {
			bitset.Reset()
			*single = 0
		}
	}
}

func codesOf8(values string, dict Dict8) map[uint8]string {
	codes := make(map[uint8]string)
	for _, v := range strings.Split(values, ",") {
		if code, ok := dict.ids[v]; ok && code > 0 {
			codes[code] = v
		}
	}
	return codes
}

func codesOf16(values string, dict Dict16) map[uint16]string {
	codes := make(map[uint16]string)
	for _, v := range strings.Split(values, ",") {
		if code, ok := dict.ids[v]; ok && code > 0 {
			codes[code] = v
		}
	}
	return codes
}

func nullOkTimestamp(op NullOp, val int32) bool {
	return op == Null_empty || (op == Null_not && val != 0) || (op == Null_yes && val == 0)
}

func nullOp(val string) (NullOp, error) {
	if "1" == val {
		return Null_yes, nil
	} else if "0" == val {
		return Null_not, nil
	} else {
		return Null_empty, fmt.Errorf("invalid *null predicate value '%v'", val)
	}
}
