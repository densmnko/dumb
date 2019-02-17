package hlc18

import (
	"archive/zip"
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fastjson"
	"io"
	"log"
	"math"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"tidwall/evio"
	"time"
	"unsafe"
)

var (
	Store *Storage
	/*	MaxId uint32
		Now   int32
	*/

	FnameDict    = makeDict8()
	SnameDict    = makeDict16()
	DomainDict   = makeDict8()
	InterestDict = makeDict8()
	CityDict     = makeDict16()
	CountryDict  = makeDict8()

	EmailMap = make(map[string]uint32, 1340000)
	PhoneMap = make(map[string]uint32, 600000)
	//CityMap  = make(map[string]map[string]bool) // (city -> country:bool), строки это объекты или указатели?

	MinJoined           = timeStampOf("2011-01-01T00:00:00Z")
	MaxJoinedMinPremium = timeStampOf("2018-01-01T00:00:00Z")
	BirthMin            = timeStampOf("1950-01-01T00:00:00Z")
	BirthMax            = timeStampOf("2005-01-01T00:00:00Z")
)

/**


 */

var (
	STATUS_OFFSET = uint(1)
	STATUS_MASK   = mask(STATUS_OFFSET, 2)

	DOMAIN_OFFSET = uint(3)
	DOMAIN_MASK   = mask(DOMAIN_OFFSET, 4)

	COUNTRY_OFFSET = uint(7)
	COUNTRY_MASK   = mask(COUNTRY_OFFSET, 7)

	CITY_OFFSET = uint(14)
	CITY_MASK   = mask(CITY_OFFSET, 10)

	FNAME_OFFSET = uint(24)
	FNAME_MASK   = mask(FNAME_OFFSET, 7)

	SNAME_OFFSET = uint(31)
	SNAME_MASK   = mask(SNAME_OFFSET, 11)

	BIRTH_Y_OFFSET = uint(42)
	BIRTH_Y_MASK   = mask(BIRTH_Y_OFFSET, 6)

	PHONECODE_OFFSET = uint(48)
	PHONECODE_MASK   = mask(PHONECODE_OFFSET, 10)

	JOINED_Y_OFFSET = uint(58)
	JOINED_Y_MASK   = mask(JOINED_Y_OFFSET, 3)

	IS_PREMIUM_OFFSET = uint(61)
	IS_PREMIUM_MASK   = mask(IS_PREMIUM_OFFSET, 1)
)

type Storage struct {
	Accounts  []Account
	Accounts2 []Account2
	interests []uint64
	MaxId     uint32
	Now       int32
	RunType   int32
	Likes     [][]Like
}

func InitStore(cap int) *Storage {
	Store = &Storage{
		Accounts:  make([]Account, cap),
		Accounts2: make([]Account2, cap),
		interests: make([]uint64, cap*2),
		Likes:     make([][]Like, cap),

		MaxId:   0,
		Now:     0,
		RunType: 0,
	}
	return Store
}

//arrayAccounts = make([]hlc18.Account, CAPACITY)
//runType       int32

type Account struct {
	data uint64

	// sex[2] 			- 1 бит
	// status[3] 		- 2 бита
	// domains[16(13)]  - 4 бита
	// country[128(70)] - 7 бит
	// city[1024(612)]	- 10 бит
	// fname[128(108)] 	- 7 бит
	// sname[2048(1638)]- 11 бит
	// birthYear[64(56)]- 6 бит (1950-2005)
	// phoneCode[1024]  - 10 бит
	// joinedYear[8]    - 3 бита (2011-2018)
	// sexStatus uint8 // 0xf0 - пол, 0x0f status
	// domain     uint8
	// country    uint8
	// city       uint16
	// fname      uint8
	// sname      uint16
	// birthYear  uint16 // Ограничено снизу 01.01.1950 и сверху 01.01.2005-ым.
	// phoneCode  uint16
	// joinedYear uint16 // 2011-01-01 : 2018-01-01
	//

	birth  int32
	joined int32

	//interests0 uint64
	//interests1 uint64

}

type Account2 struct {
	premiumStart  int32 // минимум 2018-01-01
	premiumFinish int32 // минимум 2018-01-01

	//likesId []uint32
	//likesTs []uint32

	phone string // todo ? есть ли разница хранить ссылки или строки
	email string // todo ? есть ли разница хранить ссылки или строки

	// храним в слайсе попарно (id,ts)
	// updatedLikes *[]uint32 //*VectorUint32
}

func mask(start, len uint) uint64 {
	var result uint64
	for i := uint(0); i < len; i++ {
		result |= 1 << uint(start+i)
	}
	return result
}

type IdArray []byte

func makeIdArray(cap int) IdArray {
	return make(IdArray, cap*3)
}

func (p IdArray) Len() int {
	return len(p) / 3
}

func (p IdArray) Put(i int, v uint32) {
	pos := i * 3
	p[pos] = byte(v & 0xff)
	p[pos+1] = byte((v >> 8) & 0xff)
	p[pos+2] = byte((v >> 16) & 0xff)
}

func (p IdArray) Get(i int) uint32 {
	pos := i * 3
	return uint32(p[pos]) + (uint32(p[pos+1]) << 8) + (uint32(p[pos+2]) << 16)
}

func (p IdArray) Contains(value uint32) bool {
	for i := 0; i < p.Len(); i++ {
		if p.Get(i) == value {
			return true
		}
	}
	return false

}

// бит 1; 1 - m, 2 - f
func (a *Account) getSex() byte {
	return byte(1 + (a.data & 1))
}

func (a *Account) setSex(sex string) {
	switch sex {
	case "m":
		a.data &^= 1
	case "f":
		a.data |= 1
	default:
		log.Fatalf("invalid sex '%s'", sex)
	}
}

func sexOf(code byte) string {
	if 1 == code {
		return "m"
	} else if 2 == code {
		return "f"
	}
	panic("sexOf 0?")
	//return ""
}

func sexCode(status string) byte {
	switch status {
	case "m":
		return 1
	case "f":
		return 2
	default:
		return 0
	}
}

// биты 2-3, начало 1, длина 2
func (a *Account) getStatus() byte {
	return unpackStatus(a.data)
}

func unpackStatus(data uint64) byte {
	return 1 + byte((data&STATUS_MASK)>>STATUS_OFFSET)
}

// биты 2-3, начало 1, длина 2
func (a *Account) setStatus(code byte) {
	if code < 1 || code > 3 {
		log.Fatalf("invalid status '%d'", code)
	}
	a.data = (a.data & ^STATUS_MASK) | (uint64(code-1) << STATUS_OFFSET)
}

func statusCode(status string) byte {
	switch status {
	case "свободны":
		return 1
	case "всё сложно":
		return 2
	case "заняты":
		return 3
	default:
		return 0
	}
}

func statusOf(code byte) string {
	switch code & 0x0f {
	case 1:
		return "свободны"
	case 2:
		return "всё сложно"
	case 3:
		return "заняты"
	default:
		return ""
	}
}

func (a *Account) getDomain() byte {
	return byte((a.data & DOMAIN_MASK) >> DOMAIN_OFFSET)
}

func (a *Account) setDomain(code byte) {
	if code > 15 {
		log.Fatalf("invalid domain '%d'", code)
	}
	a.data = (a.data & ^DOMAIN_MASK) | (uint64(code) << DOMAIN_OFFSET)
}

func (a *Account) getCountry() byte {
	return unpackCountry(a.data)
}

func unpackCountry(data uint64) byte {
	return byte((data & COUNTRY_MASK) >> COUNTRY_OFFSET)
}

func (a *Account) setCountry(code byte) {
	if code > 127 {
		log.Fatalf("invalid country '%d'", code)
	}
	a.data = (a.data & ^COUNTRY_MASK) | (uint64(code) << COUNTRY_OFFSET)
}

func (a *Account) getFname() byte {
	return byte((a.data & FNAME_MASK) >> FNAME_OFFSET)
}

func (a *Account) setFname(code byte) {
	if code > 127 {
		log.Fatalf("invalid FNAME '%d'", code)
	}
	a.data = (a.data & ^FNAME_MASK) | (uint64(code) << FNAME_OFFSET)
}

func (a *Account) getCity() uint16 {
	return unpackCity(a.data)
}

func unpackCity(data uint64) uint16 {
	return uint16((data & CITY_MASK) >> CITY_OFFSET)
}

func (a *Account) setCity(code uint16) {
	if code > 1023 {
		log.Fatalf("invalid city '%d'", code)
	}
	a.data = (a.data & ^CITY_MASK) | (uint64(code) << CITY_OFFSET)
}

func (a *Account) getSname() uint16 {
	return uint16((a.data & SNAME_MASK) >> SNAME_OFFSET)
}

func (a *Account) setSname(code uint16) {
	if code > 2047 {
		log.Fatalf("invalid SNAME '%d'", code)
	}
	a.data = (a.data & ^SNAME_MASK) | (uint64(code) << SNAME_OFFSET)
}

func (a *Account) getPhoneCode() uint16 {
	return uint16((a.data & PHONECODE_MASK) >> PHONECODE_OFFSET)
}

func (a *Account) setPhoneCode(code uint16) {
	if code > 1023 {
		log.Fatalf("invalid PHONECODE '%d'", code)
	}
	a.data = (a.data & ^PHONECODE_MASK) | (uint64(code) << PHONECODE_OFFSET)
}

func (a *Account) getBirthYear() uint16 {
	return 1950 + uint16((a.data&BIRTH_Y_MASK)>>BIRTH_Y_OFFSET)
}

func (a *Account) setBirthYear(year uint16) {
	if year < 1950 || year > 2005 {
		log.Fatalf("invalid BIRTH_Y '%d'", year)
	}
	a.data = (a.data & ^BIRTH_Y_MASK) | (uint64(year-1950) << BIRTH_Y_OFFSET)
}

// 2011-2018
func (a *Account) getJoinedYear() uint16 {
	return 2011 + uint16((a.data&JOINED_Y_MASK)>>JOINED_Y_OFFSET)
}

func (a *Account) setJoinedYear(year uint16) {
	if year < 2011 || year > 2018 {
		log.Fatalf("invalid JoinedYear '%d'", year)
	}
	a.data = (a.data & ^JOINED_Y_MASK) | (uint64(year-2011) << JOINED_Y_OFFSET)
}

func (a *Account) IsEmpty() bool {
	return a.data == 0 // как минимум домен не нулевой должен быть в данных
}

func (a *Account) IsPremium() byte {
	return byte((a.data & IS_PREMIUM_MASK) >> IS_PREMIUM_OFFSET)
}

func (a *Account) setPremium(premium byte) {
	a.data = (a.data & ^IS_PREMIUM_MASK) | (uint64(premium) << IS_PREMIUM_OFFSET)
}

func (a *Account2) IsPremium(nowTimestamp int32) byte {
	if a.premiumFinish >= nowTimestamp && a.premiumStart <= nowTimestamp {
		return 1
	} else {
		return 0
	}
}

/*
func CompressJsonToAccount(id uint32, acc *Account, value *fastjson.Value) error {
	email := stringOf(value, "email")
	if email != "" {
		if domain, _ := domainOf(email); domain == 0 {
			return fmt.Errorf("invalid email: %v", email)
		} else {
			acc.setDomain(domain)
		}
		EmailMap[email] = id
		acc.email = email
	} else {
		acc.email = ""
		acc.setDomain(0)
	}
	phone := stringOf(value, "phone")
	if phone != "" {
		PhoneMap[phone] = id
		acc.phone = phone
		acc.setPhoneCode(phoneCodeOf(phone))
	} else {
		acc.phone = ""
		acc.setPhoneCode(0)
	}

	city := stringOf(value, "city")
	country := stringOf(value, "country")
	if city != "" {
		// разрешаем хранить пустую страну для города, но если найдем страну то сразу ее запомним
		if countrySeen, found := CityMap[city]; found {
			// если пришла пустая страна то не страшно, просто не меняем ее в нашей CityMap
			if country != "" && country != countrySeen {
				fmt.Printf("city '%s', countries do not match: known '%s' but got '%s'", city, countrySeen, country)
				log.Fatalf("city '%s', countries do not match: known '%s' but got '%s'", city, countrySeen, country)
			}
		} else {
			if country != "" {
				// города с пустыми странами не регистрируем
				CityMap[city] = country
				//fmt.Printf("%s -> %s\n", accJson.City, accJson.Country)
			}
		}
	}

	acc.setFname(FnameDict.put(stringOf(value, "fname")))
	acc.setSname(SnameDict.put(stringOf(value, "sname")))

	for _, interest := range value.GetArray("interests") {
		Bitset128Set(InterestDict.put(stringOfValue(interest)), &acc.interests0, &acc.interests1)
	}

	acc.setSex(stringOf(value, "sex"))
	acc.setStatus(statusCode(stringOf(value, "status")))

	premium := value.Get("premium")

	acc.premiumStart = int32(premium.GetInt("start"))
	acc.premiumFinish = int32(premium.GetInt("finish"))

	acc.birth = int32(value.GetInt("birth"))
	acc.setCity(CityDict.put(city))
	acc.setCountry(CountryDict.put(country))
	acc.joined = int32(value.GetInt("joined"))
	acc.setBirthYear(yearOf(acc.birth))
	acc.setJoinedYear(yearOf(acc.joined))

	likes := value.GetArray("likes")
	// todo : сделал индекс для лайков и пока непонятно зачем их вообще сортировать
	// todo : LikeSlice(accJson.Likes).Sort()
	// todo : acc.likes = accJson.Likes

	// indexing
	// AppendAccountLikesIndex(accJson.Id, acc)
	for _, likeValue := range likes {
		likeId := likeValue.GetInt("id")
		m := LikesIndex[likeId-1]
		if m == nil {
			m = makeVector(34)
			LikesIndex[likeId-1] = m
		}
		m.Push(id)
	}
	return nil
}


func stringOf(value *fastjson.Value, name string) string {
	get := value.Get(name)
	if get == nil {
		return ""
	}
	if unquote, err := strconv.Unquote(get.String()); err != nil {
		panic(err)
	} else {
		return unquote
	}
}

func stringOfValue(value *fastjson.Value) string {
	if value == nil {
		return ""
	}
	if unquote, err := strconv.Unquote(value.String()); err != nil {
		panic(err)
	} else {
		return unquote
	}
}
*/

func (p *Storage) CompressToAccount(accJson *AccountJson) error {

	acc2 := &p.Accounts2[accJson.Id-1]
	acc := &p.Accounts[accJson.Id-1]

	if accJson.Email != "" {
		EmailMap[accJson.Email] = accJson.Id
		acc2.email = accJson.Email
		if domain, _ := domainOf(accJson.Email); domain == 0 {
			return fmt.Errorf("invalid email: %v", accJson)
		} else {
			acc.setDomain(domain)
		}
	} else {
		acc2.email = ""
		acc.setDomain(0)
	}
	if accJson.Phone != "" {
		PhoneMap[accJson.Phone] = accJson.Id
		acc2.phone = accJson.Phone
		acc.setPhoneCode(phoneCodeOf(accJson.Phone))
	} else {
		acc2.phone = ""
		acc.setPhoneCode(0)
	}

	//if accJson.City != "" && accJson.Country != "" {
	//	if countryMap, ok := CityMap[accJson.City]; ok {
	//		countryMap[accJson.Country] = true
	//	} else {
	//		CityMap[accJson.City] = map[string]bool{accJson.Country: true}
	//	}
	//}

	nameCode := FnameDict.put(accJson.Fname)
	acc.setFname(nameCode)
	SexNames[sexCode(accJson.Sex)-1][nameCode] = 1

	acc.setSname(SnameDict.put(accJson.Sname))

	i := (accJson.Id - 1) * 2
	setInterests(accJson.Interests, &Store.interests[i], &Store.interests[i+1])

	acc.setSex(accJson.Sex)
	acc.setStatus(statusCode(accJson.Status))

	acc2.premiumStart = accJson.Premium.Start
	acc2.premiumFinish = accJson.Premium.Finish

	acc.setPremium(acc2.IsPremium(p.Now))

	acc.birth = accJson.Birth
	acc.setCity(CityDict.put(accJson.City))
	acc.setCountry(CountryDict.put(accJson.Country))
	acc.joined = accJson.Joined
	acc.setBirthYear(yearOf(accJson.Birth))
	acc.setJoinedYear(yearOf(accJson.Joined))

	if len(accJson.Likes) > 0 {
		// indexing
		AppendAccountLikesIndex(accJson.Id, accJson.Likes)
		// append to store
		if len(accJson.Likes) > 1 {
			accJson.Likes[0].Id += LIKE_MOD_FLAG
		}
		p.Likes[accJson.Id-1] = accJson.Likes
	}
	return nil
}

/*
func (p *Storage) CompressToAccountDirect(value *fastjson.Value) (uint32, error) {

	id := uint32(value.GetUint("id"))

	acc2 := &p.Accounts2[id-1]
	acc := &p.Accounts[id-1]

	if email := safeString(value.Get("email")); email != "" {
		EmailMap[email] = id
		acc2.email = email
		if domain, _ := domainOf(email); domain == 0 {
			return 0, fmt.Errorf("invalid email: %v", value)
		} else {
			acc.setDomain(domain)
		}
	} else {
		acc2.email = ""
		acc.setDomain(0)
	}

	if phone := safeString(value.Get("phone")); phone != "" {
		PhoneMap[phone] = id
		acc2.phone = phone
		acc.setPhoneCode(phoneCodeOf(phone))
	} else {
		acc2.phone = ""
		acc.setPhoneCode(0)
	}


	//   todo !!!
	//   if accJson.City != "" && accJson.Country != "" {
	//   		if countryMap, ok := CityMap[accJson.City]; !ok {
	//   			countryMap[accJson.Country] = true
	//   		} else {
	//   			CityMap[accJson.City] = map[string]bool{accJson.Country: true}
	//   		}
	//   	}
	//
	//	if city := toString(value.Get("city")); city != "" {
	//		acc.setCity(CityDict.put(city))
	//		// разрешаем хранить пустую страну для города, но если найдем страну то сразу ее запомним
	//		if country, found := CityMap[city]; found {
	//			// если пришла пустая страна то не страшно, просто не меняем ее в нашей CityMap
	//			if countryValue := toString(value.Get("country")); countryValue != "" && countryValue != country {
	//				acc.setCountry(CountryDict.put(countryValue))
	//				log.Fatalf("city '%s', countries do not match: known '%s' but got '%s'", city, country, countryValue)
	//			}
	//		} else {
	//			if countryValue := toString(value.Get("country")); countryValue != "" {
	//				acc.setCountry(CountryDict.put(countryValue))
	//				// города с пустыми странами не регистрируем
	//				CityMap[city] = countryValue
	//			}
	//		}
	//	}
	//

	sex := safeString(value.Get("sex"))
	nameCode := FnameDict.put(toString(value.Get("fname")))
	acc.setFname(nameCode)
	SexNames[sexCode(sex)-1][nameCode] = 1

	acc.setSname(SnameDict.put(toString(value.Get("sname"))))

	i := (id - 1) * 2
	setInterestsDirect(value.GetArray("interests"), &Store.interests[i], &Store.interests[i+1])

	acc.setSex(sex)
	acc.setStatus(statusCode(toString(value.Get("status"))))

	if premium := value.Get("premium"); premium != nil {
		acc2.premiumStart = int32(premium.GetInt("start"))
		acc2.premiumFinish = int32(premium.GetInt("finish"))
	}

	acc.setPremium(acc2.IsPremium(p.Now))

	acc.birth = int32(value.GetInt("birth"))
	acc.joined = int32(value.GetInt("joined"))
	acc.setBirthYear(yearOf(acc.birth))
	acc.setJoinedYear(yearOf(acc.joined))

	// сделал индекс для лайков и пока непонятно зачем их вообще сортировать
	//LikeSlice(value.Likes).Sort()
	if likes := value.GetArray("likes"); len(likes) != 0 {
		xs := make([]Like, len(likes))
		for i, like := range likes {
			xs[i] = Like{uint32(like.GetUint("id")), uint32(like.GetUint("ts"))}
		}
		//LikeSlice(xs).Sort()
		p.Likes[id-1] = xs
		AppendAccountLikesIndex(id, xs)
	}

	return id, nil
}
*/

func (p *Storage) CompressUpdateToAccount(accJson *AccountJson) error {

	acc2 := &p.Accounts2[accJson.Id-1]
	acc := &p.Accounts[accJson.Id-1]

	if accJson.Email != "" {
		if acc.getDomain() != 0 {
			delete(EmailMap, acc2.email)
		}
		EmailMap[accJson.Email] = accJson.Id
		acc2.email = accJson.Email
		if domain, _ := domainOf(accJson.Email); domain == 0 {
			return fmt.Errorf("invalid email: %v", accJson)
		} else {
			acc.setDomain(domain)
		}
	}
	if accJson.Phone != "" {
		if acc2.phone != "" {
			delete(PhoneMap, acc2.phone)
		}
		PhoneMap[accJson.Phone] = accJson.Id
		acc2.phone = accJson.Phone
		acc.setPhoneCode(phoneCodeOf(accJson.Phone))
	}
	if "" != accJson.Sname {
		acc.setSname(SnameDict.put(accJson.Sname))
	}
	if "" != accJson.Status {
		acc.setStatus(statusCode(accJson.Status))
	}
	if "" != accJson.Sex {
		acc.setSex(accJson.Sex)
	}
	if "" != accJson.Fname {
		acc.setFname(FnameDict.put(accJson.Fname))
	}

	if "" != accJson.Sex || "" != accJson.Fname {
		SexNames[acc.getSex()-1][acc.getFname()] = 1
	}

	if accJson.Interests != nil {
		i := (accJson.Id - 1) * 2
		setInterests(accJson.Interests, &Store.interests[i], &Store.interests[i+1])
	}
	if accJson.Premium.Start != 0 {
		acc2.premiumStart = accJson.Premium.Start
	}
	if accJson.Premium.Finish != 0 {
		acc2.premiumFinish = accJson.Premium.Finish
	}

	if accJson.Premium.Start != 0 || accJson.Premium.Finish != 0 {
		acc.setPremium(acc2.IsPremium(p.Now))
	}

	if "" != accJson.City {
		acc.setCity(CityDict.put(accJson.City))
	}
	if "" != accJson.Country {
		acc.setCountry(CountryDict.put(accJson.Country))
	}
	if accJson.Birth != math.MaxInt32 {
		acc.birth = accJson.Birth
		acc.setBirthYear(yearOf(accJson.Birth))
	}
	if accJson.Joined != 0 {
		acc.joined = accJson.Joined
		acc.setJoinedYear(yearOf(accJson.Joined))
	}

	return nil
}

type LikeSlice []Like

func (p *LikeSlice) Len() int           { return len(*p) }
func (p *LikeSlice) Less(i, j int) bool { return (*p)[i].Id < (*p)[j].Id }
func (p *LikeSlice) Swap(i, j int)      { (*p)[i], (*p)[j] = (*p)[j], (*p)[i] }
func (p *LikeSlice) Sort()              { sort.Sort(p) }

func SearchLikes(a []Like, id uint32) int {
	return sort.Search(len(a), func(i int) bool { return a[i].Id >= id })
}

func phoneCodeOf(s string) uint16 {
	if s != "" {
		split1 := strings.Split(s, "(")
		if len(split1) > 1 {
			split2 := strings.Split(split1[1], ")")
			if len(split2) > 1 {
				if code, err := strconv.Atoi(split2[0]); err == nil {
					return uint16(code)
				}
			}
		}
	}
	return 0
}

func yearOf(ts int32) uint16 {
	return uint16(time.Unix(int64(ts), 0).UTC().Year())
}

type ByteSlice []byte

func (p ByteSlice) Len() int           { return len(p) }
func (p ByteSlice) Less(i, j int) bool { return p[i] < p[j] }
func (p ByteSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p ByteSlice) Sort()              { sort.Sort(p) }

func SearchBytes(a []byte, x byte) int {
	return sort.Search(len(a), func(i int) bool { return a[i] >= x })
}

type Uint32Slice []uint32

func (p Uint32Slice) Len() int           { return len(p) }
func (p Uint32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Uint32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p Uint32Slice) Sort()              { sort.Sort(p) }

/*func (p Uint32Slice) Partition(i int) (left Interface, right Interface) {
	return Uint32Slice(p[:i]), Uint32Slice(p[i+1:])
}
*/

func SearchUint32(a []uint32, x uint32) int {
	return sort.Search(len(a), func(i int) bool { return a[i] >= x })
}

/*
func bitsetInterests(interests []string) (bitset Bitset128) {
	for _, value := range interests {
		bitset.Set(InterestDict.put(value))
	}
	return bitset
}
*/

func setInterests(interests []string, t0, t1 *uint64) {
	*t0 = 0
	*t1 = 0
	for _, value := range interests {
		Bitset128Set(InterestDict.put(value), t0, t1)
	}
}

func setInterestsDirect(interests []*fastjson.Value, t0, t1 *uint64) {
	*t0 = 0
	*t1 = 0
	for _, value := range interests {
		Bitset128Set(InterestDict.put(toString(value)), t0, t1)
	}
}

type Dict16 struct {
	//values map[uint16]string // todo: use array instead ?
	values []string
	ids    map[string]uint16
}

type Dict8 struct {
	//values map[uint8]string // todo: use array instead ?
	values []string
	ids    map[string]uint8
}

func (m *Dict8) put(val string) uint8 {

	if "" == val {
		return 0
	}

	if id, present := m.ids[val]; present {
		return id
	}
	id := uint8(len(m.ids)) + 1
	m.ids[val] = id

	m.values = append(m.values, val)

	return id
}

func (m *Dict16) put(val string) uint16 {
	if "" == val {
		return 0
	}
	if id, present := m.ids[val]; present {
		return id
	}
	id := uint16(len(m.ids)) + 1
	m.ids[val] = id

	m.values = append(m.values, val)
	//m.values[id] = val
	return id
}

func (m *Dict8) Len() int  { return len(m.values) }
func (m *Dict16) Len() int { return len(m.values) }

/*type Dict32 struct {
	values map[uint32]string // todo: use array|VectorUint32 instead ?
	ids map[string]uint32
}

func (m* Dict32) put(val string) uint32 {
	if "" == val {
		return 0
	}
	if id, present  := m.ids[val]; present {
		return id
	}
	id := uint32(len(m.ids)) + 1
	m.ids[val] = id
	m.values[id] = val
	return id
}

func (m* Dict32) putKeyed(val string, key uint32) uint32 {
	if "" == val {
		return -1
	}
	if _, present  := m.ids[val]; present {
		delete(m.ids,val)
	}
	if _, present  := m.values[key]; present {
		delete(m.values,key)
	}
	//id := uint32(len(m.ids))
	m.ids[val] = key
	m.values[key] = val
	return key
}
func makeDict32() Dict32 {
	return Dict32 {
		make(map[uint32]string),
		make(map[string]uint32) }
}
*/

func makeDict8() Dict8 {
	return Dict8{
		make([]string, 1, 256),
		make(map[string]uint8)}
}

func makeDict16() Dict16 {
	return Dict16{
		make([]string, 1, 4096),
		make(map[string]uint16)}
}

/*
func (p *Storage) LoadDataJsonFastBlamed(dir string) error {

	if p.Now, p.RunType = LoadOptions(dir); Store.Now == 0 {
		return fmt.Errorf("error load %soptions.txt", dir)
	}

	r, err := zip.OpenReader(dir + "data.zip")
	if err != nil {
		return err
	}
	defer r.Close()

	var parser fastjson.Parser
	var bufferBytes [20 * 1024 * 1024]byte
	var fileCounter = 0
	var id uint32
	fmt.Printf("%v\tloading\n", Timenow())
	for _, f := range r.File {
		rc, err := f.Open()
		if err != nil {
			return err
		}
		buffer, err := read(rc, bufferBytes[:])
		if err != nil {
			return err
		}
		value, err := parser.ParseBytes(buffer)
		if err != nil {
			return err
		}
		for _, aValue := range value.GetArray("accounts") {
			if id, err = p.CompressToAccountDirect(aValue); err != nil {
				return err
			}
			if p.MaxId < id {
				p.MaxId = id
			}
		}
		_ = rc.Close()
		fileCounter++
		if fileCounter%16 == 0 {
			runtime.GC()
		}
	}
	runtime.GC()
	PrintMemUsage("LoadDataJsonFastBlamed done")
	p.Rebuild(true)
	return nil
}
*/

func (p *Storage) LoadData(dir string) error {

	if p.Now, p.RunType = LoadOptions(dir); Store.Now == 0 {
		return fmt.Errorf("error load %soptions.txt", dir)
	}

	r, err := zip.OpenReader(dir + "data.zip")
	if err != nil {
		return err
	}
	defer r.Close()

	var fileCounter = 0
	// Iterate through the files in the archive,
	// printing some of their contents.
	fmt.Printf("%v\tloading '%s'\n", Timenow(), dir)
	for _, f := range r.File {
		rc, err := f.Open()
		if err != nil {
			return err
		}
		dec := json.NewDecoder(rc)
		// json.Delim: {
		_, err = dec.Token()
		if err != nil {
			return err
		}
		//string: accounts
		_, err = dec.Token()
		if err != nil {
			return err
		}
		//json.Delim: [
		_, err = dec.Token()
		if err != nil {
			return err
		}
		//fmt.Printf("%T: %v\n", t, t)

		// while the array contains values
		for dec.More() {
			var accountJson = AccountJson{Birth: math.MaxInt32}
			// decode an array value (Message)
			err = dec.Decode(&accountJson)
			if err != nil {
				return err
			}
			if p.MaxId < accountJson.Id {
				p.MaxId = accountJson.Id
			}
			if err = p.CompressToAccount(&accountJson); err != nil {
				return err
			}
		}
		if err != nil {
			return err
		}
		rc.Close()
		fileCounter++

		if fileCounter%8 == 0 {
			//PrintMemUsage()
			runtime.GC()
			//PrintMemUsage()
		}
	}
	runtime.GC()
	PrintMemUsage("LoadData done")

	p.Rebuild(false)

	return nil
}

func domainOf(email string) (code uint8, err error) {
	if index := strings.LastIndex(email, "@"); index < 1 || index >= len(email)-1 {
		return 0, fmt.Errorf("invalid email %s", email)
	} else {
		return DomainDict.put(email[index+1:]), nil
	}
}

func timeStampOf(date string) int32 {
	t, _ := time.Parse(time.RFC3339, date)
	return int32(t.UTC().Unix())
}

func writeJsonIntOut(buffer []byte, prefix string, value int) []byte {
	buffer = append(buffer, prefix...)
	buffer = fasthttp.AppendUint(buffer, value)
	return buffer
}

func writeJsonStringOut(out []byte, prefix, value string) []byte {
	out = append(out, prefix...)
	out = append(out, value...)
	out = append(out, '"')
	return out
}

//var recommendFields = map[string]bool{"status": true, "fname": true, "sname": true, "birth": true, "premium": true}
func WriteAccountOutRecommend(out []byte, separate bool, account *Account, accountId uint32) []byte {
	if separate {
		out = append(out, ',')
	}
	out = writeJsonIntOut(out, "{\"id\":", int(accountId))
	out = writeJsonStringOut(out, ",\"status\":\"", statusOf(account.getStatus()))
	out = writeJsonIntOut(out, ",\"birth\":", int(account.birth))
	if account.getSname() != 0 {
		out = writeJsonStringOut(out, ",\"sname\":\"", SnameDict.values[account.getSname()])
	}
	if account.getFname() != 0 {
		out = writeJsonStringOut(out, ",\"fname\":\"", FnameDict.values[account.getFname()])
	}

	acc2 := &Store.Accounts2[accountId-1]
	out = writeJsonStringOut(out, ",\"email\":\"", acc2.email)
	if acc2.premiumFinish != 0 || acc2.premiumStart != 0 {
		out = writeJsonIntOut(out, ",\"premium\":{\"finish\":", int(acc2.premiumFinish))
		out = writeJsonIntOut(out, ",\"start\":", int(acc2.premiumStart))
		out = append(out, '}')
	}

	out = append(out, '}')
	return out
}

//var suggestFields = map[string]bool{"status": true, "fname": true, "sname": true }
func WriteAccountOutSuggest(out []byte, separate bool, account *Account, accountId uint32) []byte {
	if separate {
		out = append(out, ',')
	}
	out = writeJsonIntOut(out, "{\"id\":", int(accountId))
	out = writeJsonStringOut(out, ",\"status\":\"", statusOf(account.getStatus()))
	if account.getSname() != 0 {
		out = writeJsonStringOut(out, ",\"sname\":\"", SnameDict.values[account.getSname()])
	}
	if account.getFname() != 0 {
		out = writeJsonStringOut(out, ",\"fname\":\"", FnameDict.values[account.getFname()])
	}

	acc2 := &Store.Accounts2[accountId-1]
	out = writeJsonStringOut(out, ",\"email\":\"", acc2.email)

	out = append(out, '}')
	return out
}

/**
отдавать нужно  id, email и поля из запроса (кроме interests и likes)
*/
func WriteAccountOut(out []byte, separate bool, fields map[string]bool, account *Account, accountId uint32) []byte {
	if separate {
		out = append(out, ',')
	}
	out = writeJsonIntOut(out, "{\"id\":", int(accountId))

	var premium, phone bool

	for field, _ := range fields {
		switch field {
		case "sex":
			out = writeJsonStringOut(out, ",\"sex\":\"", sexOf(account.getSex()))
		case "sname":
			if account.getSname() != 0 {
				out = writeJsonStringOut(out, ",\"sname\":\"", SnameDict.values[account.getSname()])
			}
		case "fname":
			if account.getFname() != 0 {
				out = writeJsonStringOut(out, ",\"fname\":\"", FnameDict.values[account.getFname()])
			}
		case "city":
			if account.getCity() != 0 {
				out = writeJsonStringOut(out, ",\"city\":\"", CityDict.values[account.getCity()])
			}
		case "country":
			if account.getCountry() != 0 {
				out = writeJsonStringOut(out, ",\"country\":\"", CountryDict.values[account.getCountry()])
			}
		case "status":
			out = writeJsonStringOut(out, ",\"status\":\"", statusOf(account.getStatus()))
		case "birth":
			out = writeJsonIntOut(out, ",\"birth\":", int(account.birth))
		case "premium":
			premium = true
		case "phone":
			phone = true
		default:
			log.Printf("todo write '%s'\n", field)
		}
	}

	acc2 := &Store.Accounts2[accountId-1]
	out = writeJsonStringOut(out, ",\"email\":\"", acc2.email)
	if premium {
		if acc2.premiumFinish != 0 || acc2.premiumStart != 0 {
			out = writeJsonIntOut(out, ",\"premium\":{\"finish\":", int(acc2.premiumFinish))
			out = writeJsonIntOut(out, ",\"start\":", int(acc2.premiumStart))
			out = append(out, '}')
		}
	}
	if phone {
		if "" != acc2.phone {
			out = writeJsonStringOut(out, ",\"phone\":\"", acc2.phone)
		}
	}

	out = append(out, '}')
	return out
}

// контроли
// email - адрес электронной почты пользователя. Тип - unicode-строка длиной до 100 символов. Гарантируется уникальность.
// phone - номер мобильного телефона. Тип - unicode-строка длиной до 16 символов. Поле является опциональным, но для указанных значений гарантируется уникальность. Заполняется довольно редко.
// ? birth - дата рождения, записанная как число секунд от начала UNIX-эпохи по UTC (другими словами - это timestamp). Ограничено снизу 01.01.1950 и сверху 01.01.2005-ым.
// ? premium - начало и конец премиального периода в системе (когда пользователям очень
// хотелось найти "вторую половинку" и они делали денежный вклад). В json это поле представлено
// 	вложенным объектом с полями start и finish,
// где записаны timestamp-ы с нижней границей 01.01.2018.
// ? city - город проживания. Тип - unicode-строка длиной до 50 символов. Поле опционально и указывается редко. Каждый город расположен в определённой стране.
// joined - . Тип - timestamp с ограничениями: снизу 01.01.2011, сверху 01.01.2018.

//likes - массив известных симпатий пользователя, возможно пустой. Все симпатии идут вразнобой и каждая представляет собой объект из следующих полей:
// 		id - идентификатор другого аккаунта, к которому симпатия. Аккаунт по id в исходных данных всегда существует. В данных может быть несколько лайков с одним и тем же id.
var validateNewError = errors.New("ValidateNew error")

func ValidateNew(accountJson *AccountJson) error {

	if accountJson.Phone != "" {
		if _, found := PhoneMap[accountJson.Phone]; found {
			return validateNewError
		}
	}
	if accountJson.Email != "" {
		if _, found := EmailMap[accountJson.Email]; found {
			//log.Printf("invalid email: %s", accountJson.Email)
			return validateNewError
		}
		if code, err := domainOf(accountJson.Email); code == 0 || err != nil {
			//log.Printf("invalid email: %s", accountJson.Email)
			return validateNewError
		}
	} else {
		// обязятельное поле?
		//log.Printf("new invalid email: %s", accountJson.Email)
		return validateNewError
	}

	// todo : remove this shit
	if accountJson.Joined < MinJoined || accountJson.Joined > MaxJoinedMinPremium {
		//log.Printf("new invalid Joined: %v", accountJson.Joined)
		return validateNewError
	}
	// todo : remove this shit
	if accountJson.Birth == math.MaxInt32 || accountJson.Birth < BirthMin || accountJson.Birth > BirthMax {
		//log.Printf("new invalid Birth: %v", accountJson.Birth)
		return validateNewError
	}

	if accountJson.Premium.Start != 0 && (accountJson.Premium.Start < MaxJoinedMinPremium /*|| accountJson.Premium.Start > now*/) {
		//log.Printf("new invalid Premium.Start: %v", accountJson.Premium.Start)
		return validateNewError
	}
	if accountJson.Premium.Finish != 0 && (accountJson.Premium.Finish < MaxJoinedMinPremium /*|| accountJson.Premium.Finish > now*/) {
		//log.Printf("new invalid Premium.Finish: %v", accountJson.Premium.Finish)
		return validateNewError
	}

	if accountJson.Premium.Start != 0 && (accountJson.Premium.Start > accountJson.Premium.Finish) {
		//log.Printf("new invalid accountJson.Premium.Start > accountJson.Premium.Finish: %v > %v", accountJson.Premium.Start, accountJson.Premium.Finish)
		return validateNewError
	}
	if sexCode(accountJson.Sex) == 0 || statusCode(accountJson.Status) == 0 {
		//log.Print("new invalid sex of status")
		return validateNewError
	}

	for _, like := range accountJson.Likes {
		if like.Id < 1 || like.Id > Store.MaxId {
			return validateNewError
		}
	}

	for _, like := range accountJson.Likes {
		if like.Ts < uint32(MinJoined) || like.Ts > uint32(Store.Now) {
			return validateNewError
		}
	}

	/*	if accountJson.City != "" && accountJson.Country != "" {
			if country, found := CityMap[accountJson.City]; found && country!="" && country!=accountJson.Country {
				mutate.Unlock()
				log.Printf("invalid city->country: %s -> %s, valid country is %s", accountJson.City, accountJson.Country, country)
				ctx.SetStatusCode(400)
				return
			}
		}
	*/

	return nil
}

var validateUpdateError = errors.New("ValidateNew error")

func ValidateUpdate(accountJson *AccountJson, id uint32) error {

	if accountJson.Phone != "" {
		if pid, found := PhoneMap[accountJson.Phone]; found && pid != id {
			return validateUpdateError
		}
	}
	if accountJson.Email != "" {
		if eid, found := EmailMap[accountJson.Email]; found && eid != id {
			return validateUpdateError
		}
		if code, err := domainOf(accountJson.Email); code == 0 || err != nil {
			return validateUpdateError
		}
	}
	if accountJson.Joined != 0 && (accountJson.Joined < MinJoined || accountJson.Joined > MaxJoinedMinPremium) {
		//log.Printf("update invalid Joined: %v", accountJson.Joined)
		return validateUpdateError
	}
	if accountJson.Birth != math.MaxInt32 && (accountJson.Birth < BirthMin || accountJson.Birth > BirthMax) {
		//log.Printf("new invalid Birth: %v", accountJson.Birth)
		return validateUpdateError
	}
	if accountJson.Premium.Start != 0 && accountJson.Premium.Start < MaxJoinedMinPremium {
		//log.Printf("update invalid Premium.Start: %v", accountJson.Premium.Start)
		return validateUpdateError
	}
	if accountJson.Premium.Finish != 0 && accountJson.Premium.Finish < MaxJoinedMinPremium {
		//log.Printf("update invalid Premium.Finish: %v", accountJson.Premium.Finish)
		return validateUpdateError
	}
	if (accountJson.Sex != "" && sexCode(accountJson.Sex) == 0) || (accountJson.Status != "" && statusCode(accountJson.Status) == 0) {
		//log.Print("new invalid sex of status")
		return validateUpdateError
	}

	return nil

	/*	if accountJson.City != "" || accountJson.Country != "" {
			if accountJson.City == "" && arrayAccounts[id-1].city != 0 {
				accountJson.City = CityDict.values[arrayAccounts[id-1].city]
			}
			if accountJson.Country == "" && arrayAccounts[id-1].country != 0 {
				accountJson.Country = CountryDict.values[arrayAccounts[id-1].country]
			}
			if country, found := CityMap[accountJson.City]; found && country!="" && country!=accountJson.Country {
				mutate.Unlock()
				log.Printf("update invalid city->country: %s -> %s, valid country is %s", accountJson.City, accountJson.Country, country)
				ctx.SetStatusCode(400)
				return
			}
		}
	*/

}

var validateLikeError = errors.New("ValidateLike error")

func ValidateLikes(likes []LikeUpdate) error {
	max := atomic.LoadUint32(&Store.MaxId)
	for _, v := range likes {
		if v.Liker == 0 || v.Likee == 0 || v.Likee > max || v.Liker > max || v.Ts == 0 /*|| v.Ts < minJoined || v.Ts > now */ {
			return validateLikeError // fmt.Errorf("invalid like data: %v", v)
		}
	}
	return nil
}

func (p *Storage) LikesUpdate(likes []LikeUpdate) {
	for _, like := range likes {
		// внести в индекс
		likeeI := like.Likee - 1
		LikesIndexCompact[likeeI] = append(LikesIndexCompact[likeeI], like.Liker)
		if len(LikesIndexCompact[likeeI]) > 1 && LikesIndexCompact[likeeI][0] < LIKE_MOD_FLAG {
			LikesIndexCompact[likeeI][0] += LIKE_MOD_FLAG
		}
		// внести в счета
		likerI := like.Liker - 1
		p.Likes[likerI] = append(p.Likes[likerI], Like{like.Likee, uint32(like.Ts)})
		if len(p.Likes[likerI]) > 1 && p.Likes[likerI][0].Id < LIKE_MOD_FLAG {
			p.Likes[likerI][0].Id += LIKE_MOD_FLAG
		}

	}
}

func LoadOptions(dir string) (int32, int32) {
	file, err := os.Open(dir + "options.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var nowTime int
	if scanner.Scan() {
		if nowTime, err = strconv.Atoi(scanner.Text()); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("options.txt (now) is empty?")
	}
	if scanner.Scan() {
		if runtype, err := strconv.Atoi(scanner.Text()); err == nil {
			return int32(nowTime), int32(runtype)
		} else {
			log.Fatal(err)
		}
	}
	log.Fatal("options.txt is empty?")
	return 0, 0
}

var (
	rebuildCounter int64
	rebuildMutex   sync.RWMutex
)

func WaitRebuildComplete() {
	if atomic.LoadInt64(&rebuildCounter) != 0 {
		rebuildMutex.RLock()
		rebuildMutex.RUnlock()
	}
}

func (p *Storage) Rebuild(parallel bool) {
	fmt.Printf("%v\trebuilding...\n", Timenow())

	ResetCaches()
	ResetIndexes()

	var wg sync.WaitGroup
	wg.Add(4)

	funcRebuildRecommend := func() {
		//fmt.Printf("%v\tRebuildRecommendIndexes start\n", Timenow())
		RebuildRecommendIndexes(&p.Accounts, p.MaxId)
		fmt.Printf("%v\tRebuildRecommendIndexes done\n", Timenow())
		wg.Done()
	}

	funcRebuildAggregates := func() {
		//fmt.Printf("%v\tRebuildAggregates start\n", Timenow())
		RebuildAggregates(&p.Accounts, p.MaxId)
		fmt.Printf("%v\tRebuildAggregates done\n", Timenow())
		if parallel {
			go CalculateGroupsParallel(&wg)
			CalculateGroups()
		} else {
			CalculateGroupsParallel(&wg)
			CalculateGroups()
		}
		wg.Done()
	}

	funcLikesIndexCompact := func() {
		p.LikesSort()
		wg.Done()
	}

	if !parallel {
		funcRebuildRecommend()
		runtime.GC()
		funcRebuildAggregates()
		runtime.GC()
		funcLikesIndexCompact()
		runtime.GC()
	} else {
		go funcRebuildRecommend()
		go funcRebuildAggregates()
		go funcLikesIndexCompact()
	}
	//fmt.Printf("%v\tRebuildIndexes start\n", Timenow())
	RebuildIndexes(&p.Accounts, p.MaxId)
	fmt.Printf("%v\tRebuildIndexes done\n", Timenow())
	if parallel {
		wg.Wait()
	}
	runtime.GC()
	PrintMemUsage("rebuild final GC done")
	if parallel {
		// запускаем фазу 3 на полную
		evio.SetEpollWait(0)
	}
}

func (p *Storage) LikesSort() {

	for i := 0; i < len(LikesIndexCompact); i++ {
		target := &(LikesIndexCompact[i])
		if len(*target) > 1 && (*target)[0] > LIKE_MOD_FLAG {
			(*target)[0] -= LIKE_MOD_FLAG
			Uint32Slice(*target).Sort() //sort.Slice(LikesIndexCompact[index], func(i, j int) bool { return i < j } ) //
		}
	}
	//runtime.GC()
	fmt.Printf("%v\tLikesIndexCompact done\n", Timenow())

	for i := uint32(0); i < p.MaxId; i++ {
		if len(p.Likes[i]) > 1 && p.Likes[i][0].Id > LIKE_MOD_FLAG {
			p.Likes[i][0].Id -= LIKE_MOD_FLAG
			slice := LikeSlice(p.Likes[i])
			(&slice).Sort()
		}
	}
	runtime.GC()
	PrintMemUsage("Likes sort done")
}

func (p *Account) print(id uint32) {
	fmt.Printf("%d : %d, %s, %s, %s, %s, %v, prem: %d, city: %d\n", id, p.IsPremium(), sexOf(p.getSex()), statusOf(p.getStatus()), Store.Accounts2[id-1].email,
		time.Unix(int64(p.birth), 0).UTC(),
		//strconv.FormatUint(Store.interests[(id-1)*2], 2), strconv.FormatUint(Store.interests[(id-1)*2+1], 2),
		interestsToArray(Store.interests[(id-1)*2], Store.interests[(id-1)*2+1], make([]byte, 8)),
		p.IsPremium(), p.getCity())
}

/*
func ioutil.ReadFile(filename string) ([]byte, error)

----

 file, err := os.Open("binary.dat")

      if err != nil {
          fmt.Println(err)
          return
      }

      defer file.Close()

      info, err := file.Stat()
       if err != nil {
         return nil, err
        }

      // calculate the bytes size
      var size int64 = info.Size()
      bytes := make([]byte, size)

      // read into buffer
      buffer := bufio.NewReader(file)
      _,err = buffer.Read(bytes)
*/
/*
func LoadDataArray2(dir string, arr *[]Account) (maxId uint32, runType int32, err error) {

	if Store.Now, runType = LoadOptions(dir); Store.Now == 0 {
		return 0, 0, fmt.Errorf("error load %soptions.txt", dir)
	}

	r, err := zip.OpenReader(dir + "data.zip")
	if err != nil {
		return 0, runType, err
	}
	defer r.Close()

	//bytes := make([]byte, 20*1024*1024)

	// Iterate through the files in the archive,
	fmt.Printf("%v\tloading\n", Timenow())
	for _, f := range r.File {
		fmt.Print(".")

		rc, err := f.Open()
		if err != nil {
			return 0, runType, err
		}
		bytes, err := ioutil.ReadAll(rc)
		var p fastjson.Parser
		acc, err := p.ParseBytes(bytes)
		if err != nil {
			log.Fatal(err)
		}
		for _, v := range acc.GetArray("accounts") {
			id := uint32(v.GetInt("id"))
			if maxId < id {
				maxId = id
			}
			if err = CompressJsonToAccount(id, &(*arr)[id-1], v); err != nil {
				return 0, runType, err
			}
		}
		_ = rc.Close()
	}
	Rebuild(arr, maxId)
	return maxId, runType, nil
}
*/
// PrintMemUsage outputs the current, total and OS memory being used. As well as the number
// of garage collection cycles completed.
func PrintMemUsage(info string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("%v\t%s, %v/%v, %v/%v\n",
		Timenow(), info, bToMb(m.Alloc), bToMb(m.Sys), bToMb(m.TotalAlloc), m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func read(r io.Reader, bytes []byte) ([]byte, error) {
	done := 0
	for {
		n, _ := r.Read(bytes[done:])
		if n == 0 {
			break
		}
		done += n
	}
	return bytes[:done], nil
}

func safeString(value *fastjson.Value) string {
	if value == nil {
		return ""
	} else {
		bytes, _ := value.StringBytes()
		return string(bytes) // todo ? B2s(bytes)
	}
}

func toString(value *fastjson.Value) string {
	if value == nil {
		return ""
	} else {
		// todo ?
		// s, _ := strconv.Unquote(str)
		// return s
		value.Type()
		//return value.String() /// fixme wtf?
		bytes, _ := value.StringBytes()
		return string(bytes) // todo ? B2s(bytes)
	}
}

func B2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func S2b(s string) []byte {
	strh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	var sh reflect.SliceHeader
	sh.Data = strh.Data
	sh.Len = strh.Len
	sh.Cap = strh.Len
	return *(*[]byte)(unsafe.Pointer(&sh))
}

func Timenow() string {
	return time.Now().Format("03:04:05.000")
}
