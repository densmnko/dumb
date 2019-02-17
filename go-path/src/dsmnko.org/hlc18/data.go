package hlc18

import (
	"errors"
	"github.com/valyala/fastjson"
	"sync"
)

type LikeUpdate struct {
	Likee uint32
	Liker uint32
	Ts    uint32
}

type Premium struct {
	Start  int32 `json:"start"`
	Finish int32 `json:"finish"`
}

type Like struct {
	Id uint32 `json:"id"`
	Ts uint32 `json:"ts"`
}

//easyjson:json
type AccountJson struct {
	Id        uint32   `json:"id"`
	Fname     string   `json:"fname"`
	Sname     string   `json:"sname"`
	Email     string   `json:"email"`
	Interests []string `json:"interests"`
	Status    string   `json:"status"`
	Premium   Premium  `json:"premium"`
	Sex       string   `json:"sex"`
	Phone     string   `json:"phone"`
	Likes     []Like   `json:"likes"`
	Birth     int32    `json:"birth"`
	City      string   `json:"city"`
	Country   string   `json:"country"`
	Joined    int32    `json:"joined"`
}

var parserPool = sync.Pool{
	New: func() interface{} { return &fastjson.Parser{} },
}

var likeUpdatePool = sync.Pool{
	New: func() interface{} {
		buffer := [1024]LikeUpdate{}
		return buffer[:]
	},
}

func _LikesBufferBorrow() []LikeUpdate {
	return likeUpdatePool.Get().([]LikeUpdate)
}

func _LikesBufferRelease(buffer []LikeUpdate) {
	likeUpdatePool.Put(buffer)
}

func ParseLikesUpdate(bytes []byte) ([]LikeUpdate, error) {
	var parser = parserPool.Get().(*fastjson.Parser)
	defer parserPool.Put(parser)
	if value, err := parser.ParseBytes(bytes); err == nil {
		array := value.GetArray("likes")
		likes := make([]LikeUpdate, len(array))
		for i, v := range array {
			likes[i].Ts = uint32(v.GetUint("ts"))
			likes[i].Liker = uint32(v.GetUint("liker"))
			likes[i].Likee = uint32(v.GetUint("likee"))
		}
		return likes, nil
	} else {
		return nil, err
	}
}

/*
{
    "sname": "Хопетачан",
    "email": "orhograanenor@yahoo.com",
    "country": "Голция",
    "interests": [],
    "birth": 736598811,
    "id": 50000,
    "sex": "f",
    "likes": [
        {"ts": 1475619112, "id": 38753},
        {"ts": 1464366718, "id": 14893},
        {"ts": 1510257477, "id": 37967},
        {"ts": 1431722263, "id": 38933}
    ],
    "premium": {"start": 1519661251, "finish": 1522253251},
    "status": "всё сложно",
    "fname": "Полина",
    "joined": 1466035200
}
*/
var umnarshallError = errors.New("umnarshallError")

func UnmarshalNew(account *AccountJson, bytes []byte) error {
	var parser = parserPool.Get().(*fastjson.Parser)
	defer parserPool.Put(parser)
	if value, err := parser.ParseBytes(bytes); err == nil {
		if account.Id = uint32(value.GetUint("id")); account.Id == 0 {
			return umnarshallError
		}
		account.Birth = int32(value.GetInt("birth"))
		account.Joined = int32(value.GetInt("joined"))
		if account.Birth == 0 || account.Joined == 0 {
			return umnarshallError
		}
		if vp := value.Get("premium"); vp != nil {
			account.Premium.Start = int32(vp.GetInt("start"))
			account.Premium.Finish = int32(vp.GetInt("finish"))
			if account.Premium.Start == 0 || account.Premium.Finish == 0 {
				return umnarshallError
			}
		}
		vls := value.GetArray("likes")
		account.Likes = make([]Like, len(vls), len(vls))
		for i, vl := range vls {
			account.Likes[i].Id = uint32(vl.GetUint("id"))
			account.Likes[i].Ts = uint32(vl.GetUint("ts"))
			if account.Likes[i].Id == 0 || account.Likes[i].Ts == 0 {
				return umnarshallError
			}
		}

		// todo: по уму, все строки надо сразу херачить в словари
		account.Email = safeString(value.Get("email"))
		account.Phone = safeString(value.Get("phone"))
		account.Sex = safeString(value.Get("sex"))
		account.Fname = toString(value.Get("fname"))
		account.Sname = toString(value.Get("sname"))
		account.Status = toString(value.Get("status"))
		account.City = toString(value.Get("city"))
		account.Country = toString(value.Get("country"))

		vis := value.GetArray("interests")
		if len(vis) > 0 {
			account.Interests = make([]string, len(vis), len(vis))
			for i, vi := range vis {
				account.Interests[i] = toString(vi)
			}
		}
	} else {
		return err
	}
	return nil
}

func UnmarshalUpdate(account *AccountJson, bytes []byte) error {
	var parser = parserPool.Get().(*fastjson.Parser)
	defer parserPool.Put(parser)
	if value, err := parser.ParseBytes(bytes); err == nil {

		if v := value.Get("birth"); v != nil {
			if account.Birth = int32(v.GetInt()); account.Birth == 0 {
				return umnarshallError
			}
		}
		if v := value.Get("joined"); v != nil {
			if account.Joined = int32(v.GetInt()); account.Joined == 0 {
				return umnarshallError
			}
		}
		if vp := value.Get("premium"); vp != nil {
			account.Premium.Start = int32(vp.GetInt("start"))
			account.Premium.Finish = int32(vp.GetInt("finish"))
			if account.Premium.Start == 0 || account.Premium.Finish == 0 {
				return umnarshallError
			}
		}

		// todo: по уму, все строки надо сразу херачить в словари
		account.Email = safeString(value.Get("email"))
		account.Phone = safeString(value.Get("phone"))
		account.Sex = safeString(value.Get("sex"))
		account.Fname = toString(value.Get("fname"))
		account.Sname = toString(value.Get("sname"))
		account.Status = toString(value.Get("status"))
		account.City = toString(value.Get("city"))
		account.Country = toString(value.Get("country"))

		if vis := value.GetArray("interests"); len(vis) > 0 {
			account.Interests = make([]string, len(vis), len(vis))
			for i, vi := range vis {
				account.Interests[i] = toString(vi)
			}
		}

		if vls := value.GetArray("likes"); len(vls) > 0 {
			for _, vl := range vls {
				if vl.GetUint("id") == 0 || uint32(vl.GetUint("ts")) == 0 {
					return umnarshallError
				}
			}
			// todo : ? проверить что сюда не попадаем
			panic("oops!")

		}

	} else {
		return err
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
		acc2.likesId = makeIdArray(len(likes))
		acc2.likesTs = make([]uint32, len(likes))
		for i, like := range likes {
			acc2.likesId.Put(i, uint32(like.GetUint("id")))
			acc2.likesTs[i] = uint32(like.GetUint("ts"))
		}
		// indexing
		AppendAccountLikesIndexDirect(id, acc2.likesId)
		//appendInterestsIndex(value.Id, acc)
	}

	return id, nil
}*/
