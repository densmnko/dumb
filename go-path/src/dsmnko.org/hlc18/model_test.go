package hlc18

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"github.com/bcicen/jstream"
	"github.com/pkg/profile"
	"github.com/valyala/fastjson"
	"log"
	"runtime"
	"testing"
)

func BenchmarkJsons(b *testing.B) {

	bytes := []byte(`{"premium":{"finish":1569783897,"start":1538247897},"birth":692070602,"likes":[{"id":1066502,"ts":1457224588},{"id":78150,"ts":1510460183},{"id":853672,"ts":1515150863},{"id":733556,"ts":1481890895},{"id":856740,"ts":1487805837},{"id":1047142,"ts":1458068896},{"id":629144,"ts":1497234539},{"id":300826,"ts":1487754288},{"id":452932,"ts":1461098717},{"id":1195182,"ts":1530979034},{"id":30712,"ts":1461763696},{"id":454620,"ts":1481582929},{"id":407760,"ts":1529800373},{"id":371160,"ts":1501625150},{"id":241380,"ts":1534382164},{"id":803700,"ts":1519479388},{"id":685966,"ts":1486322921},{"id":705718,"ts":1520958917},{"id":626422,"ts":1510751192},{"id":804108,"ts":1493807254},{"id":129572,"ts":1497212902},{"id":778756,"ts":1510678372},{"id":773756,"ts":1532030052},{"id":492876,"ts":1502403536},{"id":142110,"ts":1519654549},{"id":1160208,"ts":1492142179},{"id":102868,"ts":1488957199},{"id":684186,"ts":1489454147},{"id":234630,"ts":1535043000},{"id":973762,"ts":1458619587},{"id":172290,"ts":1486412904},{"id":559182,"ts":1521790424},{"id":1181378,"ts":1495685605},{"id":1213814,"ts":1476655968},{"id":664086,"ts":1501684354},{"id":1270304,"ts":1517398037},{"id":804610,"ts":1525566197},{"id":593016,"ts":1504872249},{"id":239696,"ts":1535239086},{"id":579844,"ts":1525424588},{"id":986152,"ts":1494908263},{"id":799462,"ts":1468367050},{"id":519462,"ts":1470576898},{"id":824660,"ts":1453419877},{"id":738136,"ts":1488428182},{"id":683884,"ts":1468286120},{"id":218188,"ts":1506629719},{"id":1008478,"ts":1533144960},{"id":1090292,"ts":1515672077},{"id":1230224,"ts":1526088509},{"id":298636,"ts":1522805966},{"id":540050,"ts":1532083398},{"id":1200456,"ts":1455636126},{"id":342036,"ts":1467363135},{"id":810644,"ts":1515156323}],"sex":"m","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","country":"\u0424\u0438\u043d\u043c\u0430\u043b\u044c","interests":["\u0420\u0435\u0433\u0433\u0438","\u041f\u0430\u0441\u0442\u0430","\u0414\u0440\u0443\u0437\u044c\u044f"],"joined":1342828800,"city":"\u041a\u0440\u043e\u043d\u043e\u0433\u043e\u0440\u0441\u043a","email":"hiteher@inbox.ru"}`)
	var p fastjson.Parser

	b.Run("FastJson", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			acc, err := p.ParseBytes(bytes)
			if err != nil {
				log.Fatal(err)
			}

			s := toString(acc.Get("city"))
			if s != "Кроногорск" {
				log.Fatal("FastJson Кроногорск")
			}
			toString(acc.Get("status"))
			toString(acc.Get("country"))
			for _, interest := range acc.GetArray("interests") {
				toString(interest)
			}

		}
	})

	b.Run("UnmarshalJSON", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var accountJson AccountJson
			if err := json.Unmarshal(bytes, &accountJson); err != nil {
				log.Fatal(err)
			}
			if accountJson.City != "Кроногорск" {
				log.Fatal("UnmarshalJSON Кроногорск")
			}
		}
	})

	//bytes := acc.GetStringBytes("city")
	//s, err := strconv.Unquote()
}

func TestFastJsonLoad2(t *testing.T) {
	const capacity = 30000
	MakeIndexes(capacity)
	InitStore(capacity)
	if err := Store.LoadData("/tmp/data-test-2612/"); err != nil {
		t.Fatal(err)
	}
}

func TestBcicenJstream(t *testing.T) {

	defer profile.Start(profile.MemProfileRate(2048), profile.ProfilePath("c:/tmp")).Stop()

	r, err := zip.OpenReader("/tmp/data/data.zip")
	if err != nil {
		t.Error(err)
	}
	defer r.Close()

	arraysCount := 0
	accounts := 0

	maxid := 0

	for _, f := range r.File {
		rc, err := f.Open()
		if err != nil {
			t.Error(err)
		}
		decoder := jstream.NewDecoder(rc, 2) // extract JSON values at a depth level of 1
		for mv := range decoder.Stream() {
			switch mv.Value.(type) {
			case []interface{}:
				arraysCount++
			//case float64:
			//	label = "float  "
			//case jstream.KV:
			//	label = "kv     "
			//case string:
			//	label = "string "
			case map[string]interface{}:
				accounts++
				//m := mv.Value.(map[string]interface{})
				//i := m["id"].(int)
				//if maxid < i {
				//	maxid = i
				//}
				//println(i.(int))
			}
		}

		fmt.Printf("accouns %d, maxid so far %d\n", accounts, maxid)

		rc.Close()

		/*		value, err := p.ParseBytes(buffer)
				if err != nil {
					t.Error(err)
				}
				fmt.Printf("%s, %d, %d\n", f.Name, len(buffer), len(value.GetArray("accounts")))

				for _, a := range value.GetArray("accounts") {

					acc := AccountJson{
						Id:    uint32(a.GetUint("id")),
						Fname: toString(a.Get("fname")),
						Sname: toString(a.Get("sname")),
						Email: toString(a.Get("email")),
						//Interests : toString(a.Get("interests")),
						Status: toString(a.Get("status")),
						//Premium : { Start: }toString(a.Get("premium")),
						Sex:   toString(a.Get("sex")),
						Phone: toString(a.Get("phone")),
						//Likes : toString(a.Get("likes")),
						Birth:   int32(a.GetInt("birth")),
						City:    toString(a.Get("city")),
						Country: toString(a.Get("country")),
						Joined:  int32(a.GetInt("joined")),
					}

					if interests := a.GetArray("interests"); len(interests) > 0 {
						acc.Interests = make([]string, len(interests))
						for i, v := range interests {
							acc.Interests[i] = toString(v)
						}
					}
					//if likes := a.GetArray("likes"); len(likes) > 0 {
					//}

					emails[acc.Email] = acc.Id
				}*/
		/*
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
		*/
		//_ = rc.Close()
	}

	println("maxid ", maxid)

	PrintMemUsage("")
	runtime.GC()
	PrintMemUsage("")

	//for k,v := range emails {
	//	fmt.Println(k,v)
	//}
}

func TestFastJsonsLoad(t *testing.T) {

	defer profile.Start(profile.MemProfileRate(2048), profile.ProfilePath("c:/tmp")).Stop()

	r, err := zip.OpenReader("/tmp/data/data.zip")
	if err != nil {
		t.Error(err)
	}
	defer r.Close()

	//bytes := make([]byte, 20*1024*1024)
	// Iterate through the files in the archive,

	var p fastjson.Parser
	var bufferBytes [20 * 1024 * 1024]byte

	var emails = make(map[string]uint32)

	for _, f := range r.File {

		rc, err := f.Open()
		if err != nil {
			t.Error(err)
		}
		buffer, err := read(rc, bufferBytes[:])
		if err != nil {
			t.Error(err)
		}

		value, err := p.ParseBytes(buffer)
		if err != nil {
			t.Error(err)
		}
		fmt.Printf("%s, %d, %d\n", f.Name, len(buffer), len(value.GetArray("accounts")))

		for _, a := range value.GetArray("accounts") {

			acc := AccountJson{
				Id:    uint32(a.GetUint("id")),
				Fname: toString(a.Get("fname")),
				Sname: toString(a.Get("sname")),
				Email: toString(a.Get("email")),
				//Interests : toString(a.Get("interests")),
				Status: toString(a.Get("status")),
				//Premium : { Start: }toString(a.Get("premium")),
				Sex:   toString(a.Get("sex")),
				Phone: toString(a.Get("phone")),
				//Likes : toString(a.Get("likes")),
				Birth:   int32(a.GetInt("birth")),
				City:    toString(a.Get("city")),
				Country: toString(a.Get("country")),
				Joined:  int32(a.GetInt("joined")),
			}

			if interests := a.GetArray("interests"); len(interests) > 0 {
				acc.Interests = make([]string, len(interests))
				for i, v := range interests {
					acc.Interests[i] = toString(v)
				}
			}
			//if likes := a.GetArray("likes"); len(likes) > 0 {
			//}

			emails[acc.Email] = acc.Id
		}
		/*
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
		*/_ = rc.Close()
	}

	//for k,v := range emails {
	//	fmt.Println(k,v)
	//}
}

func TestLoad(t *testing.T) {
	//defer profile.Start(profile.MemProfileRate(2048), profile.ProfilePath("c:/tmp")).Stop()

	r, _ := zip.OpenReader("/tmp/data/data.zip")
	defer r.Close()

	var fileCounter = 0
	// Iterate through the files in the archive,
	// printing some of their contents.
	fmt.Printf("%v\tloading\n", Timenow())
	for _, f := range r.File {
		rc, _ := f.Open()

		dec := json.NewDecoder(rc)
		//json.Delim: {
		_, _ = dec.Token()
		//string: accounts
		_, _ = dec.Token()
		//json.Delim: [
		_, _ = dec.Token()
		//fmt.Printf("%T: %v\n", t, t)

		// while the array contains values
		for dec.More() {
			var account AccountJson
			// decode an array value (Message)
			err := dec.Decode(&account)
			if err != nil {
				t.Fatal(err)
			}

		}
		rc.Close()
		fileCounter++
	}
}

func TestLikesJson(t *testing.T) {
	bytes := []byte(`{"likes":[
	{"likee": 3929, "ts": 1464869768, "liker": 25486},
		{"likee": 13239, "ts": 1431103000, "liker": 26727},
		{"likee": 2407, "ts": 1439604510, "liker": 6403},
		{"likee": 26677, "ts": 1454719940, "liker": 22248},
		{"likee": 22411, "ts": 1481309376, "liker": 32820},
		{"likee": 9747, "ts": 1431850118, "liker": 43794},
		{"likee": 43575, "ts": 1499496173, "liker": 16134},
		{"likee": 29725, "ts": 1479087147, "liker": 22248}
	]}`)

	//likesBuffer := LikesBufferBorrow(); defer LikesBufferRelease(likesBuffer)
	likes, _ := ParseLikesUpdate(bytes)
	println(len(likes))

	if len(likes) != 8 {
		t.Fatal("parseLikesUpdate failed")
	}
	if likes[7].Ts != 1479087147 || likes[0].Likee != 3929 {
		t.Fatal("parseLikesUpdate failed")
	}

}
