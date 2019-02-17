package hlc18

import (
	"fmt"
	"math/bits"
	"net/url"
	"reflect"
	"testing"
)

func BenchmarkBitCount(b *testing.B) {

	var me Bitset128
	var other Bitset128

	me.Set(1)
	me.Set(16)
	me.Set(31)

	me.Set(63)
	me.Set(64)
	me.Set(65)
	me.Set(42)
	me.Set(89)

	other.Set(16)
	other.Set(31)
	other.Set(8)
	other.Set(21)

	other.Set(63)
	other.Set(64)
	other.Set(65)

	test0 := me[0] & other[0]
	//test1 := me[1] & other[1]

	//println(bitcountNaive(test0))
	//println(bitcountNaive(test1))
	//println(bitcountSparse(test0))
	//println(bitcountSparse(test1))
	//println(bitcountWP3(test0))
	//println(bitcountWP3(test1))

	//test0 = test1

	b.Run("math.OnesCount64", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bits.OnesCount64(test0)
		}

	})

	b.Run("bitcountWP3", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bitcountWP3(test0)
		}
	})

	b.Run("bitcountNaive", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bitcountNaive(test0)
		}

	})
	/*	b.Run("bitcountNaive", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for i := 0; i < b.N; i++ {
					bitcountNaive(test1)
				}
			}
		})
	*/
	b.Run("bitcountSparse[0]", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bitcountSparse(test0)
		}

	})

	/*b.Run("bitcountSparse[1]", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for i := 0; i < b.N; i++ {
				bitcountSparse(test1)
			}
		}
	})
	*/

	/*	b.Run("bitcountWP3[1]", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for i := 0; i < b.N; i++ {
					bitcountWP3(test1)
				}
			}
		})
	*/

}

func BenchmarkLoad(b *testing.B) {
	const capacity = 1310000
	MakeIndexes(capacity)
	InitStore(capacity)
	_ = Store.LoadData("/Users/den/tmp/data/")

	b.Run("Recommend", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			//for j := 1; j < 100; j++ {
			_ = Recommend(uint32(i+1), 20, map[string]string{})
			//}
		}

	})

}

func TestRecommend(t *testing.T) {
	const capacity = 30000
	MakeIndexes(capacity)
	InitStore(capacity)
	_ = Store.LoadData("/tmp/data-test-2612/")
	if l := len(Store.Accounts); l != 30000 {
		t.Fatal("load failed")
	}

	var rec []uint32
	var str string

	str, _ = url.PathUnescape("/accounts/18222/recommend/?city=%D0%9B%D0%B5%D0%B9%D0%BF%D0%BE%D1%80%D0%B8%D0%B6&query_id=840&limit=4")
	fmt.Println(str)

	// REQUEST  URI: /accounts/18222/recommend/?city=%D0%9B%D0%B5%D0%B9%D0%BF%D0%BE%D1%80%D0%B8%D0%B6&query_id=840&limit=4
	// REQUEST BODY:
	// BODY   GOT: {"accounts":[]}
	// BODY   EXP: {"accounts":[{"premium":{"finish":1558306307,"start":1526770307},"status":"\u0432\u0441\u0451 \u0441\u043b\u043e\u0436\u043d\u043e","fname":"\u0418\u043b\u044c\u044f","email":"irywiharfisiorte@yandex.ru","id":20899,"sname":"\u0414\u0430\u043d\u0430\u0442\u043e\u0432\u0435\u043d","birth":742486436},
	// {"id":20073,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u0410\u043d\u0430\u0442\u043e\u043b\u0438\u0439","birth":737114436,"email":"ahnieletsontocesnyn@yandex.ru"},
	// {"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u0420\u0443\u0441\u043b\u0430\u043d","email":"segoevdarlapitiet@list.ru","id":7461,"sname":"\u0414\u0430\u043d\u0430\u0442\u043e\u0442\u0435\u0432","birth":737518627},
	// {"id":24597,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u041d\u0438\u043a\u0438\u0442\u0430","birth":622702446,"email":"ihaltet@inbox.com"}]}

	accounts := Store.Accounts

	fmt.Printf("Now: %d\n", Store.Now)

	_ = recommendKey(0, 1, 1, 10)
	//keyCity := recommendKeyCity(key, 72)

	//countries := CityMap[CityDict.values[72]]
	//for c := range countries {
	//	keyCountry := recommendKeyCountry(key, CountryDict.ids[c])
	//	fmt.Printf("%d: %v\n%d: %v\n", key, RecommendIndexCompact[key], keyCountry, RecommendIndexCountryCompact[keyCountry])
	//}

	//fmt.Printf("%d: %v\n%d: %v\n",key,RecommendIndexCompact[key], keyCity, RecommendIndexCityCompact[keyCity])

	rec = Recommend(18222, 4, map[string]string{"city": "Лейпориж"})

	print("city code: ")
	println(CityDict.ids["Лейпориж"])

	print("me: ")
	accounts[18221].print(18222)
	print("await[0]: ")
	accounts[20899-1].print(20899)
	print("await[3]: ")
	accounts[24597-1].print(24597)

	fmt.Println("------------------")

	for _, a := range rec {
		accounts[a-1].print(a)
	}

	if rec[0] != 20899 {
		t.Error("failed [0]-20899: /accounts/18222/recommend/?city=Лейпориж&query_id=840&limit=4")
	}
	if rec[1] != 20073 {
		t.Error("failed [1]-20073: /accounts/18222/recommend/?city=Лейпориж&query_id=840&limit=4")
	}
	if rec[2] != 7461 {
		t.Error("failed [2]-7461: /accounts/18222/recommend/?city=Лейпориж&query_id=840&limit=4")
	}
	if rec[3] != 24597 {
		t.Error("failed [3]-24597: /accounts/18222/recommend/?city=Лейпориж&query_id=840&limit=4")
	}

	// 18222 : 0, f, заняты, etatrem@yandex.ru, 1991-09-01 10:07:08 +0000 UTC, 100000000000000000000100000000000000000000000100000000000:0
	// ------------------
	// 20899 : 1, m, всё сложно, irywiharfisiorte@yandex.ru, 1993-07-12 14:13:56 +0000 UTC, 100000100000:1000000000000
	//
	// 20073 : 0, m, свободны, ahnieletsontocesnyn@yandex.ru,  1993-05-11 10:00:36 +0000 UTC, 100000000000:10000000000000100000000
	// 7461 :  0, m, свободны, segoevdarlapitiet@list.ru, 		1993-05-16 02:17:07 +0000 UTC, 100000001000010100000001000:0
	// 24597 : 0, m, свободны, ihaltet@inbox.com, 				1989-09-25 04:54:06 +0000 UTC, 10000000000000000000000100000000000000000000000000000000000:0
	//
	//
	// 13235 : 0, m, свободны, inpezoh@gmail.com, 1987-12-02 16:19:15 +0000 UTC, 1000000000000000100000001010100000000000000100000000000:10000000000000000000000000
	// 27871 : 0, m, свободны, luhorog@gmail.com, 1994-04-25 17:26:46 +0000 UTC, 1000000000000000000000000000100000000000000000100000000010:1
	// 8323 : 0, m, свободны, vipofennysah@icloud.com, 1994-02-06 14:39:05 +0000 UTC, 10000000000100000000000100000000000000000000001000000000000:0
	// 13687 : 0, m, свободны, atsaihod@yandex.ru, 1989-08-09 15:45:04 +0000 UTC, 10000000100000000000000000010000000000000000100000000000:0

	// REQUEST  URI: /accounts/1741/recommend/?query_id=1080&limit=6
	// REQUEST BODY:
	// BODY   GOT: {"accounts":[{"id":13591,"email":"seynfelusylninip@email.com","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u0415\u0433\u043e\u0440","birth":665264004,"premium":{"finish":1551155844,"start":1519619844}},{"id":25201,"email":"imrevheen@yandex.ru","fname":"\u0421\u0442\u0435\u043f\u0430\u043d","sname":"\u041f\u0435\u043d\u043e\u043b\u043e\u0442\u0438\u043d","birth":651935709,"premium":{"finish":1559445559,"start":1527909559},"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b"},{"id":29811,"email":"redsiptontas@ymail.com","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u0412\u0438\u043a\u0442\u043e\u0440","sname":"\u041b\u0435\u0431\u0430\u0448\u0435\u043b\u0430\u043d","birth":769468347,"premium":{"finish":1563218399,"start":1531682399}},{"id":4363,"email":"awenkertad@icloud.com","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u0421\u0430\u0432\u0432\u0430","sname":"\u0425\u043e\u043f\u043e\u043b\u043e\u043f\u043e\u0432","birth":547700732,"premium":{"finish":1549230002,"start":1533505202}},{"id":26753,"email":"rivevocnat@gmail.com","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u041d\u0438\u043a\u0438\u0442\u0430","sname":"\u0422\u0435\u0440\u0443\u0448\u0443\u0432\u0435\u043d","birth":494307371,"premium":{"finish":1561510239,"start":1529974239}},{"id":16025,"email":"miohco@rambler.ru","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u0410\u043b\u0435\u043a\u0441\u0435\u0439","sname":"\u0414\u0430\u043d\u044b\u043a\u0430\u0432\u0435\u043d","birth":704701991,"premium":{"finish":1565880301,"start":1534344301}}]}
	// BODY   EXP: {"accounts":[{"premium":{"finish":1551955204,"start":1520419204},"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u0412\u0430\u0441\u0438\u043b\u0438\u0441\u0430","email":"fivewevheecwes@ya.ru","id":7840,"sname":"\u041a\u043b\u0435\u0440\u044b\u043a\u0430\u0442\u0435\u0432\u0430","birth":664858853},{"premium":{"finish":1550960915,"start":1543098515},"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u0410\u043b\u0451\u043d\u0430","email":"inenselrecwes@list.ru","id":15918,"sname":"\u041f\u0435\u043d\u043e\u043b\u043e\u043d\u0430\u044f","birth":760873559},{"premium":{"finish":1552365635,"start":1536640835},"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u041c\u0430\u0440\u0438\u044f","email":"efweminehsehelifa@yandex.ru","id":8666,"sname":"\u0424\u0430\u043e\u043b\u043e\u0447\u0430\u043d","birth":609534278},{"premium":{"finish":1570280743,"start":1538744743},"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u041b\u0435\u0440\u0430","email":"oscetedewavetegta@gmail.com","id":17496,"sname":"\u0422\u0435\u0440\u043b\u0435\u043d\u0432\u0438\u0447","birth":475439729},{"premium":{"finish":1562589674,"start":1531053674},"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u0415\u0432\u0433\u0435\u043d\u0438\u044f","email":"ehhanettotsaed@inbox.ru","id":29932,"sname":"\u041a\u0438\u0441\u043b\u0435\u043d\u0441\u044f\u043d","birth":704475019},{"premium":{"finish":1552687515,"start":1536962715},"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","birth":702625295,"email":"tuwleqvohagonarerle@inbox.ru","id":28910}]}

	rec = Recommend(1741, 6, map[string]string{})

	print("\nme and awaited-----------------------------------\n")

	accounts[1741-1].print(1741)
	accounts[7840-1].print(7840)

	print("\ngot -----------------------------------\n")

	for _, a := range rec {
		accounts[a-1].print(a)
	}

	if rec[0] != 7840 {
		t.Error("failed [0]-7840: /accounts/1741/recommend/?query_id=1080&limit=6")
	}

	str, _ = url.PathUnescape("/accounts/21987/recommend/?country=%D0%9C%D0%B0%D0%BB%D0%BC%D0%B0%D0%BB%D1%8C&query_id=1440&limit=2")
	fmt.Println("\n", str)

	//REQUEST  URI: /accounts/21987/recommend/?country=%D0%9C%D0%B0%D0%BB%D0%BC%D0%B0%D0%BB%D1%8C&query_id=1440&limit=2
	//REQUEST BODY:
	//BODY   GOT: {"accounts":[{"id":6045,"email":"intibinon@inbox.ru","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u0415\u0433\u043e\u0440","sname":"\u041b\u0435\u0431\u043e\u043b\u043e\u0441\u044f\u043d","birth":657448097,"premium":{"finish":1551792615,"start":1520256615}},{"id":8797,"email":"ehdatdaehrererdaas@inbox.com","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u0414\u0435\u043d\u0438\u0441","sname":"\u0424\u0435\u0442\u043e\u043b\u043e\u0447\u0430\u043d","birth":767405048,"premium":{"finish":1558658432,"start":1542933632}}]}
	//BODY   EXP: {"accounts":[{"premium":{"finish":1570444883,"start":1538908883},"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u041c\u0438\u043b\u0435\u043d\u0430","email":"pepicrinhi@yandex.ru","id":20490,"sname":"\u041b\u0435\u0431\u043e\u043b\u043e\u0432\u0438\u0447","birth":732823869},{"premium":{"finish":1565249231,"start":1533713231},"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u041d\u0430\u0442\u0430\u043b\u044c\u044f","email":"tawnatpawyd@ymail.com","id":4666,"sname":"\u041a\u0438\u0441\u0430\u0448\u0435\u0442\u0435\u0432\u0430","birth":744371189}]}

	rec = Recommend(21987, 2, map[string]string{"country": "Малмаль"})

	accounts[21987-1].print(21987)
	accounts[20490-1].print(20490)
	accounts[4666-1].print(4666)

	fmt.Println("------------------")

	for _, a := range rec {
		accounts[a-1].print(a)
	}

	if rec[0] != 20490 {
		t.Error("failed [0]-21987: /accounts/21987/recommend/?country=Малмаль&query_id=1440&limit=2")
	}
	if rec[1] != 4666 {
		t.Error("failed [1]-4666: /accounts/21987/recommend/?country=Малмаль&query_id=1440&limit=2")

	}

	// cache check
	rec2 := Recommend(21987, 2, map[string]string{"country": "Малмаль"})
	if !reflect.DeepEqual(rec2, rec) {
		t.Error("cache failed, ", str)
	}

	str, _ = url.PathUnescape("/accounts/2222/recommend/?city=%D0%A1%D0%B0%D0%BD%D0%BA%D1%82%D0%BE%D0%B3%D1%80%D0%B0%D0%B4&query_id=2054&limit=8")
	fmt.Println("\n", str)
	//REQUEST  URI: /accounts/2222/recommend/?city=%D0%A1%D0%B0%D0%BD%D0%BA%D1%82%D0%BE%D0%B3%D1%80%D0%B0%D0%B4&query_id=2054&limit=8
	rec = Recommend(2222, 8, map[string]string{"city": "Санктоград"})
	accounts[2222-1].print(2222)
	fmt.Println("------------------")
	for _, a := range rec {
		accounts[a-1].print(a)
	}
	if len(rec) != 0 {
		t.Error("failed, ", str)
	}

}
