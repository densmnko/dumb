package hlc18

import (
	"fmt"
	"net/url"
	"os"
	"testing"
	"unsafe"
)

// func (p *GroupCache) Prepare() {
//
// 	{
// 		//[city]
// 		group := groupOf("keys", "city", "order", "1")
// 		result, _ := group.aggregate()
// 		/*head, tail := */ group.Tails(result, 50)
// 		log.Println("[city]")
// 		//fmt.Println("HEAD")
// 		//for _, item := range head {
// 		//	group.DebugWriteJsonGroupItem(os.Stderr, item.Key, item.Count)
// 		//	fmt.Println()
// 		//}
// 		//fmt.Println("TAIL")
// 		//for _, item := range tail {
// 		//	group.DebugWriteJsonGroupItem(os.Stderr, item.Key, item.Count)
// 		//	fmt.Println()
// 		//}
// 	}
//
// 	{
// 		//[city status]
// 		group := groupOf("keys", "city,status", "order", "1")
// 		result, _ := group.aggregate()
// 		/*head, tail := */ group.Tails(result, 50)
// 		//[city status]
// 		log.Println("[city,status]")
//
// 		//fmt.Println("HEAD")
// 		//for _, item := range head {
// 		//	group.DebugWriteJsonGroupItem(os.Stderr, item.Key, item.Count)
// 		//	fmt.Println()
// 		//}
// 		//fmt.Println("TAIL")
// 		//for _, item := range tail {
// 		//	group.DebugWriteJsonGroupItem(os.Stderr, item.Key, item.Count)
// 		//	fmt.Println()
// 		//}
// 	}
//
// 	{
// 		//[city status]
// 		group := groupOf("keys", "city,status", "sex", "m", "order", "1")
// 		result, _ := group.aggregate()
// 		/*head, tail := */ group.Tails(result, 50)
// 		//[city status]
// 		log.Println("[city,status]/sex=m")
// 	}
//
// 	{
// 		//[city status]
// 		group := groupOf("keys", "city,status", "sex", "f", "order", "1")
// 		result, _ := group.aggregate()
// 		/*head, tail := */ group.Tails(result, 50)
// 		//[city status]
// 		log.Println("[city,status]/sex=f")
// 	}
//
// 	{
// 		//[city status]
// 		for interest := range InterestDict.ids {
// 			group := groupOf("keys", "city,status", "interests", interest, "order", "1")
// 			result, _ := group.aggregate()
// 			/*head, tail := */ group.Tails(result, 50)
// 			//[city status]
// 			log.Println("[city,status]/interests", interest)
// 		}
//
// 	}
//
// }

// func TestGroupCache(t *testing.T) {
// 	accounts := loadTestData(t)
// 	if len(accounts) != 30000 {
// 		t.Fail()
// 	}
//
// 	var cache GroupCache
// 	cache.Prepare()
//
// }

func TestGroupTemp(t *testing.T) {

	//group;[city]/1/status,;128;624;4879;7134
	//group;[city]/-1/status,;104;505;4857;5559
	//group;[city]/1/sex,;84;420;5006;5490

	accounts := loadTestData(t)
	if len(accounts) != 30000 {
		t.Fail()
	}

	//REQUEST  URI: /accounts/group/?query_id=1539&order=1&birth=1994&limit=40&keys=interests
	//REQUEST BODY:
	//STATUS GOT: 400
	//STATUS EXP: 200
	//BODY   GOT:
	//BODY   EXP: {"groups":[{"count":83,"interests":"\u0410\u043f\u0435\u043b\u044c\u0441\u0438\u043d\u043e\u0432\u044b\u0439 \u0441\u043e\u043a"},{"count":84,"interests":"\u041f\u0430\u0441\u0442\u0430"},{"count":84,"interests":"\u0422\u0438\u0442\u0430\u043d\u0438\u043a"},{"count":86,"interests":"South Park"},{"count":86,"interests":"\u041a\u043e\u043c\u043f\u044c\u044e\u0442\u0435\u0440\u044b"},{"count":88,"interests":"\u0416\u0438\u0437\u043d\u044c"},{"count":88,"interests":"\u0422\u0430\u043d\u0446\u0435\u0432\u0430\u043b\u044c\u043d\u0430\u044f"},{"count":89,"interests":"\u041e\u0432\u043e\u0449\u0438"},{"count":89,"interests":"\u041f\u0440\u0438\u0433\u043e\u0442\u043e\u0432\u043b\u0435\u043d\u0438\u0435 \u0435\u0434\u044b"},{"count":90,"interests":"\u041b\u044e\u0434\u0438 \u0418\u043a\u0441"},{"count":90,"interests":"\u041e\u0431\u0449\u0435\u043d\u0438\u0435"},{"count":91,"interests":"\u041d\u0430 \u043e\u0442\u043a\u0440\u044b\u0442\u043e\u043c \u0432\u043e\u0437\u0434\u0443\u0445\u0435"},{"count":92,"interests":"\u0413\u0430\u0440\u0440\u0438 \u041f\u043e\u0442\u0442\u0435\u0440"},{"count":92,"interests":"\u0417\u0434\u043e\u0440\u043e\u0432\u044c\u0435"},{"count":92,"interests":"\u0417\u043d\u0430\u043a\u043e\u043c\u0441\u0442\u0432\u043e"},{"count":92,"interests":"\u0421\u043f\u0430\u0433\u0435\u0442\u0442\u0438"},{"count":93,"interests":"\u041c\u0430\u0441\u0441\u0430\u0436"},{"count":93,"interests":"\u0421\u043e\u043d"},{"count":94,"interests":"\u0421\u0438\u043c\u043f\u0441\u043e\u043d\u044b"},{"count":94,"interests":"\u0422\u044f\u0436\u0451\u043b\u0430\u044f \u0430\u0442\u043b\u0435\u0442\u0438\u043a\u0430"},{"count":94,"interests":"\u0424\u043e\u0440\u0441\u0430\u0436"},{"count":95,"interests":"\u0411\u043e\u0435\u0432\u044b\u0435 \u0438\u0441\u043a\u0443\u0441\u0441\u0442\u0432\u0430"},{"count":95,"interests":"\u0414\u0440\u0443\u0437\u044c\u044f \u0438 \u0411\u043b\u0438\u0437\u043a\u0438\u0435"},{"count":95,"interests":"\u041a\u043b\u0443\u0431\u043d\u0438\u043a\u0430"},{"count":95,"interests":"\u041f\u043e\u043f \u0440\u043e\u043a"},{"count":96,"interests":"\u0411\u0443\u0440\u0433\u0435\u0440\u044b"},{"count":96,"interests":"\u041f\u0438\u0432\u043e"},{"count":96,"interests":"\u041f\u043b\u0430\u0432\u0430\u043d\u0438\u0435"},{"count":96,"interests":"\u041f\u043b\u044f\u0436\u043d\u044b\u0439 \u043e\u0442\u0434\u044b\u0445"},{"count":96,"interests":"\u0422\u0443\u0444\u043b\u0438"},{"count":97,"interests":"\u0412\u044b\u0445\u043e\u0434\u043d\u044b\u0435"},{"count":97,"interests":"\u041f\u043e\u0446\u0435\u043b\u0443\u0438"},{"count":97,"interests":"\u0424\u0438\u0442\u043d\u0435\u0441"},{"count":98,"interests":"\u0411\u0430\u0441\u043a\u0435\u0442\u0431\u043e\u043b"},{"count":98,"interests":"\u0412\u043a\u0443\u0441\u043d\u043e \u043f\u043e\u0435\u0441\u0442\u044c"},{"count":98,"interests":"\u0414\u0440\u0443\u0437\u044c\u044f"},{"count":98,"interests":"\u0420\u043e\u043c\u0430\u043d\u0442\u0438\u043a\u0430"},{"count":98,"interests":"\u0421\u043f\u043e\u0440\u0442\u0438\u0432\u043d\u044b\u0435 \u043c\u0430\u0448\u0438\u043d\u044b"},{"count":98,"interests":"\u0421\u0442\u0435\u0439\u043a"},{"count":98,"interests":"\u0422\u0430\u0442\u0443\u0438\u0440\u043e\u0432\u043a\u0438"}]}

	/*
		1 requests (0.04%) failed
		2400 queries in 655 ms => 3664 rps
		Top slowest queries:
		5.622072ms:/accounts/group/?query_id=2065&order=-1&birth=1985&limit=50&keys=interests
		5.561256ms:/accounts/group/?query_id=2161&order=1&birth=2001&limit=45&keys=interests
		5.439203ms:/accounts/group/?query_id=2111&order=1&birth=1984&limit=10&keys=interests
		5.409548ms:/accounts/group/?query_id=1287&order=-1&birth=1993&limit=30&keys=interests
		5.330997ms:/accounts/group/?query_id=2140&order=-1&limit=40&keys=city%2Cstatus&birth=1993&sex=f
		5.298989ms:/accounts/group/?query_id=2269&order=-1&birth=1988&limit=5&keys=interests
		5.282463ms:/accounts/group/?query_id=802&order=-1&birth=1985&limit=45&keys=interests
		4.906804ms:/accounts/group/?query_id=556&order=-1&limit=50&keys=city%2Cstatus&birth=1995&sex=m
		4.361599ms:/accounts/group/?query_id=1354&order=-1&limit=10&keys=city%2Cstatus&birth=2000&sex=f
		4.294093ms:/accounts/group/?status=%D1%81%D0%B2%D0%BE%D0%B1%D0%BE%D0%B4%D0%BD%D1%8B&order=1&query_id=163&keys=city%2Csex&birth=1997&limit=15
	*/

	// str, _ := url.PathUnescape("/accounts/group/?order=1&query_id=685&keys=city%2Cstatus&interests=%D0%A5%D0%B8%D0%BF+%D0%A5%D0%BE%D0%BF&birth=1984&limit=5")
	// fmt.Println(str)
	// /accounts/group/?query_id=1539&order=1&birth=1994&limit=40&keys=interests
	if _, result, group := aggregateSample(t, &accounts, 40, 90, "order", "1", "keys", "interests", "birth", "1994"); group != nil {
		//result := group.Sort(counters, 40)
		//for _, item := range result {
		//	group.DebugWriteJsonGroupItem(os.Stderr, item.Key, item.Count)
		//	fmt.Println()
		//}

		if result[0].Count != 83 || result[39].Count != 98 {
			t.Error("result[0].Count != 83 || result[39].Count != 98")
		}
	}

}

func TestPointerUpdate(t *testing.T) {

	xs := make([]uint32, 100, 100)
	for i := range xs {
		xs[i] = uint32(i)
	}
	if xs[99] != 99 {
		t.Error("xs[99] != 99")
	}

	var p *uint32

	p = (*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(&xs[0])) + 99*4))

	*p += 1

	if xs[99] != 100 {
		t.Error("xs[99] != 100 ->", xs[99])
	}

	//
	// The most common use of this pattern is to access fields in a struct
	// or elements of an array:
	//
	//	// equivalent to f := unsafe.Pointer(&s.f)
	//	f := unsafe.Pointer(uintptr(unsafe.Pointer(&s)) + unsafe.Offsetof(s.f))
	//
	//	// equivalent to e := unsafe.Pointer(&x[i])
	//	e := unsafe.Pointer(uintptr(unsafe.Pointer(&x[0])) + i*unsafe.Sizeof(x[0]))

}

func TestGroup(t *testing.T) {

	accounts := loadTestData(t)

	_, _, _ = classifySample(t, &accounts, 10, 0, "order", "-1", "keys", "status", "birth", "1983", "likes", "26242")

	//var counters map[uint64]int32
	//var group *Group

	_, _, _ = classifySample(t, &accounts, 5, 1, "keys", "sex", "order", "1", "likes", "20505")
	//BODY   EXP: {"groups":[{"count":38,"sex":"f"}]}
	// for k,v := range counters {
	// 	group.DebugWriteJsonGroupItem(os.Stdout, k, v)
	// 	fmt.Println()
	// }

	//549, limit 40, map[joined:2011 order:1 keys:city interests:Бег] -> 144
	if _, items, group := aggregateSample(t, &accounts, 40, 87, "joined", "2011", "order", "1", "keys", "city", "interests", "Бег"); group != nil {
		// BODY   EXP: {"groups":[{"count":1,"city":"\u0410\u043c\u0441\u0442\u0435\u0440\u0430\u043d\u0441\u043a"},{"count":1,"city":"\u0410\u043c\u0441\u0442\u0435\u0440\u043e\u0431\u043e\u043d"},{"count":1,"city":"\u0410\u043c\u0441\u0442\u0435\u0440\u043e\u0441\u0442\u0430\u043d"},{"count":1,"city":"\u0411\u0430\u0440\u0441\u043e\u0432\u0441\u043a"},{"count":1,"city":"\u0411\u0430\u0440\u0441\u043e\u0433\u0430\u043c\u0430"},{"count":1,"city":"\u0411\u0430\u0440\u0441\u043e\u0434\u0430\u043c"},{"count":1,"city":"\u0411\u0435\u043b\u043e\u0448\u0442\u0430\u0434\u0442"},{"count":1,"city":"\u0412\u0430\u0440\u0438\u043d\u0441\u043a"},{"count":1,"city":"\u0412\u0430\u0440\u043e\u0431\u0441\u043a"},{"count":1,"city":"\u0412\u0430\u0440\u043e\u0432\u0441\u043a"},{"count":1,"city":"\u0412\u0430\u0440\u043e\u043a\u0430\u043c\u0441\u043a"},{"count":1,"city":"\u0412\u0435\u043b\u0438\u043a\u043e\u0434\u0430\u043c"},{"count":1,"city":"\u0412\u043e\u043b\u043e\u043a\u0430\u043c\u0441\u043a"},{"count":1,"city":"\u0412\u043e\u043b\u043e\u043a\u0435\u043d\u0441\u043a"},{"count":1,"city":"\u0412\u043e\u043b\u043e\u0440\u0435\u0447\u0441\u043a"},{"count":1,"city":"\u0412\u043e\u043b\u043e\u0448\u0442\u0430\u0434\u0442"},{"count":1,"city":"\u0417\u0435\u043b\u0435\u043d\u043e\u0431\u0438\u0440\u0441\u043a"},{"count":1,"city":"\u0417\u0435\u043b\u0435\u043d\u043e\u043a\u0438\u043d\u0441\u043a"},{"count":1,"city":"\u041a\u0440\u043e\u043d\u043e\u0431\u0438\u0440\u0441\u043a"},{"count":1,"city":"\u041a\u0440\u043e\u043d\u043e\u0431\u0443\u0440\u0433"},{"count":1,"city":"\u041a\u0440\u043e\u043d\u043e\u0432\u0441\u043a"},{"count":1,"city":"\u041a\u0440\u043e\u043d\u043e\u0433\u0440\u0430\u0434"},{"count":1,"city":"\u041a\u0440\u043e\u043d\u043e\u043a\u0430\u043c\u0441\u043a"},{"count":1,"city":"\u041b\u0435\u0439\u043f\u043e\u0433\u0430\u043c\u0430"},{"count":1,"city":"\u041b\u0435\u0439\u043f\u043e\u043b\u0430\u043c\u0441\u043a"},{"count":1,"city":"\u041b\u0435\u0439\u043f\u043e\u043b\u0451\u0432"},{"count":1,"city":"\u041b\u0435\u0439\u043f\u043e\u043c\u043e\u0440\u0441\u043a"},{"count":1,"city":"\u041b\u0435\u0439\u043f\u043e\u043c\u0441\u043a"},{"count":1,"city":"\u041b\u0435\u0439\u043f\u043e\u0440\u0438\u0436"},{"count":1,"city":"\u041b\u0435\u0441\u043e\u0433\u043e\u0440\u0441\u043a"},{"count":1,"city":"\u041b\u0435\u0441\u043e\u0433\u0440\u0430\u0434"},{"count":1,"city":"\u041b\u0435\u0441\u043e\u043a\u0430\u0442\u0441\u043a"},{"count":1,"city":"\u041b\u0438\u0441\u0441\u0430\u0431\u0438\u0440\u0441\u043a"},{"count":1,"city":"\u041b\u0438\u0441\u0441\u0430\u0433\u0430\u043c\u0430"},{"count":1,"city":"\u041b\u0438\u0441\u0441\u0430\u043a\u044f\u0440\u0441\u043a"},{"count":1,"city":"\u041b\u0438\u0441\u0441\u0430\u043f\u043e\u043b\u044c"},{"count":1,"city":"\u041c\u043e\u0441\u043e\u0448\u0442\u0430\u0434\u0442"},{"count":1,"city":"\u041d\u043e\u0432\u043e\u0433\u0440\u0430\u0434"},{"count":1,"city":"\u041d\u043e\u0432\u043e\u0434\u043e\u0440\u0444"},{"count":1,"city":"\u041d\u043e\u0432\u043e\u043a\u0430\u0442\u0441\u043a"}]}
		// {"groups":[{"count":1,"city":"\u0410\u043c\u0441\u0442\u0435\u0440\u0430\u043d\u0441\u043a"},{"count":1,"city":"\u0410\u043c\u0441\u0442\u0435\u0440\u043e\u0431\u043e\u043d"},{"count":1,"city":"\u0410\u043c\u0441\u0442\u0435\u0440\u043e\u0441\u0442\u0430\u043d"},{"count":1,"city":"\u0411\u0430\u0440\u0441\u043e\u0432\u0441\u043a"},{"count":1,"city":"\u0411\u0430\u0440\u0441\u043e\u0433\u0430\u043c\u0430"},{"count":1,"city":"\u0411\u0430\u0440\u0441\u043e\u0434\u0430\u043c"},{"count":1,"city":"\u0411\u0435\u043b\u043e\u0448\u0442\u0430\u0434\u0442"},{"count":1,"city":"\u0412\u0430\u0440\u0438\u043d\u0441\u043a"},{"count":1,"city":"\u0412\u0430\u0440\u043e\u0431\u0441\u043a"},{"count":1,"city":"\u0412\u0430\u0440\u043e\u0432\u0441\u043a"},{"count":1,"city":"\u0412\u0430\u0440\u043e\u043a\u0430\u043c\u0441\u043a"},{"count":1,"city":"\u0412\u0435\u043b\u0438\u043a\u043e\u0434\u0430\u043c"},{"count":1,"city":"\u0412\u043e\u043b\u043e\u043a\u0430\u043c\u0441\u043a"},{"count":1,"city":"\u0412\u043e\u043b\u043e\u043a\u0435\u043d\u0441\u043a"},{"count":1,"city":"\u0412\u043e\u043b\u043e\u0440\u0435\u0447\u0441\u043a"},{"count":1,"city":"\u0412\u043e\u043b\u043e\u0448\u0442\u0430\u0434\u0442"},{"count":1,"city":"\u0417\u0435\u043b\u0435\u043d\u043e\u0431\u0438\u0440\u0441\u043a"},{"count":1,"city":"\u0417\u0435\u043b\u0435\u043d\u043e\u043a\u0438\u043d\u0441\u043a"},{"count":1,"city":"\u041a\u0440\u043e\u043d\u043e\u0431\u0438\u0440\u0441\u043a"},{"count":1,"city":"\u041a\u0440\u043e\u043d\u043e\u0431\u0443\u0440\u0433"},{"count":1,"city":"\u041a\u0440\u043e\u043d\u043e\u0432\u0441\u043a"},{"count":1,"city":"\u041a\u0440\u043e\u043d\u043e\u0433\u0440\u0430\u0434"},{"count":1,"city":"\u041a\u0440\u043e\u043d\u043e\u043a\u0430\u043c\u0441\u043a"},{"count":1,"city":"\u041b\u0435\u0439\u043f\u043e\u0433\u0430\u043c\u0430"},{"count":1,"city":"\u041b\u0435\u0439\u043f\u043e\u043b\u0430\u043c\u0441\u043a"},{"count":1,"city":"\u041b\u0435\u0439\u043f\u043e\u043b\u0451\u0432"},{"count":1,"city":"\u041b\u0435\u0439\u043f\u043e\u043c\u043e\u0440\u0441\u043a"},{"count":1,"city":"\u041b\u0435\u0439\u043f\u043e\u043c\u0441\u043a"},{"count":1,"city":"\u041b\u0435\u0439\u043f\u043e\u0440\u0438\u0436"},{"count":1,"city":"\u041b\u0435\u0441\u043e\u0433\u043e\u0440\u0441\u043a"},{"count":1,"city":"\u041b\u0435\u0441\u043e\u0433\u0440\u0430\u0434"},{"count":1,"city":"\u041b\u0435\u0441\u043e\u043a\u0430\u0442\u0441\u043a"},{"count":1,"city":"\u041b\u0438\u0441\u0441\u0430\u0431\u0438\u0440\u0441\u043a"},{"count":1,"city":"\u041b\u0438\u0441\u0441\u0430\u0433\u0430\u043c\u0430"},{"count":1,"city":"\u041b\u0438\u0441\u0441\u0430\u043a\u044f\u0440\u0441\u043a"},{"count":1,"city":"\u041b\u0438\u0441\u0441\u0430\u043f\u043e\u043b\u044c"},{"count":1,"city":"\u041c\u043e\u0441\u043e\u0448\u0442\u0430\u0434\u0442"},{"count":1,"city":"\u041d\u043e\u0432\u043e\u0433\u0440\u0430\u0434"},{"count":1,"city":"\u041d\u043e\u0432\u043e\u0434\u043e\u0440\u0444"},{"count":1,"city":"\u041d\u043e\u0432\u043e\u043a\u0430\u0442\u0441\u043a"}]}
		if "Амстеранск" != CityDict.values[unpackCity(items[0].Key)] {
			t.Error("invalid sort Амстеранск")
		}
		if "Новокатск" != CityDict.values[unpackCity(items[39].Key)] {
			t.Error("invalid sort Новокатск")
		}
	}

	//434, limit 30, map[] -> 2
	_, _, _ = aggregateSample(t, &accounts, 30, 3, "keys", "status", "order", "-1", "city", "Лейпоград")
	// BODY   EXP:
	// {"groups":[
	// {"count":22,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b"},
	// {"count":16,"status":"\u0437\u0430\u043d\u044f\u0442\u044b"},
	// {"count":13,"status":"\u0432\u0441\u0451 \u0441\u043b\u043e\u0436\u043d\u043e"}]}
	// for _,item := range group.Sort(counters,30) {
	// 	group.DebugWriteJsonGroupItem(os.Stdout, item.Key,item.Count)
	// 	fmt.Println()
	// }

	//REQUEST  URI: /accounts/group/?query_id=600&order=-1&sex=f&limit=10&keys=country%2Cstatus
	//REQUEST BODY:
	//BODY   GOT: {"groups":[{"country":"","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","count":1454},{"country":"","status":"\u0437\u0430\u043d\u044f\u0442\u044b","count":843},{"country":"","status":"\u0432\u0441\u0451 \u0441\u043b\u043e\u0436\u043d\u043e","count":534},{"country":"\u0420\u043e\u0441\u0446\u0438\u044f","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","count":233},{"country":"\u0420\u043e\u0441\u0430\u043d\u0438\u044f","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","count":209},{"country":"\u041c\u0430\u043b\u043b\u044f\u043d\u0434\u0438\u044f","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","count":207},{"country":"\u041c\u0430\u043b\u0438\u0437\u0438\u044f","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","count":207},{"country":"\u041c\u0430\u043b\u0435\u0437\u0438\u044f","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","count":207},{"country":"\u0420\u043e\u0441\u0435\u0437\u0438\u044f","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","count":202},{"country":"\u041c\u0430\u043b\u0430\u043d\u0438\u044f","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","count":200}]}
	//BODY   EXP: {"groups":[{"count":1454,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b"},{"count":843,"status":"\u0437\u0430\u043d\u044f\u0442\u044b"},{"count":534,"status":"\u0432\u0441\u0451 \u0441\u043b\u043e\u0436\u043d\u043e"},{"count":233,"country":"\u0420\u043e\u0441\u0446\u0438\u044f","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b"},{"count":209,"country":"\u0420\u043e\u0441\u0430\u043d\u0438\u044f","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b"},{"count":207,"country":"\u041c\u0430\u043b\u043b\u044f\u043d\u0434\u0438\u044f","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b"},{"count":207,"country":"\u041c\u0430\u043b\u0438\u0437\u0438\u044f","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b"},{"count":207,"country":"\u041c\u0430\u043b\u0435\u0437\u0438\u044f","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b"},{"count":202,"country":"\u0420\u043e\u0441\u0435\u0437\u0438\u044f","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b"},{"count":200,"country":"\u041c\u0430\u043b\u0430\u043d\u0438\u044f","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b"}]}
	if _, _, group := aggregateSample(t, &accounts, 10, 210, "order", "-1", "sex", "f", "keys", "country,status"); group != nil {
		// for _, item := range group.Sort(counters, 10) {
		// 	group.DebugWriteJsonGroupItem(os.Stderr, item.Key, item.Count)
		// 	fmt.Println()
		// }
		if group == nil {
			t.Error("group == nil")
		}
	}

	// REQUEST  URI: /accounts/group/?keys=interests&order=1&query_id=575&limit=5&likes=7480
	// REQUEST BODY:
	// BODY   GOT: {"groups":[{"interests":"Facebook","count":1},{"interests":"\u0410\u0432\u0442\u043e\u043c\u043e\u0431\u0438\u043b\u0438","count":1},{"interests":"\u0411\u0435\u0433","count":1},{"interests":"\u0411\u043e\u0435\u0432\u044b\u0435 \u0438\u0441\u043a\u0443\u0441\u0441\u0442\u0432\u0430","count":1},{"interests":"\u0411\u0443\u0440\u0433\u0435\u0440\u044b","count":1}]}
	// BODY   EXP: {"groups":[{"count":1,"interests":"Facebook"},{"count":1,"interests":"PS3"},{"count":1,"interests":"\u0410\u0432\u0442\u043e\u043c\u043e\u0431\u0438\u043b\u0438"},{"count":1,"interests":"\u0410\u043f\u0435\u043b\u044c\u0441\u0438\u043d\u043e\u0432\u044b\u0439 \u0441\u043e\u043a"},{"count":1,"interests":"\u0411\u0435\u0433"}]}
	if _, counters, group := classifySample(t, &accounts, 10, 61, "order", "1", "likes", "7480", "keys", "interests"); group != nil {
		//for _, item := range group.Sort(counters, 61) {
		//	group.DebugWriteJsonGroupItem(os.Stderr, item.Key, item.Count)
		//	fmt.Println()
		//}
		if len(counters) == 0 {
			t.Error("len(counters) == 0 ")
		}
		if group == nil {
			t.Error("group == nil")
		}
	}

	if _, counters, group := aggregateSample(t, &accounts, 10, 90, "order", "1", "keys", "interests"); group != nil {
		//for _, item := range group.Sort(counters, 100) {
		//	group.DebugWriteJsonGroupItem(os.Stderr, item.Key, item.Count)
		//	fmt.Println()
		//}
		if group == nil {
			t.Error("group == nil")
		}
		if len(counters) == 0 {
			t.Error("len(counters) == 0 ")
		}

	}

	if _, counters, group := aggregateSample(t, &accounts, 10, 90, "order", "-1", "keys", "interests", "birth", "1988"); group != nil {
		//for _, item := range group.Sort(counters, 100) {
		//	group.DebugWriteJsonGroupItem(os.Stderr, item.Key, item.Count)
		//	fmt.Println()
		//}
		if group == nil {
			t.Error("group == nil")
		}
		if len(counters) == 0 {
			t.Error("len(counters) == 0 ")
		}
	}

	//REQUEST  URI: /accounts/group/?query_id=600&order=-1&sex=f&limit=10&keys=country%2Cstatus
	//REQUEST BODY:
	//BODY   GOT: {"groups":[{"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","count":2802},{"status":"\u0437\u0430\u043d\u044f\u0442\u044b","count":1721},{"status":"\u0432\u0441\u0451 \u0441\u043b\u043e\u0436\u043d\u043e","count":1095},{"country":"\u0420\u043e\u0441\u0446\u0438\u044f","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","count":421},{"country":"\u041c\u0430\u043b\u0438\u0437\u0438\u044f","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","count":421},{"country":"\u0420\u043e\u0441\u043c\u0430\u043b\u044c","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","count":415},{"country":"\u0420\u043e\u0441\u0430\u0442\u0440\u0438\u0441","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","count":411},{"country":"\u041c\u0430\u043b\u043b\u044f\u043d\u0434\u0438\u044f","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","count":410},{"country":"\u0420\u043e\u0441\u0430\u043d\u0438\u044f","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","count":408},{"country":"\u041c\u0430\u043b\u0435\u0437\u0438\u044f","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","count":402}]}
	//BODY   EXP: {"groups":[{"count":1454,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b"},{"count":843,"status":"\u0437\u0430\u043d\u044f\u0442\u044b"},{"count":534,"status":"\u0432\u0441\u0451 \u0441\u043b\u043e\u0436\u043d\u043e"},{"count":233,"country":"\u0420\u043e\u0441\u0446\u0438\u044f","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b"},{"count":209,"country":"\u0420\u043e\u0441\u0430\u043d\u0438\u044f","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b"},{"count":207,"country":"\u041c\u0430\u043b\u043b\u044f\u043d\u0434\u0438\u044f","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b"},{"count":207,"country":"\u041c\u0430\u043b\u0438\u0437\u0438\u044f","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b"},{"count":207,"country":"\u041c\u0430\u043b\u0435\u0437\u0438\u044f","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b"},{"count":202,"country":"\u0420\u043e\u0441\u0435\u0437\u0438\u044f","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b"},{"count":200,"country":"\u041c\u0430\u043b\u0430\u043d\u0438\u044f","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b"}]}
	if _, sorted, group := aggregateSample(t, &accounts, 10, 210, "order", "-1", "keys", "country,status", "sex", "f"); group != nil {
		if sorted[0].Count != 1454 {
			t.Error("sorted[0].Count != 1454")
		}
		//for _, item := range sorted {
		//	group.DebugWriteJsonGroupItem(os.Stderr, item.Key, item.Count)
		//	fmt.Println()
		//}
	}

	//REQUEST  URI: /accounts/group/?joined=2015&order=-1&limit=25&keys=city%2Csex&query_id=378&
	//REQUEST BODY:
	//BODY   GOT: {"groups":[{"sex":"m","count":1121},{"city":"\u041c\u043e\u0441\u0438\u043d\u0441\u043a","sex":"m","count":33},{"city":"\u041b\u0435\u0439\u043f\u043e\u0440\u0438\u0436","sex":"m","count":28},{"city":"\u041d\u043e\u0432\u043e\u0433\u0440\u0430\u0434","sex":"m","count":27},{"city":"\u041b\u0438\u0441\u0441\u0430\u0433\u0430\u043c\u0430","sex":"m","count":27},{"city":"\u0411\u0435\u043b\u0430\u0442\u0441\u043a","sex":"m","count":27},{"city":"\u0417\u0435\u043b\u0435\u043d\u043e\u0440\u0435\u0447\u0441\u043a","sex":"m","count":25},{"city":"\u0421\u0432\u0435\u0442\u043b\u043e\u043a\u0435\u043b\u043e\u043d\u0430","sex":"m","count":24},{"city":"\u0412\u0435\u043b\u0438\u043a\u043e\u0434\u0430\u043c","sex":"m","count":24},{"city":"\u0411\u0430\u0440\u0441\u043e\u0431\u0438\u0440\u0441\u043a","sex":"m","count":24},{"city":"\u0420\u043e\u0441\u043e\u0440\u0438\u0436","sex":"m","count":22},{"city":"\u0412\u0430\u0440\u043e\u0440\u0435\u0447\u0441\u043a","sex":"m","count":22},{"city":"\u041d\u043e\u0432\u043e\u043b\u0435\u0441\u0441\u043a","sex":"m","count":21},{"city":"\u041c\u043e\u0441\u043e\u0433\u043e\u0440\u043e\u0434","sex":"m","count":21},{"city":"\u0412\u0430\u0440\u043e\u0448\u0442\u0430\u0434\u0442","sex":"m","count":21},{"city":"\u0412\u0430\u0440\u043e\u0431\u0441\u043a","sex":"m","count":21},{"city":"\u0420\u043e\u0441\u043e\u0440\u0435\u0447\u0441\u043a","sex":"m","count":20},{"city":"\u0412\u0430\u0440\u043e\u0434\u0430\u043c","sex":"m","count":20},{"city":"\u0417\u0435\u043b\u0435\u043d\u043e\u043b\u0430\u043c\u0441\u043a","sex":"m","count":19},{"city":"\u0412\u043e\u043b\u043e\u0440\u0435\u0447\u0441\u043a","sex":"m","count":19},{"city":"\u041d\u043e\u0432\u043e\u043a\u0435\u043d\u0441\u043a","sex":"m","count":18},{"city":"\u041c\u043e\u0441\u043e\u043b\u0451\u0432","sex":"m","count":18},{"city":"\u0412\u043e\u043b\u043e\u043a\u0430\u043c\u0441\u043a","sex":"m","count":18},{"city":"\u0412\u0430\u0440\u0438\u043d\u0441\u043a","sex":"m","count":18},{"city":"\u0410\u043c\u0441\u0442\u0435\u0440\u043e\u0433\u043e\u0440\u0441\u043a","sex":"m","count":18}]}
	//BODY   EXP: {"groups":[{"count":1121,"sex":"f"},{"count":33,"city":"\u041c\u043e\u0441\u0438\u043d\u0441\u043a","sex":"f"},{"count":28,"city":"\u041b\u0435\u0439\u043f\u043e\u0440\u0438\u0436","sex":"f"},{"count":27,"city":"\u041d\u043e\u0432\u043e\u0433\u0440\u0430\u0434","sex":"f"},{"count":27,"city":"\u041b\u0438\u0441\u0441\u0430\u0433\u0430\u043c\u0430","sex":"f"},{"count":27,"city":"\u0411\u0435\u043b\u0430\u0442\u0441\u043a","sex":"f"},{"count":25,"city":"\u0417\u0435\u043b\u0435\u043d\u043e\u0440\u0435\u0447\u0441\u043a","sex":"f"},{"count":24,"city":"\u0421\u0432\u0435\u0442\u043b\u043e\u043a\u0435\u043b\u043e\u043d\u0430","sex":"f"},{"count":24,"city":"\u0412\u0435\u043b\u0438\u043a\u043e\u0434\u0430\u043c","sex":"f"},{"count":24,"city":"\u0411\u0430\u0440\u0441\u043e\u0431\u0438\u0440\u0441\u043a","sex":"f"},{"count":22,"city":"\u0420\u043e\u0441\u043e\u0440\u0438\u0436","sex":"f"},{"count":22,"city":"\u0412\u0430\u0440\u043e\u0440\u0435\u0447\u0441\u043a","sex":"f"},{"count":21,"city":"\u041d\u043e\u0432\u043e\u043b\u0435\u0441\u0441\u043a","sex":"f"},{"count":21,"city":"\u041c\u043e\u0441\u043e\u0433\u043e\u0440\u043e\u0434","sex":"f"},{"count":21,"city":"\u0412\u0430\u0440\u043e\u0448\u0442\u0430\u0434\u0442","sex":"f"},{"count":21,"city":"\u0412\u0430\u0440\u043e\u0431\u0441\u043a","sex":"f"},{"count":20,"city":"\u0420\u043e\u0441\u043e\u0440\u0435\u0447\u0441\u043a","sex":"f"},{"count":20,"city":"\u0412\u0430\u0440\u043e\u0434\u0430\u043c","sex":"f"},{"count":19,"city":"\u0417\u0435\u043b\u0435\u043d\u043e\u043b\u0430\u043c\u0441\u043a","sex":"f"},{"count":19,"city":"\u0412\u043e\u043b\u043e\u0440\u0435\u0447\u0441\u043a","sex":"f"},{"count":18,"city":"\u041d\u043e\u0432\u043e\u043a\u0435\u043d\u0441\u043a","sex":"f"},{"count":18,"city":"\u041c\u043e\u0441\u043e\u043b\u0451\u0432","sex":"f"},{"count":18,"city":"\u0412\u043e\u043b\u043e\u043a\u0430\u043c\u0441\u043a","sex":"f"},{"count":18,"city":"\u0412\u0430\u0440\u0438\u043d\u0441\u043a","sex":"f"},{"count":18,"city":"\u0410\u043c\u0441\u0442\u0435\u0440\u043e\u0433\u043e\u0440\u0441\u043a","sex":"f"}]}
	if _, sorted, group := aggregateSample(t, &accounts, 10, 283, "order", "-1", "keys", "sex,city", "joined", "2015", "sex", "f"); group != nil {
		if sorted[0].Count != 1121 {
			t.Error("sorted[0].Count != 1121")
		}
		if sorted[0].Key&1 != 1 {
			t.Error("sex not f")
		}
	}

	//REQUEST  URI: /accounts/group/?interests=%D0%9A%D1%83%D1%80%D0%B8%D1%86%D0%B0&order=-1&query_id=681&limit=5&keys=city
	//REQUEST BODY:
	//BODY   GOT: {"groups":[{"count":362},{"city":"\u0412\u0435\u043b\u0438\u043a\u043e\u0434\u0430\u043c","count":13},{"city":"\u041c\u043e\u0441\u043e\u0433\u043e\u0440\u043e\u0434","count":12},{"city":"\u0412\u043e\u043b\u043e\u0440\u0435\u0447\u0441\u043a","count":11},{"city":"\u0420\u043e\u0441\u043e\u0440\u0438\u0436","count":10}]}
	//BODY   EXP: {"groups":[{"count":348},{"count":16,"city":"\u041d\u043e\u0432\u043e\u0433\u0440\u0430\u0434"},{"count":12,"city":"\u0417\u0435\u043b\u0435\u043d\u043e\u0440\u0435\u0447\u0441\u043a"},{"count":11,"city":"\u041c\u043e\u0441\u043e\u0433\u043e\u0440\u043e\u0434"},{"count":11,"city":"\u041b\u0438\u0441\u0441\u0430\u0431\u0438\u0440\u0441\u043a"}]}
	if _, result, group := aggregateSample(t, &accounts, 10, -1, "order", "-1", "interests", "Курица", "keys", "city"); group != nil {
		if result[0].Count != 348 && result[1].Count != 16 {
			t.Error("failed 1")
		}

	}

	//REQUEST  URI: /accounts/group/?joined=2015&order=-1&limit=25&keys=city%2Csex&query_id=378&sex=f
	//REQUEST BODY:
	//BODY   GOT: {"groups":[{"sex":"m","count":1121},{"city":"\u041c\u043e\u0441\u0438\u043d\u0441\u043a","sex":"m","count":33},{"city":"\u041b\u0435\u0439\u043f\u043e\u0440\u0438\u0436","sex":"m","count":28},{"city":"\u041d\u043e\u0432\u043e\u0433\u0440\u0430\u0434","sex":"m","count":27},{"city":"\u041b\u0438\u0441\u0441\u0430\u0433\u0430\u043c\u0430","sex":"m","count":27},{"city":"\u0411\u0435\u043b\u0430\u0442\u0441\u043a","sex":"m","count":27},{"city":"\u0417\u0435\u043b\u0435\u043d\u043e\u0440\u0435\u0447\u0441\u043a","sex":"m","count":25},{"city":"\u0421\u0432\u0435\u0442\u043b\u043e\u043a\u0435\u043b\u043e\u043d\u0430","sex":"m","count":24},{"city":"\u0412\u0435\u043b\u0438\u043a\u043e\u0434\u0430\u043c","sex":"m","count":24},{"city":"\u0411\u0430\u0440\u0441\u043e\u0431\u0438\u0440\u0441\u043a","sex":"m","count":24},{"city":"\u0420\u043e\u0441\u043e\u0440\u0438\u0436","sex":"m","count":22},{"city":"\u0412\u0430\u0440\u043e\u0440\u0435\u0447\u0441\u043a","sex":"m","count":22},{"city":"\u041d\u043e\u0432\u043e\u043b\u0435\u0441\u0441\u043a","sex":"m","count":21},{"city":"\u041c\u043e\u0441\u043e\u0433\u043e\u0440\u043e\u0434","sex":"m","count":21},{"city":"\u0412\u0430\u0440\u043e\u0448\u0442\u0430\u0434\u0442","sex":"m","count":21},{"city":"\u0412\u0430\u0440\u043e\u0431\u0441\u043a","sex":"m","count":21},{"city":"\u0420\u043e\u0441\u043e\u0440\u0435\u0447\u0441\u043a","sex":"m","count":20},{"city":"\u0412\u0430\u0440\u043e\u0434\u0430\u043c","sex":"m","count":20},{"city":"\u0417\u0435\u043b\u0435\u043d\u043e\u043b\u0430\u043c\u0441\u043a","sex":"m","count":19},{"city":"\u0412\u043e\u043b\u043e\u0440\u0435\u0447\u0441\u043a","sex":"m","count":19},{"city":"\u041d\u043e\u0432\u043e\u043a\u0435\u043d\u0441\u043a","sex":"m","count":18},{"city":"\u041c\u043e\u0441\u043e\u043b\u0451\u0432","sex":"m","count":18},{"city":"\u0412\u043e\u043b\u043e\u043a\u0430\u043c\u0441\u043a","sex":"m","count":18},{"city":"\u0412\u0430\u0440\u0438\u043d\u0441\u043a","sex":"m","count":18},{"city":"\u0410\u043c\u0441\u0442\u0435\u0440\u043e\u0433\u043e\u0440\u0441\u043a","sex":"m","count":18}]}
	//BODY   EXP: {"groups":[{"count":1121,"sex":"f"},{"count":33,"city":"\u041c\u043e\u0441\u0438\u043d\u0441\u043a","sex":"f"},{"count":28,"city":"\u041b\u0435\u0439\u043f\u043e\u0440\u0438\u0436","sex":"f"},{"count":27,"city":"\u041d\u043e\u0432\u043e\u0433\u0440\u0430\u0434","sex":"f"},{"count":27,"city":"\u041b\u0438\u0441\u0441\u0430\u0433\u0430\u043c\u0430","sex":"f"},{"count":27,"city":"\u0411\u0435\u043b\u0430\u0442\u0441\u043a","sex":"f"},{"count":25,"city":"\u0417\u0435\u043b\u0435\u043d\u043e\u0440\u0435\u0447\u0441\u043a","sex":"f"},{"count":24,"city":"\u0421\u0432\u0435\u0442\u043b\u043e\u043a\u0435\u043b\u043e\u043d\u0430","sex":"f"},{"count":24,"city":"\u0412\u0435\u043b\u0438\u043a\u043e\u0434\u0430\u043c","sex":"f"},{"count":24,"city":"\u0411\u0430\u0440\u0441\u043e\u0431\u0438\u0440\u0441\u043a","sex":"f"},{"count":22,"city":"\u0420\u043e\u0441\u043e\u0440\u0438\u0436","sex":"f"},{"count":22,"city":"\u0412\u0430\u0440\u043e\u0440\u0435\u0447\u0441\u043a","sex":"f"},{"count":21,"city":"\u041d\u043e\u0432\u043e\u043b\u0435\u0441\u0441\u043a","sex":"f"},{"count":21,"city":"\u041c\u043e\u0441\u043e\u0433\u043e\u0440\u043e\u0434","sex":"f"},{"count":21,"city":"\u0412\u0430\u0440\u043e\u0448\u0442\u0430\u0434\u0442","sex":"f"},{"count":21,"city":"\u0412\u0430\u0440\u043e\u0431\u0441\u043a","sex":"f"},{"count":20,"city":"\u0420\u043e\u0441\u043e\u0440\u0435\u0447\u0441\u043a","sex":"f"},{"count":20,"city":"\u0412\u0430\u0440\u043e\u0434\u0430\u043c","sex":"f"},{"count":19,"city":"\u0417\u0435\u043b\u0435\u043d\u043e\u043b\u0430\u043c\u0441\u043a","sex":"f"},{"count":19,"city":"\u0412\u043e\u043b\u043e\u0440\u0435\u0447\u0441\u043a","sex":"f"},{"count":18,"city":"\u041d\u043e\u0432\u043e\u043a\u0435\u043d\u0441\u043a","sex":"f"},{"count":18,"city":"\u041c\u043e\u0441\u043e\u043b\u0451\u0432","sex":"f"},{"count":18,"city":"\u0412\u043e\u043b\u043e\u043a\u0430\u043c\u0441\u043a","sex":"f"},{"count":18,"city":"\u0412\u0430\u0440\u0438\u043d\u0441\u043a","sex":"f"},{"count":18,"city":"\u0410\u043c\u0441\u0442\u0435\u0440\u043e\u0433\u043e\u0440\u0441\u043a","sex":"f"}]}
	if _, sorted, group := aggregateSample(t, &accounts, 10, -1, "order", "-1", "keys", "city,sex", "joined", "2015", "sex", "f"); group != nil {
		if sorted[0].Count != 1121 {
			t.Error("sorted[0].Count != 1121")
		}
		if sorted[0].Key&1 != 1 {
			t.Error("sex !f")
		}
	}

	str, _ := url.PathUnescape("/accounts/group/?interests=%D0%9A%D1%83%D1%80%D0%B8%D1%86%D0%B0&order=-1&query_id=681&limit=5&keys=city")
	fmt.Println(str)
	//REQUEST  URI: /accounts/group/?interests=%D0%9A%D1%83%D1%80%D0%B8%D1%86%D0%B0&order=-1&query_id=681&limit=5&keys=city
	//REQUEST BODY:
	//BODY   GOT: {"groups":[{"count":362},{"city":"\u0412\u0435\u043b\u0438\u043a\u043e\u0434\u0430\u043c","count":13},{"city":"\u041c\u043e\u0441\u043e\u0433\u043e\u0440\u043e\u0434","count":12},{"city":"\u0412\u043e\u043b\u043e\u0440\u0435\u0447\u0441\u043a","count":11},{"city":"\u0420\u043e\u0441\u043e\u0440\u0438\u0436","count":10}]}
	//BODY   EXP: {"groups":[{"count":348},{"count":16,"city":"\u041d\u043e\u0432\u043e\u0433\u0440\u0430\u0434"},{"count":12,"city":"\u0417\u0435\u043b\u0435\u043d\u043e\u0440\u0435\u0447\u0441\u043a"},{"count":11,"city":"\u041c\u043e\u0441\u043e\u0433\u043e\u0440\u043e\u0434"},{"count":11,"city":"\u041b\u0438\u0441\u0441\u0430\u0431\u0438\u0440\u0441\u043a"}]}
	if _, result, group := aggregateSample(t, &accounts, 5, -1, "order", "-1", "interests", "Курица", "keys", "city"); group != nil {
		if result[0].Count != 348 || result[1].Count != 16 {
			t.Error("failed 1")
		}
		if decode("city", result[4].Key) != "Лиссабирск" {
			t.Error("ждали Лиссабирск, получили иное")
		}
		for _, item := range result {
			group.DebugWriteJsonGroupItem(os.Stderr, item.Key, item.Count)
			fmt.Println()
		}

	}

	str, _ = url.PathUnescape("/accounts/group/?country=%D0%9C%D0%B0%D0%BB%D0%BC%D0%B0%D0%BB%D1%8C&order=-1&query_id=585&keys=interests&birth=1986&limit=40")
	fmt.Println(str)

	// REQUEST  URI: /accounts/group/?country=%D0%9C%D0%B0%D0%BB%D0%BC%D0%B0%D0%BB%D1%8C&order=-1&query_id=585&keys=interests&birth=1986&limit=40
	// REQUEST BODY:
	// BODY   GOT: {"groups":[{"interests":"\u0412\u043a\u0443\u0441\u043d\u043e \u043f\u043e\u0435\u0441\u0442\u044c","count":2},{"interests":"\u0427\u0443\u0434\u0430\u043a","count":1},{"interests":"\u0427\u0435\u0441\u0442\u043d\u043e\u0441\u0442\u044c","count":1},{"interests":"\u0424\u0438\u0442\u043d\u0435\u0441","count":1},{"interests":"\u0422\u0443\u0444\u043b\u0438","count":1},{"interests":"\u0422\u0430\u0442\u0443\u0438\u0440\u043e\u0432\u043a\u0438","count":1},{"interests":"\u0421\u043f\u043e\u0440\u0442\u0438\u0432\u043d\u044b\u0435 \u043c\u0430\u0448\u0438\u043d\u044b","count":1},{"interests":"\u0421\u043e\u043d","count":1},{"interests":"\u0421\u0430\u043b\u0430\u0442\u044b","count":1},{"interests":"\u0420\u044b\u0431\u0430","count":1},{"interests":"\u041f\u043b\u0430\u0432\u0430\u043d\u0438\u0435","count":1},{"interests":"\u041f\u0430\u0441\u0442\u0430","count":1},{"interests":"\u041e\u0431\u0449\u0435\u043d\u0438\u0435","count":1},{"interests":"\u041e\u0431\u043d\u0438\u043c\u0430\u0448\u043a\u0438","count":1},{"interests":"\u041c\u043e\u0442\u043e\u0441\u043f\u043e\u0440\u0442","count":1},{"interests":"\u041b\u0435\u0442\u043e","count":1},{"interests":"\u041a\u0440\u0430\u0441\u043d\u043e\u0435 \u0432\u0438\u043d\u043e","count":1},{"interests":"\u0418\u0442\u0430\u043b\u044c\u044f\u043d\u0441\u043a\u0430\u044f \u043a\u0443\u0445\u043d\u044f","count":1},{"interests":"\u0416\u0438\u0437\u043d\u044c","count":1},{"interests":"\u0414\u0440\u0443\u0437\u044c\u044f \u0438 \u0411\u043b\u0438\u0437\u043a\u0438\u0435","count":1},{"interests":"\u0411\u043e\u043a\u0441","count":1},{"interests":"\u0411\u043e\u0435\u0432\u044b\u0435 \u0438\u0441\u043a\u0443\u0441\u0441\u0442\u0432\u0430","count":1},{"interests":"\u0410\u0432\u0442\u043e\u043c\u043e\u0431\u0438\u043b\u0438","count":1},{"interests":"South Park","count":1},{"interests":"PS3","count":1},{"interests":"50 Cent","count":1}]}
	// BODY   EXP: {"groups":[{"count":2,"interests":"\u0424\u0438\u0442\u043d\u0435\u0441"},{"count":1,"interests":"\u042e\u043c\u043e\u0440"},{"count":1,"interests":"\u0422\u0435\u043a\u0438\u043b\u0430"},{"count":1,"interests":"\u0422\u0430\u043d\u0446\u0435\u0432\u0430\u043b\u044c\u043d\u0430\u044f"},{"count":1,"interests":"\u0421\u0442\u0435\u0439\u043a"},{"count":1,"interests":"\u0421\u043f\u043e\u0440\u0442\u0438\u0432\u043d\u044b\u0435 \u043c\u0430\u0448\u0438\u043d\u044b"},{"count":1,"interests":"\u0421\u0438\u043c\u043f\u0441\u043e\u043d\u044b"},{"count":1,"interests":"\u0420\u044d\u043f"},{"count":1,"interests":"\u0420\u044b\u0431\u0430"},{"count":1,"interests":"\u0420\u0435\u0433\u0433\u0438"},{"count":1,"interests":"\u041f\u0440\u043e\u0433\u0443\u043b\u043a\u0438 \u043f\u043e \u043f\u043b\u044f\u0436\u0443"},{"count":1,"interests":"\u041d\u0430 \u043e\u0442\u043a\u0440\u044b\u0442\u043e\u043c \u0432\u043e\u0437\u0434\u0443\u0445\u0435"},{"count":1,"interests":"\u041c\u0430\u0441\u0441\u0430\u0436"},{"count":1,"interests":"\u041a\u043e\u0444\u0435"},{"count":1,"interests":"\u0418\u043d\u0442\u0435\u0440\u043d\u0435\u0442"},{"count":1,"interests":"\u0417\u0434\u043e\u0440\u043e\u0432\u044c\u0435"},{"count":1,"interests":"\u0416\u0438\u0437\u043d\u044c"},{"count":1,"interests":"\u0414\u0440\u0443\u0437\u044c\u044f"},{"count":1,"interests":"\u0414\u0435\u0432\u0443\u0448\u043a\u0438"},{"count":1,"interests":"\u0412\u044b\u0445\u043e\u0434\u043d\u044b\u0435"},{"count":1,"interests":"\u0411\u0443\u0440\u0433\u0435\u0440\u044b"},{"count":1,"interests":"\u0411\u043e\u043a\u0441"},{"count":1,"interests":"\u0411\u0430\u0441\u043a\u0435\u0442\u0431\u043e\u043b"},{"count":1,"interests":"\u0410\u043f\u0435\u043b\u044c\u0441\u0438\u043d\u043e\u0432\u044b\u0439 \u0441\u043e\u043a"},{"count":1,"interests":"PS3"},{"count":1,"interests":"50 Cent"}]}
	if _, result, group := aggregateSample(t, &accounts, 10, -1, "order", "-1", "country", "Малмаль", "birth", "1986", "keys", "interests"); group != nil {
		if result[0].Count != 2 || decode("interests", result[0].Key) != "Фитнес" {
			t.Error("failed 2")
		}
	}

	// awited 50 of:
	// {"status":свободны,"count":199}
	// {"status":заняты,"count":126}
	// {"status":всё сложно,"count":91}
	// ...
	// {"city":Новокатск,"status":свободны,"count":3}
	// {"city":Новоград,"status":заняты,"count":3}
	// {"city":Мососинки,"status":свободны,"count":3}
	//  BODY   EXP: {"groups":[{"count":199,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b"},{"count":126,"status":"\u0437\u0430\u043d\u044f\u0442\u044b"},{"count":91,"status":"\u0432\u0441\u0451 \u0441\u043b\u043e\u0436\u043d\u043e"},{"count":9,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u0420\u043e\u0442\u0442\u0435\u0440\u043e\u043f\u043e\u043b\u044c"},{"count":8,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u0420\u043e\u0442\u0442\u0435\u0440\u043e\u0448\u0442\u0430\u0434\u0442"},{"count":7,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u041c\u043e\u0441\u043e\u0433\u043e\u0440\u043e\u0434"},{"count":7,"status":"\u0432\u0441\u0451 \u0441\u043b\u043e\u0436\u043d\u043e","city":"\u041b\u0435\u0439\u043f\u043e\u0440\u0438\u0436"},{"count":7,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u0412\u0435\u043b\u0438\u043a\u043e\u0434\u0430\u043c"},{"count":6,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u0420\u043e\u0441\u043e\u0440\u0438\u0436"},{"count":6,"status":"\u0437\u0430\u043d\u044f\u0442\u044b","city":"\u041d\u043e\u0432\u043e\u043a\u0430\u0442\u0441\u043a"},{"count":6,"status":"\u0437\u0430\u043d\u044f\u0442\u044b","city":"\u041b\u0438\u0441\u0441\u0430\u0433\u0430\u043c\u0430"},{"count":6,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u0412\u0430\u0440\u0438\u043d\u0441\u043a"},{"count":5,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u0421\u0432\u0435\u0442\u043b\u043e\u043a\u043e\u043c\u0441\u043a"},{"count":5,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u0421\u0432\u0435\u0442\u043b\u043e\u043a\u0435\u043b\u043e\u043d\u0430"},{"count":5,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u041d\u043e\u0432\u043e\u0433\u0440\u0430\u0434"},{"count":5,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u041c\u043e\u0441\u044f\u0440\u0441\u043a"},{"count":5,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u041c\u043e\u0441\u0438\u043d\u0441\u043a"},{"count":5,"status":"\u0437\u0430\u043d\u044f\u0442\u044b","city":"\u041a\u0440\u043e\u043d\u043e\u043c\u0441\u043a"},{"count":5,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u0412\u043e\u043b\u043e\u0440\u0435\u0447\u0441\u043a"},{"count":5,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u0412\u043e\u043b\u043e\u0431\u0438\u0440\u0441\u043a"},{"count":5,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u0411\u0435\u043b\u0430\u0442\u0441\u043a"},{"count":5,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u0410\u043c\u0441\u0442\u0435\u0440\u043e\u0431\u043e\u043d"},{"count":4,"status":"\u0432\u0441\u0451 \u0441\u043b\u043e\u0436\u043d\u043e","city":"\u0420\u043e\u0442\u0442\u0435\u0440\u043e\u043f\u043e\u043b\u044c"},{"count":4,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u0420\u043e\u0442\u0442\u0435\u0440\u0438\u043d\u0441\u043a"},{"count":4,"status":"\u0437\u0430\u043d\u044f\u0442\u044b","city":"\u0420\u043e\u0441\u043e\u0440\u0438\u0436"},{"count":4,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u0420\u043e\u0441\u043e\u0440\u0435\u0447\u0441\u043a"},{"count":4,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u041b\u0438\u0441\u0441\u0430\u0433\u0430\u043c\u0430"},{"count":4,"status":"\u0437\u0430\u043d\u044f\u0442\u044b","city":"\u041a\u0440\u043e\u043d\u043e\u043b\u0451\u0432"},{"count":4,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u041a\u0440\u043e\u043d\u043e\u0434\u0430\u043c"},{"count":4,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u0417\u0435\u043b\u0435\u043d\u043e\u043b\u0430\u043c\u0441\u043a"},{"count":4,"status":"\u0437\u0430\u043d\u044f\u0442\u044b","city":"\u0417\u0435\u043b\u0435\u043d\u043e\u043b\u0430\u043c\u0441\u043a"},{"count":4,"status":"\u0437\u0430\u043d\u044f\u0442\u044b","city":"\u0412\u043e\u043b\u043e\u0431\u0438\u0440\u0441\u043a"},{"count":4,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u0412\u0430\u0440\u043e\u0440\u0435\u0447\u0441\u043a"},{"count":4,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u0412\u0430\u0440\u043e\u043a\u0430\u043c\u0441\u043a"},{"count":4,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u0412\u0430\u0440\u043e\u0434\u0430\u043c"},{"count":4,"status":"\u0437\u0430\u043d\u044f\u0442\u044b","city":"\u0411\u0435\u043b\u0430\u0442\u0441\u043a"},{"count":4,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u0411\u0430\u0440\u0441\u043e\u0433\u0430\u043c\u0430"},{"count":3,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u0421\u0435\u0432\u0435\u0440\u043e\u0434\u043e\u0440\u0444"},{"count":3,"status":"\u0437\u0430\u043d\u044f\u0442\u044b","city":"\u0421\u0432\u0435\u0442\u043b\u043e\u043a\u0435\u043b\u043e\u043d\u0430"},{"count":3,"status":"\u0437\u0430\u043d\u044f\u0442\u044b","city":"\u0420\u043e\u0442\u0442\u0435\u0440\u043e\u0448\u0442\u0430\u0434\u0442"},{"count":3,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u0420\u043e\u0442\u0442\u0435\u0440\u043e\u0440\u0435\u0447\u0441\u043a"},{"count":3,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u0420\u043e\u0442\u0442\u0435\u0440\u043e\u043c\u0441\u043a"},{"count":3,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u0420\u043e\u0442\u0442\u0435\u0440\u043e\u0433\u0430\u043c\u0430"},{"count":3,"status":"\u0437\u0430\u043d\u044f\u0442\u044b","city":"\u0420\u043e\u0442\u0442\u0435\u0440\u0435\u043d\u0441\u043a"},{"count":3,"status":"\u0437\u0430\u043d\u044f\u0442\u044b","city":"\u0420\u043e\u0442\u0442\u0435\u0440\u0430\u0442\u0441\u043a"},{"count":3,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u041d\u043e\u0432\u043e\u043a\u043e\u0432\u043e"},{"count":3,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u041d\u043e\u0432\u043e\u043a\u0435\u043d\u0441\u043a"},{"count":3,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u041d\u043e\u0432\u043e\u043a\u0430\u0442\u0441\u043a"},{"count":3,"status":"\u0437\u0430\u043d\u044f\u0442\u044b","city":"\u041d\u043e\u0432\u043e\u0433\u0440\u0430\u0434"},{"count":3,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","city":"\u041c\u043e\u0441\u043e\u0441\u0438\u043d\u043a\u0438"}]}

	//556, limit 50, map[order:-1 keys:city,status birth:1995 sex:m] -> 633
	if _, result, group := aggregateSample(t, &accounts, 50, 374, "order", "-1", "keys", "city,status", "birth", "1995", "sex", "m"); group != nil {
		//result := group.Sort(counters, 50)
		/*
			for _, item := range result {
				group.DebugWriteJsonGroupItem(os.Stderr, item.Key, item.Count)
				fmt.Println()
			}
		*/

		if len(result) != 50 {
			t.Error("failed len(result) != 50")
		}

		if result[0].Count != 199 || result[1].Count != 126 || result[49].Count != 3 {
			t.Errorf("failed result %v", result)
		}

	}

	// accounts/group/?order=1&query_id=685&keys=city,status&interests=Хип+Хоп&birth=1984&limit=5
	if _, result, group := aggregateSample(t, &accounts, 50, 4, "order", "1", "keys", "city,status", "birth", "1984", "interests", "Хип Хоп"); group != nil {
		if len(result) != 4 {
			t.Error("len(result) != 4")
		}
		if result[0].Count != 1 || result[3].Count != 1 {
			t.Error("result[0].Count != 1 || result[3].Count != 1")
		}
	}

	// /accounts/group/?query_id=1539&order=1&birth=1994&limit=40&keys=interests
	if _, result, group := aggregateSample(t, &accounts, 40, 90, "order", "1", "keys", "interests", "birth", "1994"); group != nil {

		//for _, item := range result {
		//	group.DebugWriteJsonGroupItem(os.Stderr, item.Key, item.Count)
		//	fmt.Println()
		//}

		if result[0].Count != 83 || result[39].Count != 98 {
			t.Error("result[0].Count != 83 || result[39].Count != 98")
		}
	}

}

func classifySample(t *testing.T, accounts *[]Account, limit, awaited int, conditions ...string) (map[string]string, []GroupItem, *Group) {
	var params = make(map[string]string)
	for i, cond := range conditions {
		if i%2 == 0 {
			params[cond] = conditions[i+1]
		}
	}

	group := Group{Limit: limit}
	if err := group.FromParams(params); err != nil {
		t.Errorf("FromParams failed, %v, %v\n", params, err)
	}

	result := group.Classify(accounts, false)

	return params, result, &group
}

func aggregateSample(t *testing.T, _ *[]Account, limit, awaited int, conditions ...string) (map[string]string, []GroupItem, *Group) {
	var params = make(map[string]string)
	for i, cond := range conditions {
		if i%2 == 0 {
			params[cond] = conditions[i+1]
		}
	}

	group := Group{Limit: limit}
	if err := group.FromParams(params); err != nil {
		t.Errorf("FromParams failed, %v, %v\n", params, err)
	}

	result, err := group.aggregate(false)

	if !err {
		t.Errorf("sample aggragate unsupported for params: %v, %v\n", params, err)
	}
	//else if awaited != -1 && len(result) != awaited {
	//	t.Errorf("sample aggragate failed, awaited %d, got %v -> %d\n", awaited, params, len(result))
	//} else {
	//	fmt.Printf("OK %v -> %d\n", params, len(result))
	//}

	return params, result, &group
}

func loadTestData(t *testing.T) []Account {
	const capacity = 30000
	MakeIndexes(capacity)
	InitStore(capacity)
	_ = Store.LoadData("/tmp/data-test-2612/")
	return Store.Accounts
}

/*
func loadTestData2(t *testing.T) []Account {
	const capacity = 30000
	MakeIndexes(capacity)
	accounts := make([]Account, capacity)
	var err error
	if MaxId, _, err = LoadDataArray2("/Users/den/tmp/data-test-2612/", &accounts); err != nil {
		t.Error("load error", err)
	}
	return accounts
}


func TestLoad(t *testing.T) {

	a1 := loadTestData(t)
	a2 := loadTestData2(t)

	if a1[0].data != a2[0].data {
		t.Errorf("load error\n%v\n%v\n", a1[0], a2[0])
	}

}

*/
