package main

import (
	"dsmnko.org/hlc18"
	"fmt"
	"testing"
	"time"
)

func TestDatesTs(t *testing.T) {
	fmt.Printf("date %v\n", time.Unix(1391299200, 0).UTC())
	fmt.Printf("bitrh %v\n", time.Unix(596264959, 0).UTC())

	fmt.Printf("like.ts %v\n", time.Unix(1480017567, 0).UTC())

	fmt.Printf("joined %v\n", time.Unix(1332115200, 0).UTC())
	fmt.Printf("now %v\n", time.Unix(1545834028, 0).UTC())
	fmt.Printf("now-real %d\n", time.Now().UTC().Unix())

	fmt.Printf("joined-max %d, %v\n", hlc18.MaxJoinedMinPremium, time.Unix(int64(hlc18.MaxJoinedMinPremium), 0).UTC())
	fmt.Printf("joined-min %d, %v\n", hlc18.MinJoined, time.Unix(int64(hlc18.MinJoined), 0).UTC())

	//fmt.Printf("min-premium %v\n", timeStampOf("2018-01-01T00:00:00Z"))
	//parsed := timeStampOf("1950-01-01T00:00:00Z")
	//fmt.Printf("%v, %v, %v\n", time.Unix(803238391, 0).UTC(), parsed, uint32(parsed))
	//fmt.Printf("min %v\n", time.Unix(int64(parsed), 0).UTC())
	//fmt.Printf("uint32 min %v\n", time.Unix(int64(uint32(parsed)), 0).UTC())

	//fmt.Printf("joined-min %d, %v\n", minJoined, time.Unix(int64(minJoined), 0).UTC())
	//fmt.Printf("joined-max %d, %v\n", maxJoinedMinPremium, time.Unix(int64(maxJoinedMinPremium), 0).UTC())

	//	fmt.Println("Time parsing");
	//	dateString := "2014-11-12T00:00:00.00	0Z"
	//	t, e := time.Parse(time.RFC3339,dateString) {
	//	}
}

/*
func TestLikes(t *testing.T) {

	json := `{"likes":[
{"likee": 3929, "ts": 1464869768, "liker": 25486},
{"likee": 13239, "ts": 1431103000, "liker": 26727},
{"likee": 2407, "ts": 1439604510, "liker": 6403},
{"likee": 26677, "ts": 1454719940, "liker": 22248},
{"likee": 22411, "ts": 1481309376, "liker": 32820},
{"likee": 9747, "ts": 1431850118, "liker": 43794},
{"likee": 43575, "ts": 1499496173, "liker": 16134},
{"likee": 29725, "ts": 1479087147, "liker": 22248}
]}`

	likes := hlc18.LikesJson{}
	if err := likes.UnmarshalJSON([]byte(json)); err != nil {
		t.Error("UnmarshalJSON failed", err)
	}

	json_e1 := `{"likes":[{"likeS":1090354,"likee":1265187,"ts":1539147049},{"liker":170169,"likee":1218842,"ts":1466399211},{"liker":1230885,"likee":1241432,"ts":1502073097},{"liker":159840,"likee":780037,"ts":1454571186},{"liker":170169,"likee":197962,"ts":1530321211},{"liker":21501,"likee":370380,"ts":1489574354},{"liker":917010,"likee":1277359,"ts":1541567544},{"liker":1041411,"likee":924176,"ts":1528403302},{"liker":917010,"likee":1143455,"ts":1528370621},{"liker":917010,"likee":770291,"ts":1502086250},{"liker":917010,"likee":165963,"ts":1531346942},{"liker":170169,"likee":260866,"ts":1469019928},{"liker":159840,"likee":325585,"ts":1460007344},{"liker":917010,"likee":46193,"ts":1496548847},{"liker":170169,"likee":107890,"ts":1475811909},{"liker":159840,"likee":351503,"ts":1501461434},{"liker":917010,"likee":378435,"ts":1474619345},{"liker":1041411,"likee":143608,"ts":1498102770},{"liker":1090354,"likee":862719,"ts":1470049747},{"liker":917010,"likee":646907,"ts":1468621932},{"liker":21501,"likee":852484,"ts":1485352282},{"liker":917010,"likee":91715,"ts":1501137002},{"liker":21501,"likee":631966,"ts":1512659286},{"liker":21501,"likee":530578,"ts":1498079095},{"liker":151910,"likee":1630163,"ts":1535876732},{"liker":1090354,"likee":1118859,"ts":1528514719},{"liker":1090354,"likee":1148473,"ts":1488293405},{"liker":1230885,"likee":505838,"ts":1524076400},{"liker":21501,"likee":105292,"ts":1490557679},{"liker":159840,"likee":263047,"ts":1540004458},{"liker":1117672,"likee":226741,"ts":1453448609}]}`
	if err := likes.UnmarshalJSON([]byte(json_e1)); err == nil {
		t.Error("UnmarshalJSON failed", err)
	} else {
		fmt.Print(err)
	}
}

func TestFilter(t *testing.T) {

	var accounts = make([]Account, 30000)
	maxId, e := loadDataArray(accounts)

	if maxId != 30000 || e != nil {
		t.Error("load fail")
	}
	params := make(map[string]string)
	params["sname_null"] = "0"
	params["sex_eq"] = "m"
	filter, e := makeFilter(params)
	account := &accounts[29999-1]
	writeAccount(os.Stdout, false, filter, account, 29999)
	if b, e := filter.test(account); !b || e != nil {
		t.Errorf("29999 test failed: %v,%v,  for filter: %#v", b, e, filter)
	}
}
*/
