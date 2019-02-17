package hlc18

import (
	"fmt"
	"net/url"
	"reflect"
	"testing"
)

func TestSuggest(t *testing.T) {
	accounts := loadTestData(t)
	if l := len(accounts); l != 30000 {
		t.Fatal("load failed")
	}

	var rec []uint32
	var str string

	str, _ = url.PathUnescape("/accounts/20126/suggest/?query_id=518&limit=6")
	fmt.Println(str)

	///accounts/20126/suggest/?query_id=518&limit=6
	//REQUEST  URI: /accounts/20126/suggest/?query_id=518&limit=6
	//REQUEST BODY:
	//BODY   GOT: {"accounts":[]}
	//BODY   EXP: {"accounts":[{"sname":"\u0421\u0442\u0430\u043c\u044b\u043a\u0430\u043a\u0438\u0439","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u0414\u0430\u043d\u0438\u043b\u0430","email":"ilhetahidvesitylit@inbox.com","id":20049},
	// {"sname":"\u0414\u0430\u043d\u043e\u043b\u043e\u0432\u0438\u0447","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u041e\u043b\u0435\u0433","email":"utnele@yandex.ru","id":19487},
	// {"sname":"\u0422\u0435\u0440\u0430\u0448\u0435\u0432\u0438\u0447","status":"\u0432\u0441\u0451 \u0441\u043b\u043e\u0436\u043d\u043e","fname":"\u0415\u0433\u043e\u0440","email":"odtuqwatudetve@yahoo.com","id":16585},
	// {"sname":"\u0425\u043e\u043f\u0430\u0442\u043e\u0441\u044f\u043d","status":"\u0437\u0430\u043d\u044f\u0442\u044b","fname":"\u0412\u044f\u0447\u0435\u0441\u043b\u0430\u0432","email":"takletgobnattepadreti@ya.ru","id":15317},
	// {"sname":"\u0414\u0430\u043d\u044b\u043a\u0430\u0442\u0438\u043d","status":"\u0432\u0441\u0451 \u0441\u043b\u043e\u0436\u043d\u043e","fname":"\u0421\u0435\u043c\u0451\u043d","email":"fehase@ya.ru","id":13417},
	// {"sname":"\u0424\u0435\u0442\u0435\u0442\u0430\u0447\u0430\u043d","status":"\u0437\u0430\u043d\u044f\u0442\u044b","fname":"\u0415\u0433\u043e\u0440","email":"vadebroawasam@list.ru","id":8169}]}

	rec = Suggest(20126, 6, map[string]string{})
	if len(rec) != 6 {
		t.Error("20126 failed, len(rec) != 6")
	}
	if len(rec) > 0 && rec[0] != 20049 {
		t.Error("20126 failed, rec[0] != 20049")
	}
	fmt.Printf("%v\n", rec)

	// REQUEST  URI: /accounts/5784/suggest/?country=%D0%98%D1%81%D0%BF%D0%B5%D0%B7%D0%B8%D1%8F&query_id=1850&limit=10
	// REQUEST BODY:
	// BODY   GOT: {"accounts":[{"id":29999,"email":"fyshaffatenodladha@yandex.ru","sname":"Стамашелан","status":"свободны","fname":"Алексей"},{"id":29513,"email":"tagredlavemo@mail.ru","status":"свободны","fname":"Роман","sname":"Колленпов"},{"id":29477,"email":"datalsenunpi@yandex.ru","status":"свободны","fname":"Даниил","sname":"Хопатотин"},{"id":29043,"email":"ehnesavtar@mail.ru","status":"свободны","fname":"Евгений"},{"id":28937,"email":"datososercaives@inbox.com","status":"свободны","fname":"Владимир","sname":"Фаашекий"},{"id":28937,"email":"datososercaives@inbox.com","status":"свободны","fname":"Владимир","sname":"Фаашекий"},{"id":28877,"email":"amsidehuhnarwinac@mail.ru","fname":"Степан","sname":"Пенушусян","status":"свободны"},{"id":28653,"email":"egnanysmasomrotow@me.com","status":"свободны","fname":"Никита","sname":"Терушусян"},{"id":28511,"email":"petetietguwnafan@inbox.ru","status":"свободны","fname":"Алексей","sname":"Колыкако"},{"id":28505,"email":"mysdyllotfacugit@me.com","sname":"Данашело","status":"всё сложно","fname":"Леонид"}]}
	// BODY   EXP: {"accounts":[{"sname":"\u0421\u0442\u0430\u043c\u0430\u0448\u0435\u043b\u0430\u043d","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u0410\u043b\u0435\u043a\u0441\u0435\u0439","email":"fyshaffatenodladha@yandex.ru",
	// "id":29999},{"sname":"\u041a\u043e\u043b\u043b\u0435\u043d\u043f\u043e\u0432","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u0420\u043e\u043c\u0430\u043d","email":"tagredlavemo@mail.ru"
	// "id":29513},{"sname":"\u0425\u043e\u043f\u0430\u0442\u043e\u0442\u0438\u043d","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u0414\u0430\u043d\u0438\u0438\u043b","email":"datalsenunpi@yandex.ru",
	// "id":29477},
	// "id":29043,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u0415\u0432\u0433\u0435\u043d\u0438\u0439","email":"ehnesavtar@mail.ru"},{"sname":"\u0424\u0430\u0430\u0448\u0435\u043a\u0438\u0439","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u0412\u043b\u0430\u0434\u0438\u043c\u0438\u0440","email":"datososercaives@inbox.com",
	// "id":28937},{"sname":"\u041f\u0435\u043d\u0443\u0448\u0443\u0441\u044f\u043d","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u0421\u0442\u0435\u043f\u0430\u043d","email":"amsidehuhnarwinac@mail.ru",
	// "id":28877},{"sname":"\u0422\u0435\u0440\u0443\u0448\u0443\u0441\u044f\u043d","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u041d\u0438\u043a\u0438\u0442\u0430","email":"egnanysmasomrotow@me.com",
	// "id":28653},{"sname":"\u041a\u043e\u043b\u044b\u043a\u0430\u043a\u043e","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u0410\u043b\u0435\u043a\u0441\u0435\u0439","email":"petetietguwnafan@inbox.ru",
	// "id":28511},{"sname":"\u0414\u0430\u043d\u0430\u0448\u0435\u043b\u043e","status":"\u0432\u0441\u0451 \u0441\u043b\u043e\u0436\u043d\u043e","fname":"\u041b\u0435\u043e\u043d\u0438\u0434","email":"mysdyllotfacugit@me.com",
	// "id":28505},{"sname":"\u041f\u0435\u043d\u0443\u0448\u0443\u0447\u0430\u043d","status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","fname":"\u0421\u0435\u0440\u0433\u0435\u0439","email":"romotgalenic@email.com",
	// "id":27775}]}

	str, _ = url.PathUnescape("/accounts/5784/suggest/?country=%D0%98%D1%81%D0%BF%D0%B5%D0%B7%D0%B8%D1%8F&query_id=1850&limit=10")
	fmt.Println(str)

	rec = Suggest(5784, 10, map[string]string{"country": "Испезия"})
	if len(rec) != 10 {
		t.Error("5784 failed, len(rec) != 10")
	}
	if len(rec) > 0 && rec[0] != 29999 {
		t.Error("5784 failed, rec[0] != 29999")
	}

	awaited := []uint32{
		29999,
		29513,
		29477,
		29043,
		28937,
		28877,
		28653,
		28511,
		28505,
		27775,
	}
	for i, id := range rec {
		if id != awaited[i] {
			t.Errorf("5784 failed, rec[%d]%d != %d\n", i, id, awaited[i])
		}
	}
	fmt.Printf("%v\n", rec)

	// cache
	rec2 := Suggest(5784, 10, map[string]string{"country": "Испезия"})
	if !reflect.DeepEqual(rec2, rec) {
		t.Error("cache failed, ", str)
	}

}
