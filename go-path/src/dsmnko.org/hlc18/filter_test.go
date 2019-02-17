package hlc18

import (
	"fmt"
	"log"
	"net/url"
	"testing"
)

const TMP_DATA_DIR = "/tmp/data-test-2612/"

func BenchmarkFilterInretestsContains(b *testing.B) {

	const capacity = 30000
	MakeIndexes(capacity)
	InitStore(capacity)
	_ = Store.LoadData(TMP_DATA_DIR)
	if l := len(Store.Accounts); l != 30000 {
		log.Fatal("load failed")
	}

	b.Run("interests_contains[2], country_null=1,     sex_eq=f", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = filterSample(nil, 28, "interests_contains", "Выходные,Рубашки", "country_null", "1", "sex_eq", "f")
		}
	})

	b.Run("interests_contains[1], status_eq=свободны, sex_eq=m", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = filterSample(nil, 20, "interests_contains", "Рубашки", "status_eq", "свободны", "sex_eq", "m")
		}
	})

}

func TestFilterInretestsContains(t *testing.T) {
	const capacity = 30000
	MakeIndexes(capacity)
	InitStore(capacity)

	_ = Store.LoadData(TMP_DATA_DIR)
	if l := len(Store.Accounts); l != 30000 {
		t.Fatal("load failed")
	}
	var result []uint32

	//REQUEST  URI: /accounts/filter/?interests_contains=%D0%9F%D0%BE%D1%86%D0%B5%D0%BB%D1%83%D0%B8%2C%D0%A2%D0%B5%D0%BB%D0%B5%D0%B2%D0%B8%D0%B4%D0%B5%D0%BD%D0%B8%D0%B5&premium_null=0&query_id=1635&limit=10
	//REQUEST BODY:
	//BODY   GOT: {"accounts":[{"id":10387,"email":"hedenfeh@yandex.ru","premium":{"finish":1549850513,"start":1541988113}}]}
	//BODY   EXP: {"accounts":[{"premium":{"finish":1557667257,"start":1526131257},"email":"ahtininebuson@ymail.com","id":28962},{"premium":{"finish":1532007885,"start":1516283085},"email":"losedluihet@yandex.ru","id":28389},{"premium":{"finish":1545107928,"start":1537245528},"email":"syehollo@me.com","id":28010},{"premium":{"finish":1552946982,"start":1537222182},"email":"avitalnedi@email.com","id":27629},{"premium":{"finish":1532313560,"start":1529721560},"email":"toeltolnobcain@email.com","id":26147},{"premium":{"finish":1572869912,"start":1541333912},"email":"poncihednehpeogty@yandex.ru","id":20811},{"premium":{"finish":1572409241,"start":1540873241},"email":"nenatititreleer@email.com","id":20209},{"premium":{"finish":1526155357,"start":1518292957},"email":"heohevecdotket@yahoo.com","id":17168},{"premium":{"finish":1555770461,"start":1524234461},"email":"tirehsuqnerohunro@ya.ru","id":14573},{"premium":{"finish":1549850513,"start":1541988113},"email":"hedenfeh@yandex.ru","id":10387}]}
	print_path("/accounts/filter/?interests_contains=%D0%9F%D0%BE%D1%86%D0%B5%D0%BB%D1%83%D0%B8%2C%D0%A2%D0%B5%D0%BB%D0%B5%D0%B2%D0%B8%D0%B4%D0%B5%D0%BD%D0%B8%D0%B5&premium_null=0&query_id=1635&limit=10")
	///accounts/filter/?interests_contains=Поцелуи,Телевидение&premium_null=0&query_id=1635&limit=10
	result, _ = filterSample(t, 10, "interests_contains", "Поцелуи,Телевидение", "premium_null", "0")
	if len(result) != 10 || result[0] != 28962 || result[9] != 10387 {
		t.Errorf("invalid reply: %v\n", result)
	}

	//REQUEST  URI: /accounts/filter/?interests_contains=%D0%92%D1%8B%D1%85%D0%BE%D0%B4%D0%BD%D1%8B%D0%B5%2C%D0%A0%D1%83%D0%B1%D0%B0%D1%88%D0%BA%D0%B8&country_null=1&query_id=1646&limit=28&sex_eq=f
	//REQUEST BODY:
	//BODY   GOT: {"accounts":[]}
	//BODY   EXP: {"accounts":[{"id":27942,"sex":"f","email":"ytahtomen@inbox.ru"}]}
	print_path("/accounts/filter/?interests_contains=%D0%92%D1%8B%D1%85%D0%BE%D0%B4%D0%BD%D1%8B%D0%B5%2C%D0%A0%D1%83%D0%B1%D0%B0%D1%88%D0%BA%D0%B8&country_null=1&query_id=1646&limit=28&sex_eq=f")
	///accounts/filter/?interests_contains=Выходные,Рубашки&country_null=1&query_id=1646&limit=28&sex_eq=f
	result, _ = filterSample(t, 28, "interests_contains", "Выходные,Рубашки", "country_null", "1", "sex_eq", "f")
	if len(result) != 1 || result[0] != 27942 {
		t.Errorf("invalid reply: %v\n", result)
	}

	// REQUEST  URI: /accounts/filter/?interests_contains=%D0%A0%D1%83%D0%B1%D0%B0%D1%88%D0%BA%D0%B8&status_eq=%D1%81%D0%B2%D0%BE%D0%B1%D0%BE%D0%B4%D0%BD%D1%8B&query_id=1028&limit=20&sex_eq=m
	// REQUEST BODY:
	// BODY   GOT: {"accounts":[]}
	// BODY   EXP: {"accounts":[
	// {"id":29755,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","sex":"m","email":"edehemtisdawi@rambler.ru"},
	// {"id":29547,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","sex":"m","email":"ettehoeslabo@mail.ru"},
	// {"id":29199,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","sex":"m","email":"dytetafinep@me.com"},
	// {"id":29147,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","sex":"m","email":"tenaedonontigtes@icloud.com"},
	// {"id":29089,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","sex":"m","email":"mynensedtoohru@icloud.com"},
	// {"id":28961,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","sex":"m","email":"hirodun@ya.ru"},
	// {"id":28915,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","sex":"m","email":"terohehesiratten@email.com"},
	// {"id":28887,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","sex":"m","email":"gamocvoirhaloag@inbox.com"},
	// {"id":28541,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","sex":"m","email":"posiet@gmail.com"},
	// {"id":28405,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","sex":"m","email":"ofatnerlinutas@ymail.com"},
	// {"id":28379,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","sex":"m","email":"ehterernetna@list.ru"},
	// {"id":28375,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","sex":"m","email":"homistudu@ya.ru"},
	// {"id":28351,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","sex":"m","email":"letnytuclinsincetobas@list.ru"},
	// {"id":28095,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","sex":"m","email":"omacpetgawsawyh@me.com"},
	// {"id":28081,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","sex":"m","email":"netdigosoasweah@email.com"},
	// {"id":28069,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","sex":"m","email":"hewawgoeg@me.com"},
	// {"id":27839,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","sex":"m","email":"hedparersistalve@list.ru"},
	// {"id":27559,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","sex":"m","email":"ehetiftaitotih@email.com"},
	// {"id":27541,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","sex":"m","email":"soletit@mail.ru"},
	// {"id":27517,"status":"\u0441\u0432\u043e\u0431\u043e\u0434\u043d\u044b","sex":"m","email":"tinesseetireh@ya.ru"}]}
	print_path("/accounts/filter/?interests_contains=%D0%A0%D1%83%D0%B1%D0%B0%D1%88%D0%BA%D0%B8&status_eq=%D1%81%D0%B2%D0%BE%D0%B1%D0%BE%D0%B4%D0%BD%D1%8B&query_id=1028&limit=20&sex_eq=m")
	///accounts/filter/?interests_contains=Рубашки&status_eq=свободны&query_id=1028&limit=20&sex_eq=m
	result, _ = filterSample(t, 20, "interests_contains", "Рубашки", "status_eq", "свободны", "sex_eq", "m")
	if len(result) != 20 || result[0] != 29755 || result[19] != 27517 {
		t.Errorf("invalid reply: %v\n", result)
	}

}

func filterSample(t *testing.T, limit int, conditions ...string) ([]uint32, *Filter) {
	var params = make(map[string]string)
	for i, cond := range conditions {
		if i%2 == 0 {
			params[cond] = conditions[i+1]
		}
	}
	var filter *Filter
	var err error
	if filter, err = MakeFilter(params); err != nil {
		if t != nil {
			t.Errorf("MakeFilter failed, %v, %v\n", params, err)
		} else {
			log.Fatalf("MakeFilter failed, %v, %v\n", params, err)
		}
		return nil, nil
	}
	result := make([]uint32, 50)[:0]
	filter.Process(limit, func(_ bool, _ map[string]bool, _ *Account, id uint32) {
		result = append(result, id)
	})
	return result, filter
}

func print_path(path string) {
	var str string
	str, _ = url.PathUnescape(path)
	fmt.Println(str)
}

/*

/accounts/filter/?interests_contains=Выходные,Рубашки&country_null=1&query_id=1646&limit=28&sex_eq=f
/accounts/filter/?interests_contains=Горы,Пляжный+отдых,Апельсиновый+сок,Фотография&limit=38&sex_eq=m&status_eq=свободны&query_id=13582&city_null=1
/accounts/filter/?interests_contains=Спортивные+машины,Салаты,Курица,AC/DC,Симпсоны&limit=36&sex_eq=f&status_eq=свободны&query_id=14789&city_null=1
/accounts/filter/?interests_contains=Пиво,Туфли,Симпсоны&limit=40&sex_eq=f&status_eq=свободны&query_id=15631&city_null=1
/accounts/filter/?interests_contains=Регги,Юмор,Фрукты&limit=16&sex_eq=m&status_neq=свободны&query_id=13031&city_null=1
/accounts/filter/?interests_contains=Телевидение,Автомобили,Pitbull,Ужин+с+друзьями,Обнимашки&limit=28&sex_eq=f&status_eq=свободны&query_id=2603&city_null=1
/accounts/filter/?interests_contains=Стейк,Боевые+искусства,Сон&limit=40&sex_eq=m&status_eq=всё+сложно&query_id=12288&city_null=1
/accounts/filter/?interests_contains=Pitbull,Бокс,Клубника&limit=28&city_null=1&status_neq=свободны&query_id=3236
/accounts/filter/?interests_contains=Еда+и+Напитки,Салаты,Бокс,Мороженое,Рыба&limit=38&sex_eq=m&status_neq=всё+сложно&query_id=5878&city_null=1
/accounts/filter/?interests_contains=Вкусно+поесть,Мясо,Фитнес&limit=40&city_null=1&sex_eq=m&query_id=1596
/accounts/filter/?interests_contains=Любовь,Красное+вино,Фильмы,Клубника&limit=32&sex_eq=m&status_neq=всё+сложно&query_id=15312&city_null=1
/accounts/filter/?interests_contains=Хип+Хоп,South+Park,Боевые+искусства&limit=26&sex_eq=m&status_neq=свободны&query_id=29441&city_null=1
/accounts/filter/?interests_contains=50+Cent,Красное+вино,Компьютеры,Матрица&limit=14&sex_eq=m&status_neq=свободны&query_id=5956&city_null=1
/accounts/filter/?interests_contains=Выходные,Матрица,Девушки&limit=6&sex_eq=f&status_eq=свободны&query_id=26264&city_null=1
/accounts/filter/?interests_contains=Вечер+с+друзьями,Целоваться,Поцелуи,Знакомство,Путешествия&limit=2&query_id=5343&city_null=1&status_eq=заняты
/accounts/filter/?interests_contains=Танцевальная,Автомобили,Плавание&limit=26&sex_eq=m&status_neq=заняты&query_id=21349&city_null=1
/accounts/filter/?interests_contains=Стейк,Любовь,Овощи,Люди+Икс&limit=34&sex_eq=m&status_eq=заняты&query_id=15066&city_null=1
/accounts/filter/?interests_contains=Боевые+искусства,Знакомство,Итальянская+кухня&limit=28&city_null=1&sex_eq=f&query_id=18110
/accounts/filter/?interests_contains=Регги,Металлика,South+Park,Фитнес&limit=16&city_null=1&status_neq=заняты&query_id=10122


11.7446ms:/accounts/filter/?interests_contains=%D0%93%D0%BE%D1%80%D1%8B%2C%D0%9F%D0%BB%D1%8F%D0%B6%D0%BD%D1%8B%D0%B9+%D0%BE%D1%82%D0%B4%D1%8B%D1%85%2C%D0%90%D0%BF%D0%B5%D0%BB%D1%8C%D1%81%D0%B8%D0%BD%D0%BE%D0%B2%D1%8B%D0%B9+%D1%81%D0%BE%D0%BA%2C%D0%A4%D0%BE%D1%82%D0%BE%D0%B3%D1%80%D0%B0%D1%84%D0%B8%D1%8F&limit=38&sex_eq=m&status_eq=%D1%81%D0%B2%D0%BE%D0%B1%D0%BE%D0%B4%D0%BD%D1%8B&query_id=13582&city_null=1
11.7392ms:/accounts/filter/?interests_contains=%D0%A1%D0%BF%D0%BE%D1%80%D1%82%D0%B8%D0%B2%D0%BD%D1%8B%D0%B5+%D0%BC%D0%B0%D1%88%D0%B8%D0%BD%D1%8B%2C%D0%A1%D0%B0%D0%BB%D0%B0%D1%82%D1%8B%2C%D0%9A%D1%83%D1%80%D0%B8%D1%86%D0%B0%2CAC%2FDC%2C%D0%A1%D0%B8%D0%BC%D0%BF%D1%81%D0%BE%D0%BD%D1%8B&limit=36&sex_eq=f&status_eq=%D1%81%D0%B2%D0%BE%D0%B1%D0%BE%D0%B4%D0%BD%D1%8B&query_id=14789&city_null=1
11.7118ms:/accounts/filter/?interests_contains=%D0%9F%D0%B8%D0%B2%D0%BE%2C%D0%A2%D1%83%D1%84%D0%BB%D0%B8%2C%D0%A1%D0%B8%D0%BC%D0%BF%D1%81%D0%BE%D0%BD%D1%8B&limit=40&sex_eq=f&status_eq=%D1%81%D0%B2%D0%BE%D0%B1%D0%BE%D0%B4%D0%BD%D1%8B&query_id=15631&city_null=1
11.6907ms:/accounts/filter/?interests_contains=%D0%A0%D0%B5%D0%B3%D0%B3%D0%B8%2C%D0%AE%D0%BC%D0%BE%D1%80%2C%D0%A4%D1%80%D1%83%D0%BA%D1%82%D1%8B&limit=16&sex_eq=m&status_neq=%D1%81%D0%B2%D0%BE%D0%B1%D0%BE%D0%B4%D0%BD%D1%8B&query_id=13031&city_null=1
10.7657ms:/accounts/filter/?interests_contains=%D0%A2%D0%B5%D0%BB%D0%B5%D0%B2%D0%B8%D0%B4%D0%B5%D0%BD%D0%B8%D0%B5%2C%D0%90%D0%B2%D1%82%D0%BE%D0%BC%D0%BE%D0%B1%D0%B8%D0%BB%D0%B8%2CPitbull%2C%D0%A3%D0%B6%D0%B8%D0%BD+%D1%81+%D0%B4%D1%80%D1%83%D0%B7%D1%8C%D1%8F%D0%BC%D0%B8%2C%D0%9E%D0%B1%D0%BD%D0%B8%D0%BC%D0%B0%D1%88%D0%BA%D0%B8&limit=28&sex_eq=f&status_eq=%D1%81%D0%B2%D0%BE%D0%B1%D0%BE%D0%B4%D0%BD%D1%8B&query_id=2603&city_null=1
10.7428ms:/accounts/filter/?interests_contains=%D0%A1%D1%82%D0%B5%D0%B9%D0%BA%2C%D0%91%D0%BE%D0%B5%D0%B2%D1%8B%D0%B5+%D0%B8%D1%81%D0%BA%D1%83%D1%81%D1%81%D1%82%D0%B2%D0%B0%2C%D0%A1%D0%BE%D0%BD&limit=40&sex_eq=m&status_eq=%D0%B2%D1%81%D1%91+%D1%81%D0%BB%D0%BE%D0%B6%D0%BD%D0%BE&query_id=12288&city_null=1
10.7357ms:/accounts/filter/?interests_contains=Pitbull%2C%D0%91%D0%BE%D0%BA%D1%81%2C%D0%9A%D0%BB%D1%83%D0%B1%D0%BD%D0%B8%D0%BA%D0%B0&limit=28&city_null=1&status_neq=%D1%81%D0%B2%D0%BE%D0%B1%D0%BE%D0%B4%D0%BD%D1%8B&query_id=3236
10.7346ms:/accounts/filter/?interests_contains=%D0%95%D0%B4%D0%B0+%D0%B8+%D0%9D%D0%B0%D0%BF%D0%B8%D1%82%D0%BA%D0%B8%2C%D0%A1%D0%B0%D0%BB%D0%B0%D1%82%D1%8B%2C%D0%91%D0%BE%D0%BA%D1%81%2C%D0%9C%D0%BE%D1%80%D0%BE%D0%B6%D0%B5%D0%BD%D0%BE%D0%B5%2C%D0%A0%D1%8B%D0%B1%D0%B0&limit=38&sex_eq=m&status_neq=%D0%B2%D1%81%D1%91+%D1%81%D0%BB%D0%BE%D0%B6%D0%BD%D0%BE&query_id=5878&city_null=1
9.7898ms:/accounts/filter/?interests_contains=%D0%92%D0%BA%D1%83%D1%81%D0%BD%D0%BE+%D0%BF%D0%BE%D0%B5%D1%81%D1%82%D1%8C%2C%D0%9C%D1%8F%D1%81%D0%BE%2C%D0%A4%D0%B8%D1%82%D0%BD%D0%B5%D1%81&limit=40&city_null=1&sex_eq=m&query_id=1596
9.7874ms:/accounts/filter/?interests_contains=%D0%9B%D1%8E%D0%B1%D0%BE%D0%B2%D1%8C%2C%D0%9A%D1%80%D0%B0%D1%81%D0%BD%D0%BE%D0%B5+%D0%B2%D0%B8%D0%BD%D0%BE%2C%D0%A4%D0%B8%D0%BB%D1%8C%D0%BC%D1%8B%2C%D0%9A%D0%BB%D1%83%D0%B1%D0%BD%D0%B8%D0%BA%D0%B0&limit=32&sex_eq=m&status_neq=%D0%B2%D1%81%D1%91+%D1%81%D0%BB%D0%BE%D0%B6%D0%BD%D0%BE&query_id=15312&city_null=1

59.5562ms:/accounts/filter/?email_lt=ab&limit=4&query_id=28609
38.0642ms:/accounts/filter/?email_lt=ab&limit=38&sex_eq=f&query_id=5971
11.7405ms:/accounts/filter/?interests_contains=%D0%A5%D0%B8%D0%BF+%D0%A5%D0%BE%D0%BF%2CSouth+Park%2C%D0%91%D0%BE%D0%B5%D0%B2%D1%8B%D0%B5+%D0%B8%D1%81%D0%BA%D1%83%D1%81%D1%81%D1%82%D0%B2%D0%B0&limit=26&sex_eq=m&status_neq=%D1%81%D0%B2%D0%BE%D0%B1%D0%BE%D0%B4%D0%BD%D1%8B&query_id=29441&city_null=1
11.7118ms:/accounts/filter/?interests_contains=50+Cent%2C%D0%9A%D1%80%D0%B0%D1%81%D0%BD%D0%BE%D0%B5+%D0%B2%D0%B8%D0%BD%D0%BE%2C%D0%9A%D0%BE%D0%BC%D0%BF%D1%8C%D1%8E%D1%82%D0%B5%D1%80%D1%8B%2C%D0%9C%D0%B0%D1%82%D1%80%D0%B8%D1%86%D0%B0&limit=14&sex_eq=m&status_neq=%D1%81%D0%B2%D0%BE%D0%B1%D0%BE%D0%B4%D0%BD%D1%8B&query_id=5956&city_null=1
11.6801ms:/accounts/filter/?interests_contains=%D0%92%D1%8B%D1%85%D0%BE%D0%B4%D0%BD%D1%8B%D0%B5%2C%D0%9C%D0%B0%D1%82%D1%80%D0%B8%D1%86%D0%B0%2C%D0%94%D0%B5%D0%B2%D1%83%D1%88%D0%BA%D0%B8&limit=6&sex_eq=f&status_eq=%D1%81%D0%B2%D0%BE%D0%B1%D0%BE%D0%B4%D0%BD%D1%8B&query_id=26264&city_null=1
10.7646ms:/accounts/filter/?interests_contains=%D0%92%D0%B5%D1%87%D0%B5%D1%80+%D1%81+%D0%B4%D1%80%D1%83%D0%B7%D1%8C%D1%8F%D0%BC%D0%B8%2C%D0%A6%D0%B5%D0%BB%D0%BE%D0%B2%D0%B0%D1%82%D1%8C%D1%81%D1%8F%2C%D0%9F%D0%BE%D1%86%D0%B5%D0%BB%D1%83%D0%B8%2C%D0%97%D0%BD%D0%B0%D0%BA%D0%BE%D0%BC%D1%81%D1%82%D0%B2%D0%BE%2C%D0%9F%D1%83%D1%82%D0%B5%D1%88%D0%B5%D1%81%D1%82%D0%B2%D0%B8%D1%8F&limit=2&query_id=5343&city_null=1&status_eq=%D0%B7%D0%B0%D0%BD%D1%8F%D1%82%D1%8B
10.7643ms:/accounts/filter/?interests_contains=%D0%A2%D0%B0%D0%BD%D1%86%D0%B5%D0%B2%D0%B0%D0%BB%D1%8C%D0%BD%D0%B0%D1%8F%2C%D0%90%D0%B2%D1%82%D0%BE%D0%BC%D0%BE%D0%B1%D0%B8%D0%BB%D0%B8%2C%D0%9F%D0%BB%D0%B0%D0%B2%D0%B0%D0%BD%D0%B8%D0%B5&limit=26&sex_eq=m&status_neq=%D0%B7%D0%B0%D0%BD%D1%8F%D1%82%D1%8B&query_id=21349&city_null=1
10.7633ms:/accounts/filter/?interests_contains=%D0%A1%D1%82%D0%B5%D0%B9%D0%BA%2C%D0%9B%D1%8E%D0%B1%D0%BE%D0%B2%D1%8C%2C%D0%9E%D0%B2%D0%BE%D1%89%D0%B8%2C%D0%9B%D1%8E%D0%B4%D0%B8+%D0%98%D0%BA%D1%81&limit=34&sex_eq=m&status_eq=%D0%B7%D0%B0%D0%BD%D1%8F%D1%82%D1%8B&query_id=15066&city_null=1
10.7632ms:/accounts/filter/?interests_contains=%D0%91%D0%BE%D0%B5%D0%B2%D1%8B%D0%B5+%D0%B8%D1%81%D0%BA%D1%83%D1%81%D1%81%D1%82%D0%B2%D0%B0%2C%D0%97%D0%BD%D0%B0%D0%BA%D0%BE%D0%BC%D1%81%D1%82%D0%B2%D0%BE%2C%D0%98%D1%82%D0%B0%D0%BB%D1%8C%D1%8F%D0%BD%D1%81%D0%BA%D0%B0%D1%8F+%D0%BA%D1%83%D1%85%D0%BD%D1%8F&limit=28&city_null=1&sex_eq=f&query_id=18110
10.7538ms:/accounts/filter/?interests_contains=%D0%A0%D0%B5%D0%B3%D0%B3%D0%B8%2C%D0%9C%D0%B5%D1%82%D0%B0%D0%BB%D0%BB%D0%B8%D0%BA%D0%B0%2CSouth+Park%2C%D0%A4%D0%B8%D1%82%D0%BD%D0%B5%D1%81&limit=16&city_null=1&status_neq=%D0%B7%D0%B0%D0%BD%D1%8F%D1%82%D1%8B&query_id=10122
*/
