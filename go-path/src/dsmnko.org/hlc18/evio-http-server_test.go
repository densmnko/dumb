package hlc18

import (
	// "github.com/valyala/fasthttp"
	"testing"
)

func TestParseQuery(t *testing.T) {
	//queryArgs.ParseBytes(u.queryString)
	// order=-1&query_id=120&keys=status&birth=1983&limit=35&likes=26242
	// joined=2016&order=-1&query_id=0&keys=interests&limit=35&likes=21514
	// city=%D0%9F%D0%B5%D1%80%D0%B5%D0%B3%D0%B0%D0%BC%D0%B0&query_id=2040&limit=10
	// joined=2016&order=-1&query_id=1800&limit=30&keys=interests
	// sname_null=0&query_id=2160&limit=18&sex_eq=m
	// interests_any=%D0%A1%D0%B8%D0%BC%D0%BF%D1%81%D0%BE%D0%BD%D1%8B%2C%D0%90%D0%BF%D0%B5%D0%BB%D1%8C%D1%81%D0%B8%D0%BD%D0%BE%D0%B2%D1%8B%D0%B9+%D1%81%D0%BE%D0%BA%2C%D0%A0%D1%8D%D0%BF%2C%D0%A1%D1%82%D0%B5%D0%B9%D0%BA&query_id=2280&limit=26&likes_contains=26566
	// query_id=960&limit=14&likes_contains=401
	// city=%D0%9B%D0%B5%D0%B9%D0%BF%D0%BE%D1%80%D0%B8%D0%B6&query_id=840&limit=4
	// query_id=360&birth_lt=691597982&city_any=%D0%9C%D0%BE%D1%81%D0%BE%D0%BB%D0%B5%D1%81%D1%81%D0%BA%2C%D0%A0%D0%BE%D1%82%D1%82%D0%B5%D1%80%D0%BE%D1%88%D1%82%D0%B0%D0%B4%D1%82%2C%D0%9D%D0%BE%D0%B2%D0%BE%D0%BA%D0%B5%D0%BD%D1%81%D0%BA%2C%D0%9B%D0%B8%D1%81%D1%81%D0%B0%D0%BA%D0%BE%D0%B1%D1%81%D0%BA%2C%D0%97%D0%B5%D0%BB%D0%B5%D0%BD%D0%BE%D0%BB%D0%B0%D0%BC%D1%81%D0%BA&limit=22&sex_eq=f
	// city=%D0%91%D0%B0%D1%80%D1%81%D0%BE%D0%B3%D0%B0%D0%BC%D0%B0&query_id=480&limit=18
	// order=1&query_id=1200&keys=city%2Cstatus&interests=%D0%9E%D0%B1%D1%89%D0%B5%D0%BD%D0%B8%D0%B5&birth=1990&limit=20
	// interests=%D0%98%D0%BD%D1%82%D0%B5%D1%80%D0%BD%D0%B5%D1%82&order=1&query_id=1320&limit=50&keys=city%2Csex
	// email_gt=wa&country_null=1&query_id=720&limit=32&sex_eq=f
	// joined=2013&order=1&query_id=1680&keys=country&interests=%D0%9F%D0%BB%D0%B0%D0%B2%D0%B0%D0%BD%D0%B8%D0%B5&limit=30
	// country=%D0%A0%D0%BE%D1%81%D0%B0%D1%82%D1%80%D0%B8%D1%81&query_id=240&limit=16
	// country=%D0%9C%D0%B0%D0%BB%D0%BC%D0%B0%D0%BB%D1%8C&query_id=1440&limit=2
	// query_id=600&order=-1&sex=f&limit=10&keys=country%2Cstatus
	// query_id=1560&interests_contains=%D0%9C%D0%B0%D1%81%D1%81%D0%B0%D0%B6%2C%D0%A2%D1%8F%D0%B6%D1%91%D0%BB%D0%B0%D1%8F+%D0%B0%D1%82%D0%BB%D0%B5%D1%82%D0%B8%D0%BA%D0%B0%2C%D0%9A%D0%B8%D0%BD%D0%BE%D0%BA%D0%B8%D1%84%D0%B8%D0%BB%D1%8C%D0%BC%D1%8B%2C%D0%A7%D1%83%D0%B4%D0%B0%D0%BA%2C%D0%9F%D0%BE%D0%BF+%D1%80%D0%BE%D0%BA&status_neq=%D0%B7%D0%B0%D0%BD%D1%8F%D1%82%D1%8B&limit=2
	// country_null=0&query_id=1920&interests_any=%D0%9C%D0%BE%D1%80%D0%BE%D0%B6%D0%B5%D0%BD%D0%BE%D0%B5%2C%D0%A1%D0%BE%D0%BD&sex_eq=f&limit=18&status_eq=%D1%81%D0%B2%D0%BE%D0%B1%D0%BE%D0%B4%D0%BD%D1%8B

}
