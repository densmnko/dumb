package main

import (
	h "dsmnko.org/hlc18"
	"fmt"
	"github.com/valyala/fasthttp"
	"log"
	"math"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"tidwall/evio"
	"time"
)

const CAPACITY = 1320000

// rating`s
const (
	DataDir         = "/tmp/data/"
	ListenPort      = 80
	Phase1Count     = 27000
	Phase2Count     = 90000
	Phase3Count     = 60000
	Phase1TestCount = 3000
	Phase2TestCount = 10000
	Phase3TestCount = 15000
	collectStat     = false
	postOperations  = true
)

const (
	//DataDir         = "/var/data/data2/" //data-test-2612/" //data-test-2401/"
	//ListenPort      = 8080
	//Phase1Count     = 16000 //27000
	//Phase2Count     = 90000
	//Phase3Count     = 30000 //60000
	//Phase1TestCount = 2400  //3000
	//Phase2TestCount = 10000
	//Phase3TestCount = 10000 //15000
	//collectStat     = false
	//postOperations  = true

	Phase1Unique = 16000
	Phase3Unique = 30000

	Phase1TestUnique = 2400
	Phase3TestUnique = 10000
)

var (
	getOverall  int64
	getDone     int64
	postOverall int64
	postDone    int64

	cacheRead int64

	mutate sync.Mutex

	phase = int64(1)

	queryStatMutex sync.Mutex
)

const Status200 = "200 OK"
const Status400 = "400 Bad Request"
const Status404 = "404 Not Found"
const HeaderConnectionKeepAlive = "Connection: keep-alive\r\n"

type QueryStat struct {
	key   string
	count int
	sum   int64 // nano
	max   int64 // nano
}

var queryStat = make(map[string]QueryStat, 64*1024)

func maxNano(a, b int64) int64 {
	if a > b {
		return a
	} else {
		return b
	}
}

func printQueryStatReport() {
	if len(queryStat) > 0 {
		report := make([]QueryStat, len(queryStat))[:0]
		queryStatMutex.Lock()
		for _, v := range queryStat {
			report = append(report, v)
		}
		//todo: reset --- queryStat = make(map[string]QueryStat)
		queryStatMutex.Unlock()
		sort.Slice(report, func(i, j int) bool {
			return report[i].sum > report[j].sum
			//return report[i].max > report[j].max
		})
		fmt.Println("\nrequest;params;count;sum(mls);avg;max")
		for _, v := range report[:] {
			count := v.count
			if count > 0 {
				fmt.Printf("%s;%d;%d;%d;%d\n", v.key, count, v.sum/int64(time.Millisecond), v.sum/int64(time.Microsecond)/int64(count), v.max/int64(time.Microsecond))
			} else {
				fmt.Printf("%s;%d\n", v.key, count)
			}
		}
		fmt.Println()
	}
}

func updateQueryStat(key string, start int64) {
	nano := time.Now().UnixNano() - start
	queryStatMutex.Lock()
	if stat, ok := queryStat[key]; ok {
		queryStat[key] = QueryStat{key, stat.count + 1, stat.sum + nano, maxNano(nano, stat.max)}
	} else {
		queryStat[key] = QueryStat{key, 1, nano, nano}
	}
	queryStatMutex.Unlock()
}

func filterQueryKey(args *fasthttp.Args) string {
	keys := make([]string, args.Len())[:0]
	args.VisitAll(func(keyBytes, _ []byte) {
		k := h.B2s(keyBytes)
		if k != "limit" && k != "query_id" {
			keys = append(keys, k)
		}
	})
	key := "filter;"
	sort.StringSlice(keys).Sort()
	for _, k := range keys {
		key += k + ","
	}
	return key
}

func sortedParams(params map[string]string) (key string) {
	keys := make([]string, len(params))[:0]
	for k, _ := range params {
		if k != "order" && k != "keys" {
			keys = append(keys, k)
		}
	}
	sort.StringSlice(keys).Sort()
	for _, k := range keys {
		key += k + ","
	}
	return key
}

func groupQueryKey(p *h.Group) string {
	key := fmt.Sprintf("group;%v/%d/", p.GroupBy, p.Order)
	return key + sortedGroupFilters(&p.FilterBy)
}

func sortedGroupFilters(p *map[string]bool) (key string) {
	keys := make([]string, len(*p))[:0]
	for k := range *p {
		keys = append(keys, k)
	}
	sort.StringSlice(keys).Sort()
	for _, k := range keys {
		key += k + ","
	}
	return key
}

func recommendQueryKey(params map[string]string) string {
	return "recommend;" + sortedParams(params)
}

func suggestQueryKey(params map[string]string) string {
	return "suggest;" + sortedParams(params)
}

var EMPTY_ACCOUNTS = []byte("{\"accounts\":[]}")
var EMPTY_GROUPS = []byte("{\"groups\":[]}")
var EMPTY_POST = []byte("{}")

func FilterHandler(c evio.Conn, args *fasthttp.Args, out /*, body */ []byte, query string, readCache bool) []byte {
	var cacheToken = string(h.S2b(query))

	if readCache {
		if bytes := h.GetFilterCache(cacheToken); bytes != nil {
			return c.WriteAhead(bytes)
		}
		println("GetFilterCache ", "miss")
	}
	filter, err := h.MakeFilterArgs(args)
	if err != nil || filter.Limit < 1 {
		out = h.AppendHttpResponse(out, Status400, "", nil)
		tail := c.WriteAhead(out)
		h.PutFilterCache(cacheToken, out)
		return tail
	}
	if filter.ShortCircuit || filter.Limit > 50 {
		out = h.AppendHttpResponse(out, Status200, "", EMPTY_ACCOUNTS)
		tail := c.WriteAhead(out)
		h.PutFilterCache(cacheToken, out)
		return tail
	}
	if collectStat {
		nanoStart := time.Now().UnixNano()
		defer updateQueryStat(filterQueryKey(args), nanoStart)
	}
	bodyBuffer := bodyBufferPool.Get().([]byte)
	body := bodyBuffer[:0]

	var consumer = func(separate bool, fields map[string]bool, account *h.Account, accountId uint32) {
		body = h.WriteAccountOut(body, separate, filter.Fields, account, accountId)
	}

	body = append(body, "{\"accounts\":["...)
	filter.Process(filter.Limit, consumer)
	body = append(body, "]}"...)

	out = h.AppendHttpResponse(out, Status200, "", body)
	tail := c.WriteAhead(out)
	bodyBufferPool.Put(bodyBuffer)
	h.PutFilterCache(cacheToken, out)
	return tail
}

func GroupHandler(c evio.Conn, args *fasthttp.Args, out []byte, query string, readCache bool) []byte {
	var cacheToken = string(h.S2b(query))
	if readCache {
		if bytes := h.GetGroupCache(cacheToken); bytes != nil {
			return c.WriteAhead(bytes)
		}
		println("GetGroupCache ", "miss")
	}
	var group h.Group
	if err := group.FromArgs(args); err != nil || group.Limit < 1 {
		out = h.AppendHttpResponse(out, Status400, "", nil)
		tail := c.WriteAhead(out)
		h.PutGroupCache(cacheToken, out)
		return tail
	}
	if group.Filter.ShortCircuit {
		out = h.AppendHttpResponse(out, Status200, "", EMPTY_GROUPS)
		tail := c.WriteAhead(out)
		h.PutGroupCache(cacheToken, out)
		return tail
	}
	if collectStat {
		nanoStart := time.Now().UnixNano()
		defer updateQueryStat(groupQueryKey(&group), nanoStart)
	}

	bodyBuffer := bodyBufferPool.Get().([]byte)
	body := bodyBuffer[:0]

	if items := group.Classify(&h.Store.Accounts, true); len(items) > 0 {
		body = append(body, "{\"groups\":["...)
		for i, item := range items {
			if i > 0 {
				body = append(body, ',')
			}
			body = group.WriteJsonGroupItemOut(body, item.Key, item.Count)
		}
		body = append(body, "]}"...)
	} else {
		out = h.AppendHttpResponse(out, Status200, "", EMPTY_GROUPS)
		tail := c.WriteAhead(out)
		bodyBufferPool.Put(bodyBuffer)
		h.PutGroupCache(cacheToken, out)
		return tail
	}
	out = h.AppendHttpResponse(out, Status200, "", body)
	tail := c.WriteAhead(out)
	bodyBufferPool.Put(bodyBuffer)
	h.PutGroupCache(cacheToken, out)
	return tail
}

// Особенность 8. Если в хранимых данных не существует пользователя с переданным id, то ожидается код 404 с пустым телом ответа.
func RecommendHandler(c evio.Conn, args *fasthttp.Args, id uint32, out []byte, query string, readCache bool) []byte {
	var cacheToken = string(h.S2b(query))
	if readCache {
		if bytes := h.GetRecommendCache(cacheToken); bytes != nil {
			return c.WriteAhead(bytes)
		}
		println("GetRecommendCache ", "miss")
	}
	limit, _, params := ParseParams(args)
	if limit < 1 {
		out = h.AppendHttpResponse(out, Status400, "", nil)
		tail := c.WriteAhead(out)
		h.PutRecommendCache(cacheToken, out)
		return tail
	}
	if v, ok := params["city"]; ok && v == "" {
		out = h.AppendHttpResponse(out, Status400, "", nil)
		tail := c.WriteAhead(out)
		h.PutRecommendCache(cacheToken, out)
		return tail
	}
	if v, ok := params["country"]; ok && v == "" {
		out = h.AppendHttpResponse(out, Status400, "", nil)
		tail := c.WriteAhead(out)
		h.PutRecommendCache(cacheToken, out)
		return tail
	}
	if id > h.Store.MaxId || id < 1 || h.Store.Accounts[id-1].IsEmpty() {
		out = h.AppendHttpResponse(out, Status404, "", nil)
		tail := c.WriteAhead(out)
		h.PutRecommendCache(cacheToken, out)
		return tail
	}
	if collectStat {
		nanoStart := time.Now().UnixNano()
		defer updateQueryStat(recommendQueryKey(params), nanoStart)
	}

	bodyBuffer := bodyBufferPool.Get().([]byte)
	body := bodyBuffer[:0]

	body = append(body, "{\"accounts\":["...)
	recommend := h.Recommend(id, limit, params)
	for i, accId := range recommend {
		body = h.WriteAccountOutRecommend(body, i != 0, &h.Store.Accounts[accId-1], accId)
	}
	body = append(body, "]}"...)

	out = h.AppendHttpResponse(out, Status200, "", body)
	tail := c.WriteAhead(out)
	bodyBufferPool.Put(bodyBuffer)
	h.PutRecommendCache(cacheToken, out)
	return tail
}

func SuggestHandler(c evio.Conn, args *fasthttp.Args, id uint32, out []byte, query string, readCache bool) []byte {
	var cacheToken = string(h.S2b(query))
	if readCache {
		if bytes := h.GetSuggestCache(cacheToken); bytes != nil {
			return c.WriteAhead(bytes)
		}
		println("GetSuggestCache ", "miss")
	}
	limit, _, params := ParseParams(args)
	if limit < 1 {
		out = h.AppendHttpResponse(out, Status400, "", nil)
		tail := c.WriteAhead(out)
		h.PutSuggestCache(cacheToken, out)
		return tail
	}
	if v, ok := params["city"]; ok && v == "" {
		out = h.AppendHttpResponse(out, Status400, "", nil)
		tail := c.WriteAhead(out)
		h.PutSuggestCache(cacheToken, out)
		return tail
	}
	if v, ok := params["country"]; ok && v == "" {
		out = h.AppendHttpResponse(out, Status400, "", nil)
		tail := c.WriteAhead(out)
		h.PutSuggestCache(cacheToken, out)
		return tail
	}
	if id > h.Store.MaxId || id < 1 || h.Store.Accounts[id-1].IsEmpty() {
		out = h.AppendHttpResponse(out, Status404, "", nil)
		tail := c.WriteAhead(out)
		h.PutSuggestCache(cacheToken, out)
		return tail
	}
	if collectStat {
		nanoStart := time.Now().UnixNano()
		defer updateQueryStat(suggestQueryKey(params), nanoStart)
	}
	bodyBuffer := bodyBufferPool.Get().([]byte)
	body := bodyBuffer[:0]

	body = append(body, "{\"accounts\":["...)
	suggested := h.Suggest(id, limit, params)
	for i, accId := range suggested {
		body = h.WriteAccountOutSuggest(body, i != 0, &h.Store.Accounts[accId-1], accId)
	}
	body = append(body, "]}"...)

	out = h.AppendHttpResponse(out, Status200, "", body)
	tail := c.WriteAhead(out)
	bodyBufferPool.Put(bodyBuffer)
	h.PutSuggestCache(cacheToken, out)
	return tail
}

/**
В ответе ожидается код 201 с пустым json-ом в теле ответа ({}), если создание нового пользователя прошло успешно.
В случае некорректных типов данных или неизвестных ключей нужно вернуть код 400 с пустым телом.
*/
// /accounts/new/ -> 201
func NewHandler(c evio.Conn, out, body []byte) []byte {
	var accountJson h.AccountJson
	if err := h.UnmarshalNew(&accountJson, body); /*accountJson.UnmarshalJSON(body)*/ err != nil {
		return c.WriteAhead(h.AppendHttpResponse(out, Status400, HeaderConnectionKeepAlive, nil))
	}
	if accountJson.Id > CAPACITY || !h.Store.Accounts[accountJson.Id-1].IsEmpty() {
		return c.WriteAhead(h.AppendHttpResponse(out, Status400, HeaderConnectionKeepAlive, nil))
	}
	mutate.Lock()
	if err := h.ValidateNew(&accountJson); err != nil {
		mutate.Unlock()
		return c.WriteAhead(h.AppendHttpResponse(out, Status400, HeaderConnectionKeepAlive, nil))
	}
	mutate.Unlock()
	out = c.WriteAhead(h.AppendHttpResponse(out, "201 Created", HeaderConnectionKeepAlive, EMPTY_POST))
	if h.Store.MaxId < accountJson.Id {
		h.Store.MaxId = accountJson.Id
	}
	mutate.Lock()
	if err := h.Store.CompressToAccount(&accountJson); err != nil {
		log.Fatalf("new account fatal: %v, accountJson: %v", err, accountJson)
	}
	mutate.Unlock()
	return out
}

/**
В ответе ожидается код 202 с пустым json-ом в теле ответа ({}), если обновление прошло успешно.
Если запись с указанным id не существует в имеющихся данных, то ожидается код 404 с пустым телом.
Если запись существует, но в теле запроса переданы неизвестные поля или типы значений неверны, то ожидается код 400.
*/

// /accounts/<id>/ (/accounts/46133/?query_id=308) -> 202
func UpdateHandler(c evio.Conn, out, body []byte, id uint32) []byte {
	//nanoStart := time.Now().UnixNano();defer updateQueryStat("update;full;", nanoStart)
	if id < 1 || id > atomic.LoadUint32(&h.Store.MaxId) /*|| h.Store.Accounts[id-1].sexStatus == 0*/ {
		return h.AppendHttpResponse(out, Status404, HeaderConnectionKeepAlive, nil)
	}
	var accountJson h.AccountJson
	accountJson.Birth = math.MaxInt32
	accountJson.Id = id
	if err := h.UnmarshalUpdate(&accountJson, body); /*accountJson.UnmarshalJSON(body)*/ err != nil {
		return c.WriteAhead(h.AppendHttpResponse(out, Status400, HeaderConnectionKeepAlive, nil))
	}
	// проверить уникальность телефона и email
	mutate.Lock()
	if err := h.ValidateUpdate(&accountJson, id); err != nil {
		mutate.Unlock()
		return c.WriteAhead(h.AppendHttpResponse(out, Status400, HeaderConnectionKeepAlive, nil))
	}
	mutate.Unlock()
	out = c.WriteAhead(h.AppendHttpResponse(out, "202 Accepted", HeaderConnectionKeepAlive, EMPTY_POST))
	//nanoStart2 := time.Now().UnixNano(); defer updateQueryStat("update;update;", nanoStart2)
	accountJson.Id = id
	mutate.Lock()
	if err := h.Store.CompressUpdateToAccount(&accountJson); err != nil {
		log.Fatalf("update account fatal: %v, accountJson: %v", err, accountJson)
	}
	mutate.Unlock()
	return out
}

/**
В ответе ожидается код 202 с пустым json-ом в теле ответа ({}), если обновление прошло успешно.
Если в теле запроса переданы неизвестные поля или типы значений неверны, то ожидается код 400.
*/
func LikesHandler(c evio.Conn, out, body []byte) []byte {
	//nanoStart := time.Now().UnixNano();defer updateQueryStat("like;full;", nanoStart)
	var err error
	var likes []h.LikeUpdate
	if likes, err = h.ParseLikesUpdate(body); err != nil {
		return c.WriteAhead(h.AppendHttpResponse(out, Status400, "", nil))
	}
	if err := h.ValidateLikes(likes); err != nil {
		return c.WriteAhead(h.AppendHttpResponse(out, Status400, "", nil))
	}

	out = c.WriteAhead(h.AppendHttpResponse(out, "202 Accepted", HeaderConnectionKeepAlive, EMPTY_POST))
	//nanoStart2 := time.Now().UnixNano();defer updateQueryStat("like;;", nanoStart2)
	mutate.Lock() // todo: ? отдельные локи на обновление индекса лайков и данных store/счетов
	h.Store.LikesUpdate(likes)
	mutate.Unlock()
	return out
}

func main() {

	debug.SetGCPercent(10)
	h.InitStore(CAPACITY)
	h.MakeIndexes(CAPACITY)
	var err error
	if err = h.Store.LoadData(DataDir); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%v\truntype: %d, now: %d\n", h.Timenow(), h.Store.RunType, h.Store.Now)
	runtime.GC()
	PrintReport()

	evio.SetEpollWait(-1)
	debug.SetGCPercent(-1)
	h.PrintMemUsage("GC off")

	//defer profile.Start(profile.MemProfileRate(512), profile.ProfilePath(".")).Stop()
	//defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()
	//defer profile.Start(profile.MutexProfile, profile.ProfilePath(".")).Stop()

	h.EvioServer(ListenPort, EvioHandler)
	return

}

//var prof  interface {Stop()}
func PrePost() int64 {
	counter := atomic.AddInt64(&postOverall, 1)
	if counter == 1 {
		// начало фазы 2
		//prof = profile.Start(profile.MutexProfile/*profile.MemProfileRate(512)*/, profile.ProfilePath("."))
		evio.SetEpollWait(0)
		if atomic.CompareAndSwapInt64(&phase, 1, 2) {
			_, _ = fmt.Printf("%v\t* phase-2 started, getOverall %d\n", h.Timenow(), atomic.LoadInt64(&getOverall))
		}
	}
	return counter
}

func PostPost(_ int64) {
	done := atomic.AddInt64(&postDone, 1)
	if (done == Phase2TestCount && h.Store.RunType == 0) || (done == Phase2Count && h.Store.RunType == 1) {
		evio.SetEpollWait(-1)
		//prof.Stop()
		if postOperations {
			Phase_2_Copmplete()
		}
	}

	// todo: spikes hunt
	// if done == 1000 {
	// 	printQueryStatReport()
	// }
}

func PreGet() int64 {
	counter := atomic.AddInt64(&getOverall, 1)
	if 1 == counter {
		evio.SetEpollWait(0)
		_, _ = fmt.Printf("%v\t* phase-1 started, postOverall %d\n", h.Timenow(), atomic.LoadInt64(&postOverall))
	}
	if 2 == atomic.LoadInt64(&phase) {
		//evio.SetEpollWait(0)
		if atomic.CompareAndSwapInt64(&phase, 2, 3) {
			_, _ = fmt.Printf("%v\t* phase-3 started, postOverall %d\n", h.Timenow(), atomic.LoadInt64(&postOverall))
		}
	}
	return counter
}

func PostGet(_ int64) {
	done := atomic.AddInt64(&getDone, 1)

	if h.Store.RunType == 1 {
		if done == Phase1Unique || done == (Phase1Count+Phase3Unique) {
			println("cache on")
			atomic.StoreInt64(&cacheRead, 1)
		}
	} else {
		if done == Phase1TestUnique || done == (Phase1TestCount+Phase3TestUnique) {
			println("cache on")
			atomic.StoreInt64(&cacheRead, 1)
		}
	}

	if (h.Store.RunType == 1 && done == Phase1Count) ||
		(h.Store.RunType == 0 && done == Phase1TestCount) {
		evio.SetEpollWait(-1)
		if postOperations {
			Phase_1_Complete(done)
		}
	} else if (h.Store.RunType == 1 && done == (Phase1Count+Phase3Count)) ||
		(h.Store.RunType == 0 && done == (Phase1TestCount+Phase3TestCount)) {
		evio.SetEpollWait(-1)
		atomic.StoreInt64(&cacheRead, 0)
		println("cache off")
		if postOperations {
			Phase_3_Complete(done)
		}
	}

}

func ParseParams(args *fasthttp.Args) (limit, queryId int, params map[string]string) {
	limit = -1
	params = make(map[string]string)

	args.VisitAll(func(keyBytes, valueBytes []byte) {
		key := h.B2s(keyBytes)
		switch key {
		case "limit":
			limit, _ = fasthttp.ParseUint(valueBytes)
		case "query_id":
			queryId, _ = fasthttp.ParseUint(valueBytes)
		default:
			params[key] = h.B2s(valueBytes)
		}
	})
	return limit, queryId, params
}

func PrintReport() {
	mutate.Lock()
	info := fmt.Sprintf("capacity %v accounts, maxId %v, ", len(h.Store.Accounts), h.Store.MaxId)
	info += fmt.Sprintf("fnames:%v,", h.FnameDict.Len())
	info += fmt.Sprintf("snames:%v,", h.SnameDict.Len())
	info += fmt.Sprintf("interests:%v,", h.InterestDict.Len())
	info += fmt.Sprintf("cities:%v,", h.CityDict.Len())
	info += fmt.Sprintf("countries:%v,", h.CountryDict.Len())
	info += fmt.Sprintf("domains:%v,", h.DomainDict.Len())
	info += fmt.Sprintf("emails:%v,", len(h.EmailMap))
	info += fmt.Sprintf("phones:%v", len(h.PhoneMap))
	mutate.Unlock()
	h.PrintMemUsage(info)
}

var bodyBufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 4096)
	},
}

func EvioHandler(c evio.Conn, in []byte) (out []byte, action evio.Action) {
	if len(in) == 0 { // len(in)
		//todo: wtf ?
		log.Printf("empty in\n")
		return
	}

	ctx := c.Context().(*h.Context)
	data := ctx.Is.Begin(in)
	out = ctx.Out[:0]
	var req h.Request
	for {
		leftover, err := h.Parsereq(data, &req)
		if err != nil {
			log.Printf("500 Error: %v, %v\n", err, req)
			out = h.AppendHttpResponse(out, "500 Error", "", nil)
			action = evio.Close
			break
		} else if len(leftover) == len(data) {
			// Request not ready, yet
			// log.Printf("leftover (%d) %s\n", len(leftover), req.Query)
			break
		}

		// handle the Request
		if req.Method == "GET" {
			//out = c.WriteAhead(h.AppendHttpResponse(out, Status404, "", nil))
			counter := PreGet()
			readCache := 1 == atomic.LoadInt64(&cacheRead)
			var args = fasthttp.AcquireArgs()
			args.ParseBytes(h.S2b(req.Query))
			//println(req.Path, req.Query)
			if "/accounts/filter/" == req.Path {
				out = FilterHandler(c, args, out, req.Query, readCache)
				//updateQueryStat(fmt.Sprintf("%d:%s%s", counter, req.Path, req.Query), nanoStart)
			} else if "/accounts/group/" == req.Path {
				out = GroupHandler(c, args, out, req.Query, readCache)
				//updateQueryStat(fmt.Sprintf("%d:%s%s", counter, req.Path, req.Query), nanoStart)
			} else if strings.HasSuffix(req.Path, "/recommend/") {
				if id, err := fasthttp.ParseUint(h.S2b(req.Path)[10 : len(req.Path)-11]); err == nil && id > 0 {
					out = RecommendHandler(c, args, uint32(id), out, req.Query, readCache)
				} else {
					// todo: cache this shit too?
					out = c.WriteAhead(h.AppendHttpResponse(out, Status404, "", nil))
				}
			} else if strings.HasSuffix(req.Path, "/suggest/") {
				if id, err := fasthttp.ParseUint(h.S2b(req.Path)[10 : len(req.Path)-9]); err == nil && id > 0 {
					out = SuggestHandler(c, args, uint32(id), out, req.Query, readCache)
				} else {
					// todo: cache this shit too?
					out = c.WriteAhead(h.AppendHttpResponse(out, Status404, "", nil))
				}

			} else {
				out = c.WriteAhead(h.AppendHttpResponse(out, Status404, "", nil))
			}
			fasthttp.ReleaseArgs(args)
			PostGet(counter)
		} else { // POST
			counter := PrePost()
			if strings.HasSuffix(req.Path, "/new/") {
				out = NewHandler(c, out, h.S2b(req.Body))
			} else if strings.HasSuffix(req.Path, "/likes/") {
				out = LikesHandler(c, out, h.S2b(req.Body))
			} else if strings.HasPrefix(req.Path, "/accounts/") {
				if id, err := fasthttp.ParseUint(h.S2b(req.Path)[10 : len(req.Path)-1]); err == nil && id > 0 {
					out = UpdateHandler(c, out, h.S2b(req.Body), uint32(id))
				} else {
					out = c.WriteAhead(h.AppendHttpResponse(out, Status404, HeaderConnectionKeepAlive, nil))
				}
			} else {
				out = c.WriteAhead(h.AppendHttpResponse(out, Status400, HeaderConnectionKeepAlive, nil))
			}
			PostPost(counter)
			//updateQueryStat(fmt.Sprintf("%d:%s%s", counter, req.Path, req.Query), nanoStart)
		}
		if len(leftover) == 2 && leftover[0] == '\r' && leftover[1] == '\n' {
			data = leftover[2:]
		} else {
			data = leftover
		}
		if len(data) == 0 {
			//updateQueryStat(string(h.S2b(req.Query)), nanoStart)
			break
		}
	}

	ctx.Is.End(data)
	return
}

func Phase_2_Copmplete() {

	timer := time.NewTimer(50 * time.Millisecond)
	go func() {
		<-timer.C
		h.PrintMemUsage(fmt.Sprintf("* phase-2 finished, getOverall %d, fire rebuild timer", atomic.LoadInt64(&getOverall)))
		debug.SetGCPercent(70)
		println("GC on 70")
		h.Store.Rebuild(true)
		PrintReport()
		debug.SetGCPercent(-1)
		h.PrintMemUsage("GC off")
	}()
}

func Phase_3_Complete(counter int64) {
	_, _ = fmt.Printf("%v\t* phase-3 complete, getOverall %d, postOverall %d\n", h.Timenow(), counter, postOverall)
	timer := time.NewTimer(250 * time.Millisecond)
	go func() {
		<-timer.C
		h.PrintMemUsage("GC on 10")
		debug.SetGCPercent(10)
		printQueryStatReport()
	}()
}

func Phase_1_Complete(counter int64) {
	timer := time.NewTimer(500 * time.Millisecond)
	go func() {
		<-timer.C
		atomic.StoreInt64(&cacheRead, 0)
		println("cache off")
		h.PrintMemUsage(fmt.Sprintf("* phase-1 complete, getOverall %d", counter))
		debug.SetGCPercent(70)
		fmt.Printf("%v\tGC on 70, and ResetIndexes()/h.ResetCaches()\n", h.Timenow())
		h.ResetIndexes()
		h.ResetCaches()
		runtime.GC()
		debug.SetGCPercent(-1)
		h.PrintMemUsage("GC off")
		//evio.SetEpollWait(0)
	}()
}
