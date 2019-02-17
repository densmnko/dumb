package hlc18

import (
	"errors"
	"sync"
)

// todo: по suggest & recommend можно попробовать считать (и кешировать) всегда по 20, но, есть сомнение что это окажется эффективнее чем сейчас...

type CacheKey struct {
	id          uint32
	cityCode    uint16
	countryCode byte
}

var recommendL2CacheMutex sync.RWMutex
var recommendL2Cache map[CacheKey][]uint32

var suggestL2CacheMutex sync.RWMutex
var suggestL2Cache map[CacheKey][]uint32

var groupL2Cache map[GroupCacheKey][]GroupItem
var groupL2CacheMutex sync.RWMutex

type Reply struct {
	Status int
	Body   []byte
}

var filterCache map[string][]byte
var filterCacheMutex sync.Mutex

var groupCache map[string][]byte
var groupCacheMutex sync.Mutex

var suggestCache map[string][]byte
var suggestCacheMutex sync.Mutex

var recommendCache map[string][]byte
var recommendCacheMutex sync.Mutex

var errorNotFound = errors.New("not found")

func ResetCaches() {
	recommendL2CacheMutex.Lock()
	recommendL2Cache = make(map[CacheKey][]uint32, 22000)
	recommendL2CacheMutex.Unlock()

	suggestL2CacheMutex.Lock()
	suggestL2Cache = make(map[CacheKey][]uint32, 13000)
	suggestL2CacheMutex.Unlock()

	filterCacheMutex.Lock()
	filterCache = make(map[string][]byte, 32000)
	filterCacheMutex.Unlock()

	groupCacheMutex.Lock()
	groupCache = make(map[string][]byte, 12500)
	groupCacheMutex.Unlock()

	groupL2CacheMutex.Lock()
	groupL2Cache = make(map[GroupCacheKey][]GroupItem, 25000)
	groupL2CacheMutex.Unlock()

	recommendCacheMutex.Lock()
	recommendCache = make(map[string][]byte, 11000)
	recommendCacheMutex.Unlock()

	suggestCacheMutex.Lock()
	suggestCache = make(map[string][]byte, 6500)
	suggestCacheMutex.Unlock()
}

func getRecommendL2Cache(token CacheKey, limit int) ([]uint32, error) {
	recommendL2CacheMutex.RLock()
	if res, found := recommendL2Cache[token]; found && res != nil {
		recommendL2CacheMutex.RUnlock()
		if len(res) >= limit {
			return res[:limit], nil
		}
		return nil, errorNotFound
	}
	recommendL2CacheMutex.RUnlock()
	return nil, errorNotFound
}

func putRecommendL2Cache(token CacheKey, value []uint32) {
	if value != nil {
		recommendL2CacheMutex.Lock()
		old := recommendL2Cache[token]
		if len(old) < len(value) {
			valueCopy := append([]uint32{}, value...)
			recommendL2Cache[token] = valueCopy
		}
		recommendL2CacheMutex.Unlock()
	}
}

func getSuggestL2Cache(token CacheKey, limit int) ([]uint32, error) {
	suggestL2CacheMutex.RLock()
	if res, found := suggestL2Cache[token]; found && res != nil {
		suggestL2CacheMutex.RUnlock()
		if len(res) >= limit {
			return res[:limit], nil
		}
		return nil, errorNotFound
	}
	suggestL2CacheMutex.RUnlock()
	return nil, errorNotFound
}

func putSuggestL2Cache(token CacheKey, value []uint32) {
	if value != nil {
		suggestL2CacheMutex.Lock()
		old := suggestL2Cache[token]
		if len(old) < len(value) {
			valueCopy := append([]uint32{}, value...)
			suggestL2Cache[token] = valueCopy
		}
		suggestL2CacheMutex.Unlock()
	}
}

func GetFilterCache(token string) []byte {
	return filterCache[token]
}

func PutFilterCache(token string, value []byte) {
	reply := append([]byte{}, value...)
	filterCacheMutex.Lock()
	filterCache[token] = reply
	filterCacheMutex.Unlock()
}

func GetGroupCache(token string) []byte {
	return groupCache[token]
}

func PutGroupCache(token string, value []byte) {
	reply := append([]byte{}, value...)
	groupCacheMutex.Lock()
	groupCache[token] = reply
	groupCacheMutex.Unlock()
}

func GetSuggestCache(token string) []byte {
	return suggestCache[token]
}

func PutSuggestCache(token string, value []byte) {
	reply := append([]byte{}, value...)
	suggestCacheMutex.Lock()
	suggestCache[token] = reply
	suggestCacheMutex.Unlock()
}

func GetRecommendCache(token string) []byte {
	return recommendCache[token]
}

func PutRecommendCache(token string, value []byte) {
	reply := append([]byte{}, value...)
	recommendCacheMutex.Lock()
	recommendCache[token] = reply
	recommendCacheMutex.Unlock()
}
