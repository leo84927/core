package config

import (
	"fmt"
	"strconv"
	"time"
)

// 把值與它在 Redis 上的真實鍵名綁在一起。鍵名只用於錯誤訊息，讓人看到錯誤就知道去改哪一筆。
type entry struct {
	value    string // Redis 值
	redisKey string // Redis 鍵名
}

// 累積「缺鍵」與「型別轉換失敗」兩類錯誤，讓 parse 一次報齊而不是遇到第一個就返回
type reader struct {
	raw  map[string]entry // key 為正規化後的鍵名，與 proto enum 的值名同形
	errs []error
}

/*
 * lookup 的判準：鍵不存在報錯，鍵存在但值為空放行。
 *
 * raw 的 key 全部來自 Scan 之後的正規化，所以 ok 精確等於「Redis 有沒有這筆」。
 * 於是 Redis 鍵的存在本身就是宣告：
 * 明確放一筆空值 =「我知道有這個設定，我選擇不啟用」
 * 整筆不存在 =「你漏了」
 */
func (r *reader) lookup(key fmt.Stringer) (string, bool) {
	name := key.String()

	e, ok := r.raw[name]
	if !ok {
		r.errs = append(r.errs, fmt.Errorf("setting key %s not found in redis", name))
	}

	return e.value, ok
}

func (r *reader) str(key fmt.Stringer) string {
	v, _ := r.lookup(key)
	return v
}

func (r *reader) uint(key fmt.Stringer) uint {
	v, ok := r.lookup(key)
	if !ok {
		return 0
	}

	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		r.errs = append(r.errs, fmt.Errorf("setting key %s is not a non-negative integer: %q", r.name(key), v))
		return 0
	}

	return uint(n)
}

func (r *reader) duration(key fmt.Stringer) time.Duration {
	v, ok := r.lookup(key)
	if !ok {
		return 0
	}

	d, err := time.ParseDuration(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("setting key %s is not a duration: %q", r.name(key), v))
		return 0
	}

	return d
}

func (r *reader) location(key fmt.Stringer) *time.Location {
	v, ok := r.lookup(key)
	if !ok {
		return nil
	}

	loc, err := time.LoadLocation(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("setting key %s is not a valid timezone: %q", r.name(key), v))
		return nil
	}

	return loc
}

func (r *reader) service(keys []fmt.Stringer) map[string]string {
	m := make(map[string]string, len(keys))
	for _, key := range keys {
		m[key.String()] = r.str(key)
	}

	return m
}

/*
 * 錯誤訊息裡該印的鍵名。
 *
 * 鍵存在就印 Redis 上的真實鍵名（含冒號），照著它就能在 Upstash 上找到那一筆。
 * 鍵不存在時只能印正規化名 —— 冒號落在哪裡無從得知，硬猜一個位置就會把人送去找一個不存在的鍵，
 */
func (r *reader) name(key fmt.Stringer) string {
	name := key.String()

	if e, ok := r.raw[name]; ok {
		return e.redisKey
	}

	return name
}
