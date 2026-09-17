package config

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"time"

	_ "time/tzdata"

	env "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/env"
	"github.com/leo84927/core/v2/logger"
	"github.com/leo84927/core/v2/mariadb"
	"github.com/leo84927/core/v2/redis"
	goredis "github.com/redis/go-redis/v9"
)

// GLOBAL 是全服務共用的命名空間
const globalNamespace = "GLOBAL"

// QueueKeys 存的是 Redis 上的鍵名，到 Load 階段才會透過這個鍵名取得實際的值
type QueueKeys struct {
	NameKey    fmt.Stringer
	RoutingKey fmt.Stringer
}

/*
 * Spec 是能力宣告，不是鍵清單：各服務真正的差異不是「要哪些鍵」，而是要不要 MariaDB、是不是 consumer。
 * 逐鍵列舉的版本會讓 Load 退化成包了 Redis 的 map getter，排序與組裝原封不動留在呼叫端。
 *
 * Prefix 保持顯式而不從 ServiceNameKey 推導：TrimSuffix("_SERVICE_NAME") 是靠命名慣例的巧技，
 * 改個鍵名就靜默壞掉，正是這批改動要消滅的東西。
 */
type Spec struct {
	Prefix         string       // 對應 Redis 的 <PREFIX>:* 命名空間
	ServiceNameKey fmt.Stringer // 服務名放在哪個鍵
	MariaDB        bool         // 是否需要 mariadb.Config（只有 bookkeeping 為 true）
	Queue          *QueueKeys   // nil = 只當 producer；non-nil = consumer，需完整 topology
	ServiceKeys    []fmt.Stringer
}

/*
 * Settings 是純值：不寫任何全域、不初始化 logger 與 tracer。
 *
 * 副作用與其生命週期管理留在 initialize.New / app.Close 同一層，詳見 docs/adr/0003。
 */
type Settings struct {
	ServiceName string
	Loc         *time.Location
	Logger      logger.Config // 直接沿用既有型別，initialize.New 因此少一次搬運
	RabbitMQ    RabbitMQ
	MariaDB     *mariadb.Config // Spec.MariaDB 為 false 時為 nil

	// GLOBAL 鍵，gRPC client 與 gRPC server 共讀
	GrpcSockPath string

	/*
	 * 服務專屬鍵
	 * core 只保證 Spec.ServiceKeys 列出的鍵存在，命名與型別由服務端自組具名結構
	 */
	Service map[string]string
}

func Load(ctx context.Context, spec Spec) (Settings, error) {
	cm := redis.NewConnectionManager(redis.Config{
		Host:           os.Getenv("REDIS_HOST"),
		Port:           os.Getenv("REDIS_PORT"),
		Password:       os.Getenv("REDIS_PASSWORD"),
		DB:             0,
		DialTimeout:    5 * time.Second,
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   5 * time.Second,
		PoolSize:       10,
		MinIdleConns:   2,
		MaxRetries:     3,
		MaxElapsedTime: 30 * time.Second,
	})
	defer cm.Close() // 設定只在啟動時讀一次，執行期不再回查；連線用完就關，不留給呼叫端

	client, err := cm.Client(ctx)
	if err != nil {
		return Settings{}, err
	}

	raw, err := readAll(ctx, client, spec.Prefix)
	if err != nil {
		return Settings{}, err
	}

	return parse(raw, spec)
}

// 讀 GLOBAL:* 與 <PREFIX>:* 兩個命名空間，回傳的 map 以 Redis 端真實鍵名為 key
func readAll(ctx context.Context, client *goredis.Client, prefix string) (map[string]string, error) {
	raw := make(map[string]string)

	// 先讀 GLOBAL:*，再讀 <PREFIX>:*
	for _, namespace := range []string{globalNamespace, prefix} {
		vals, err := list(ctx, client, namespace+":*")
		if err != nil {
			return nil, err
		}
		maps.Copy(raw, vals)
	}

	return raw, nil
}

// 根據 pattern 取得所有鍵值，回傳以 Redis 鍵名為 key 的 map
func list(ctx context.Context, client *goredis.Client, pattern string) (map[string]string, error) {
	var keys []string

	// 先用 Scan 取得符合 pattern 的 keys，再透過 Iterator 遍歷，把 keys 存到 keys slice 中
	iter := client.Scan(ctx, 0, pattern, 100).Iterator()
	if err := iter.Err(); err != nil {
		return nil, err
	}
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}

	/*
	 * 迴圈後必須再檢查一次：cursor 續掃失敗時 iter.Next 只是回 false，
	 * 漏掉這裡就會回傳一個截斷的 map 且 err 為 nil，接著被 parse 報成「鍵不存在」——
	 * 把一個 Redis 連線問題說成是設定漏了一筆，operator 會被送去 Upstash 加一筆重複的鍵。
	 */
	if err := iter.Err(); err != nil {
		return nil, err
	}

	if len(keys) == 0 {
		return nil, nil
	}

	// MGet 一次取回所有鍵的值，回傳的 vals slice 與 keys slice 對應
	vals, err := client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	// 將 keys & vals 轉成 map[string]string
	result := make(map[string]string, len(keys))
	for i, val := range vals {
		if s, ok := val.(string); ok {
			result[keys[i]] = s
		}
	}

	return result, nil
}

/*
 * index 把 Redis 鍵名正規化成 proto enum 的值名：冒號一律換成底線。
 *
 * 方向是這裡唯一重要的事。反過來從 enum 值名重建 Redis 鍵名必須知道冒號落在哪裡，正規化這個方向不需要猜。
 */
func index(raw map[string]string) (map[string]entry, error) {
	indexed := make(map[string]entry, len(raw))

	var errs []error
	for _, redisKey := range slices.Sorted(maps.Keys(raw)) {
		name := strings.ReplaceAll(redisKey, ":", "_")

		// 鍵重複了，紀錄錯誤
		if existing, ok := indexed[name]; ok {
			errs = append(errs, fmt.Errorf("setting keys %s and %s both normalize to %s", existing.redisKey, redisKey, name))
			continue
		}

		indexed[name] = entry{value: raw[redisKey], redisKey: redisKey}
	}

	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	return indexed, nil
}

/*
 * 用正規化後的鍵名去 raw map 取值，組裝成 Settings。
 * 設定來源只有 Redis 一個，注入 Source interface 只是假想的 seam；測試性由這個內部 seam 提供。
 */
func parse(raw map[string]string, spec Spec) (Settings, error) {
	indexed, err := index(raw)
	if err != nil {
		return Settings{}, err
	}

	r := &reader{raw: indexed}

	// ServiceName 是真實來源，Logger 與 RabbitMQ 各持一份自己的設定複本，由 Load 一次填好
	serviceName := r.str(spec.ServiceNameKey)

	settings := Settings{
		ServiceName:  serviceName,
		Loc:          r.location(env.GlobalEnvKey_GLOBAL_TIMEZONE),
		GrpcSockPath: r.str(env.GlobalEnvKey_GLOBAL_BOOKKEEPING_SOCK_FILE_PATH),
		Logger: logger.Config{
			ServiceName: serviceName,
			Endpoint:    r.str(env.GlobalEnvKey_GLOBAL_GRAFANA_ENDPOINT),
			AuthHeader:  r.str(env.GlobalEnvKey_GLOBAL_GRAFANA_TOKEN),
		},
		RabbitMQ: r.rabbitMQ(serviceName, spec.Queue),
		Service:  r.service(spec.ServiceKeys),
	}

	if spec.MariaDB {
		settings.MariaDB = r.mariaDB()
	}

	// errors.Join 一次報齊全部問題，不自訂 error 型別 —— Load 失敗就是啟動失敗，沒有呼叫端會做 errors.As
	if err := errors.Join(r.errs...); err != nil {
		return Settings{}, err
	}

	return settings, nil
}
