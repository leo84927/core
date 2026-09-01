package config

import (
	"fmt"
	"strings"
	"testing"
	"time"

	env "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/env"
)

// ─────────────────────────────────────────────
// Helper
// ─────────────────────────────────────────────

/*
 * 測試打的是未匯出的純函式 parse：驗證、型別轉換、組裝全在它裡面。
 * Load 的 Redis 往返不測 —— 驗證靠 Upstash pre-flight，在這裡塞一個假的 Redis 只會測到假的東西。
 *
 * raw 以 Redis 端真實鍵名為 key，跟 readAll 回傳的形狀一致。
 */
func globalRaw() map[string]string {
	return map[string]string{
		"GLOBAL:TIMEZONE":                           "Asia/Taipei",
		"GLOBAL:BOOKKEEPING_SOCK_FILE_PATH":         "/tmp/bookkeeping.sock",
		"GLOBAL:GRAFANA_ENDPOINT":                   "https://otlp.grafana.net",
		"GLOBAL:GRAFANA_TOKEN":                      "Basic token",
		"GLOBAL:RABBITMQ_USER":                      "guest",
		"GLOBAL:RABBITMQ_PASSWORD":                  "guest",
		"GLOBAL:RABBITMQ_HOST":                      "localhost",
		"GLOBAL:RABBITMQ_PORT":                      "5672",
		"GLOBAL:RABBITMQ_VHOST":                     "job",
		"GLOBAL:RABBITMQ_CONN_MAX_RETRIES":          "5",
		"GLOBAL:RABBITMQ_CONN_MAX_ELAPSED_TIME":     "20s",
		"GLOBAL:RABBITMQ_JOB_EXCHANGE":              "job.exchange",
		"GLOBAL:RABBITMQ_EXCHANGE_KIND":             "topic",
		"GLOBAL:RABBITMQ_TOPOLOGY_MAX_RETRIES":      "3",
		"GLOBAL:RABBITMQ_TOPOLOGY_MAX_ELAPSED_TIME": "20s",
	}
}

func mariadbRaw() map[string]string {
	return map[string]string{
		"GLOBAL:MARIADB_USER":                  "root",
		"GLOBAL:MARIADB_PASSWORD":              "secret",
		"GLOBAL:MARIADB_PORT":                  "3306",
		"GLOBAL:MARIADB_DATABASE_NAME":         "bookkeeping",
		"GLOBAL:MARIADB_CHARSET":               "utf8mb4",
		"GLOBAL:MARIADB_COLLATION":             "utf8mb4_unicode_ci",
		"GLOBAL:MARIADB_TIMEOUT":               "5s",
		"GLOBAL:MARIADB_READ_TIMEOUT":          "5s",
		"GLOBAL:MARIADB_WRITE_TIMEOUT":         "5s",
		"GLOBAL:MARIADB_PRIMARY_HOST":          "primary.db",
		"GLOBAL:MARIADB_REPLICA_HOST":          "replica.db",
		"GLOBAL:MARIADB_CONN_MAX_RETRIES":      "3",
		"GLOBAL:MARIADB_CONN_MAX_ELAPSED_TIME": "20s",
		// 空值刻意保留：本地開發不開 TLS
		"GLOBAL:MARIADB_TLS_CA_PEM": "",
	}
}

// telegram：consumer + 服務專屬鍵，涵蓋 Spec 的所有欄位（MariaDB 除外）
func telegramRaw() map[string]string {
	raw := globalRaw()
	raw["TELEGRAM:SERVICE_NAME"] = "telegram"
	raw["TELEGRAM:RABBITMQ_QUEUE"] = "telegram.queue"
	raw["TELEGRAM:RABBITMQ_KEY"] = "telegram.#"
	raw["TELEGRAM:TOKEN"] = "bot-token"
	raw["TELEGRAM:CHAT_ID"] = "12345"

	return raw
}

func telegramSpec() Spec {
	return Spec{
		Prefix:         "TELEGRAM",
		ServiceNameKey: env.TelegramEnvKey_TELEGRAM_SERVICE_NAME,
		Queue: &QueueKeys{
			NameKey:    env.TelegramEnvKey_TELEGRAM_RABBITMQ_QUEUE,
			RoutingKey: env.TelegramEnvKey_TELEGRAM_RABBITMQ_KEY,
		},
		ServiceKeys: []fmt.Stringer{
			env.TelegramEnvKey_TELEGRAM_TOKEN,
			env.TelegramEnvKey_TELEGRAM_CHAT_ID,
		},
	}
}

func assertErrorMentions(t *testing.T, err error, want ...string) {
	t.Helper()

	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	for _, w := range want {
		if !strings.Contains(err.Error(), w) {
			t.Errorf("expected error to mention %q, got:\n%v", w, err)
		}
	}
}

// ─────────────────────────────────────────────
// Tests: 組裝
// ─────────────────────────────────────────────

func TestParse_Consumer(t *testing.T) {
	settings, err := parse(telegramRaw(), telegramSpec())
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if settings.ServiceName != "telegram" {
		t.Errorf("expected ServiceName telegram, got %q", settings.ServiceName)
	}

	// ServiceName 出現三次不是漂移：頂層是真實來源，另兩份是各 adapter 自己的設定複本
	if settings.Logger.ServiceName != "telegram" || settings.RabbitMQ.Config.ServiceName != "telegram" {
		t.Errorf("expected ServiceName to be copied into Logger and RabbitMQ.Config, got %q / %q",
			settings.Logger.ServiceName, settings.RabbitMQ.Config.ServiceName)
	}

	if settings.Loc == nil || settings.Loc.String() != "Asia/Taipei" {
		t.Errorf("expected Loc Asia/Taipei, got %v", settings.Loc)
	}

	if settings.GrpcSockPath != "/tmp/bookkeeping.sock" {
		t.Errorf("expected GrpcSockPath /tmp/bookkeeping.sock, got %q", settings.GrpcSockPath)
	}

	if settings.RabbitMQ.Config.MaxRetries != 5 || settings.RabbitMQ.Config.MaxElapsedTime != 20*time.Second {
		t.Errorf("expected conn budget {5, 20s}, got {%d, %v}",
			settings.RabbitMQ.Config.MaxRetries, settings.RabbitMQ.Config.MaxElapsedTime)
	}

	// consumer 需要完整 topology：exchange kind 與 topology 重試預算都要填
	topology := settings.RabbitMQ.Topology
	if topology.Exchange.Name != "job.exchange" || topology.Exchange.Kind != "topic" {
		t.Errorf("expected exchange job.exchange/topic, got %q/%q", topology.Exchange.Name, topology.Exchange.Kind)
	}
	if topology.MaxRetries != 3 || topology.MaxElapsedTime != 20*time.Second {
		t.Errorf("expected topology budget {3, 20s}, got {%d, %v}", topology.MaxRetries, topology.MaxElapsedTime)
	}

	if settings.RabbitMQ.Queue == nil {
		t.Fatalf("expected Queue to be non-nil for a consumer")
	}
	if settings.RabbitMQ.Queue.Name != "telegram.queue" {
		t.Errorf("expected queue telegram.queue, got %q", settings.RabbitMQ.Queue.Name)
	}
	if len(topology.Queues) != 1 || topology.Queues[0].Name != "telegram.queue" {
		t.Errorf("expected topology to carry the service queue, got %v", topology.Queues)
	}
	if len(settings.RabbitMQ.Queue.Keys) != 1 || settings.RabbitMQ.Queue.Keys[0] != "telegram.#" {
		t.Errorf("expected routing key telegram.#, got %v", settings.RabbitMQ.Queue.Keys)
	}

	// 服務專屬鍵以 enum 的值名為 key 回傳，服務端自組具名結構
	if got := settings.Service[env.TelegramEnvKey_TELEGRAM_TOKEN.String()]; got != "bot-token" {
		t.Errorf("expected service key TELEGRAM_TOKEN bot-token, got %q", got)
	}
	if len(settings.Service) != 2 {
		t.Errorf("expected only the declared ServiceKeys, got %v", settings.Service)
	}

	// 沒宣告 MariaDB 就不該有
	if settings.MariaDB != nil {
		t.Errorf("expected MariaDB to be nil, got %v", settings.MariaDB)
	}
}

// producer 只需要基本 topology，不該要求 queue binding 那三個鍵
func TestParse_ProducerOnly(t *testing.T) {
	raw := globalRaw()
	raw["CENTER:SERVICE_NAME"] = "center"
	delete(raw, "GLOBAL:RABBITMQ_EXCHANGE_KIND")
	delete(raw, "GLOBAL:RABBITMQ_TOPOLOGY_MAX_RETRIES")
	delete(raw, "GLOBAL:RABBITMQ_TOPOLOGY_MAX_ELAPSED_TIME")

	settings, err := parse(raw, Spec{
		Prefix:         "CENTER",
		ServiceNameKey: env.CenterEnvKey_CENTER_SERVICE_NAME,
	})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if settings.RabbitMQ.Queue != nil {
		t.Errorf("expected Queue to be nil for a producer, got %v", settings.RabbitMQ.Queue)
	}
	if settings.RabbitMQ.Topology.Exchange.Name != "job.exchange" {
		t.Errorf("expected exchange name to be loaded, got %q", settings.RabbitMQ.Topology.Exchange.Name)
	}
}

func TestParse_MariaDB(t *testing.T) {
	raw := globalRaw()
	raw["BOOKKEEPING:SERVICE_NAME"] = "bookkeeping"
	raw["BOOKKEEPING:RABBITMQ_QUEUE"] = "bookkeeping.queue"
	raw["BOOKKEEPING:RABBITMQ_KEY"] = "bookkeeping.#"
	for k, v := range mariadbRaw() {
		raw[k] = v
	}

	settings, err := parse(raw, Spec{
		Prefix:         "BOOKKEEPING",
		ServiceNameKey: env.BookkeepingEnvKey_BOOKKEEPING_SERVICE_NAME,
		MariaDB:        true,
		Queue: &QueueKeys{
			NameKey:    env.BookkeepingEnvKey_BOOKKEEPING_RABBITMQ_QUEUE,
			RoutingKey: env.BookkeepingEnvKey_BOOKKEEPING_RABBITMQ_KEY,
		},
	})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if settings.MariaDB == nil {
		t.Fatalf("expected MariaDB to be non-nil")
	}
	if settings.MariaDB.WriteDB.Host != "primary.db" || settings.MariaDB.ReadDB.Host != "replica.db" {
		t.Errorf("expected primary/replica hosts to differ, got %q / %q",
			settings.MariaDB.WriteDB.Host, settings.MariaDB.ReadDB.Host)
	}
	if settings.MariaDB.WriteDB.User != settings.MariaDB.ReadDB.User {
		t.Errorf("expected read and write DSN to share credentials")
	}
	if settings.MariaDB.MaxRetries != 3 || settings.MariaDB.MaxElapsedTime != 20*time.Second {
		t.Errorf("expected mariadb budget {3, 20s}, got {%d, %v}",
			settings.MariaDB.MaxRetries, settings.MariaDB.MaxElapsedTime)
	}
}

// ─────────────────────────────────────────────
// Tests: 鍵不存在報錯，一次報齊，帶 Redis 端真實鍵名
// ─────────────────────────────────────────────

/*
 * 缺鍵只能用正規化名回報：那一筆從未出現在 Scan 的結果裡，冒號落在哪裡無從得知。
 * 前一版會猜一個位置印出來，而 GLOBAL:MARIADB:CONN_MAX_ELAPSED_TIME 證明了猜錯的代價 ——
 * 把人送去 Upstash 找一個不存在的鍵。命名空間仍然看得出來，那是定位需要的最小資訊。
 */
func TestParse_MissingKeys_ReportedTogether(t *testing.T) {
	raw := telegramRaw()
	delete(raw, "GLOBAL:RABBITMQ_HOST")
	delete(raw, "TELEGRAM:TOKEN")
	delete(raw, "TELEGRAM:RABBITMQ_QUEUE")

	_, err := parse(raw, telegramSpec())

	// 一次報齊三個，而不是遇到第一個就返回
	assertErrorMentions(t, err, "GLOBAL_RABBITMQ_HOST", "TELEGRAM_TOKEN", "TELEGRAM_RABBITMQ_QUEUE")
}

// 鍵存在（只是值錯）時，真實鍵名拿得到 —— 含冒號的原樣，照著就能在 Upstash 上找到
func TestParse_ConversionFailure_ReportsRealRedisKeyName(t *testing.T) {
	raw := telegramRaw()
	for k, v := range mariadbRaw() {
		raw[k] = v
	}

	delete(raw, "GLOBAL:MARIADB_CONN_MAX_ELAPSED_TIME")
	raw["GLOBAL:MARIADB:CONN_MAX_ELAPSED_TIME"] = "20"

	spec := telegramSpec()
	spec.MariaDB = true

	_, err := parse(raw, spec)

	assertErrorMentions(t, err, "GLOBAL:MARIADB:CONN_MAX_ELAPSED_TIME")
	if strings.Contains(err.Error(), "GLOBAL:MARIADB_CONN_MAX_ELAPSED_TIME") {
		t.Errorf("expected the key name as stored in redis, got:\n%v", err)
	}
}

// 缺鍵時回的是零值 Settings，不是「填了一半」的設定
func TestParse_MissingKeys_ReturnsZeroSettings(t *testing.T) {
	settings, err := parse(map[string]string{}, telegramSpec())
	if err == nil {
		t.Fatalf("expected error for an empty raw map")
	}

	if settings.ServiceName != "" || settings.RabbitMQ.Queue != nil || settings.Service != nil {
		t.Errorf("expected zero Settings alongside the error, got %+v", settings)
	}
}

/*
 * 多段命名空間的 prefix 曾經是最脆弱的形狀：反推鍵名的版本只換第一個底線，
 * EXCHANGE_RATE_API_KEY 會變成 EXCHANGE:RATE_API_KEY。正規化的方向不需要知道 prefix 有幾段。
 */
func TestParse_MultiWordPrefix(t *testing.T) {
	raw := globalRaw()
	raw["EXCHANGE_RATE:SERVICE_NAME"] = "exchange_rate"
	raw["EXCHANGE_RATE:RABBITMQ_QUEUE"] = "exchange_rate.queue"
	raw["EXCHANGE_RATE:RABBITMQ_KEY"] = "exchange_rate.#"
	raw["EXCHANGE_RATE:API_KEY"] = "api-key"

	settings, err := parse(raw, Spec{
		Prefix:         "EXCHANGE_RATE",
		ServiceNameKey: env.ExchangeRateEnvKey_EXCHANGE_RATE_SERVICE_NAME,
		Queue: &QueueKeys{
			NameKey:    env.ExchangeRateEnvKey_EXCHANGE_RATE_RABBITMQ_QUEUE,
			RoutingKey: env.ExchangeRateEnvKey_EXCHANGE_RATE_RABBITMQ_KEY,
		},
		ServiceKeys: []fmt.Stringer{env.ExchangeRateEnvKey_EXCHANGE_RATE_API_KEY},
	})
	if err != nil {
		t.Fatalf("expected a multi-word prefix to resolve, got: %v", err)
	}

	if got := settings.Service[env.ExchangeRateEnvKey_EXCHANGE_RATE_API_KEY.String()]; got != "api-key" {
		t.Errorf("EXCHANGE_RATE:API_KEY = %q, want %q", got, "api-key")
	}
	if settings.ServiceName != "exchange_rate" {
		t.Errorf("ServiceName = %q, want %q", settings.ServiceName, "exchange_rate")
	}
}

// ─────────────────────────────────────────────
// Tests: 鍵存在但值為空 → 放行
// ─────────────────────────────────────────────

/*
 * Redis 鍵的存在本身就是宣告：明確放一筆空值 =「我知道有這個設定，我選擇不啟用」。
 * MariaDB 的 TLS 憑證與 telegram 的 webhook secret 兩處空值是刻意且有語意的。
 */
func TestParse_EmptyValueIsAllowed(t *testing.T) {
	raw := telegramRaw()
	raw["TELEGRAM:TOKEN"] = ""
	for k, v := range mariadbRaw() {
		raw[k] = v
	}

	spec := telegramSpec()
	spec.MariaDB = true

	settings, err := parse(raw, spec)
	if err != nil {
		t.Fatalf("expected empty values to pass, got: %v", err)
	}

	if settings.MariaDB.WriteDB.TLSCaPEM != "" {
		t.Errorf("expected TLSCaPEM to stay empty, got %q", settings.MariaDB.WriteDB.TLSCaPEM)
	}
	if got := settings.Service[env.TelegramEnvKey_TELEGRAM_TOKEN.String()]; got != "" {
		t.Errorf("expected empty service key to be kept, got %q", got)
	}
}

// ─────────────────────────────────────────────
// Tests: 六處型別轉換失敗 → 報錯且不 fallback
// ─────────────────────────────────────────────

/*
 * 改動前這六處轉換失敗會 log.Printf 加塞寫死的預設值（5／20s／3／20s／3／20s）。
 * 留這個後門會讓 Load 的契約變成「回 nil error 不代表設定是對的」。
 */
func TestParse_ConversionFailure_NoFallback(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		bad      string
		fallback string // 改動前會被塞進來的值
	}{
		{"rabbitmq conn max retries", "GLOBAL:RABBITMQ_CONN_MAX_RETRIES", "five", "5"},
		{"rabbitmq conn max elapsed time", "GLOBAL:RABBITMQ_CONN_MAX_ELAPSED_TIME", "20", "20s"},
		{"rabbitmq topology max retries", "GLOBAL:RABBITMQ_TOPOLOGY_MAX_RETRIES", "", "3"},
		{"rabbitmq topology max elapsed time", "GLOBAL:RABBITMQ_TOPOLOGY_MAX_ELAPSED_TIME", "forever", "20s"},
		{"mariadb conn max retries", "GLOBAL:MARIADB_CONN_MAX_RETRIES", "-1", "3"},
		{"mariadb conn max elapsed time", "GLOBAL:MARIADB_CONN_MAX_ELAPSED_TIME", "20 seconds", "20s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := telegramRaw()
			for k, v := range mariadbRaw() {
				raw[k] = v
			}
			raw[tt.key] = tt.bad

			spec := telegramSpec()
			spec.MariaDB = true

			settings, err := parse(raw, spec)

			assertErrorMentions(t, err, tt.key)
			if settings.MariaDB != nil || settings.RabbitMQ.Config.MaxRetries != 0 {
				t.Errorf("expected zero Settings instead of a fallback of %q, got %+v", tt.fallback, settings)
			}
		})
	}
}

// 缺鍵不該同時報成轉換失敗 —— 一個鍵只該有一則錯誤
func TestParse_MissingNumericKey_ReportedOnce(t *testing.T) {
	raw := telegramRaw()
	delete(raw, "GLOBAL:RABBITMQ_CONN_MAX_RETRIES")

	_, err := parse(raw, telegramSpec())

	assertErrorMentions(t, err, "GLOBAL_RABBITMQ_CONN_MAX_RETRIES not found")
	if strings.Count(err.Error(), "GLOBAL_RABBITMQ_CONN_MAX_RETRIES") != 1 {
		t.Errorf("expected a single error for the missing key, got:\n%v", err)
	}
}

// 時區同樣是轉換，失敗一併報錯（改動前是 log.Fatalf）
func TestParse_InvalidTimezone(t *testing.T) {
	raw := telegramRaw()
	raw["GLOBAL:TIMEZONE"] = "Mars/Olympus"

	_, err := parse(raw, telegramSpec())

	assertErrorMentions(t, err, "GLOBAL:TIMEZONE")
}

/*
 * Upstash 上的真實形狀：GLOBAL:MARIADB:CONN_MAX_ELAPSED_TIME 有兩個冒號。
 * 從 enum 值名反推 Redis 鍵名會去找 GLOBAL:MARIADB_CONN_MAX_ELAPSED_TIME —— 一個不存在的鍵，
 * 於是把「鍵在 Redis 上」報成「鍵不存在」。bookkeeping 首次部署就是這樣起不來的。
 */
func TestParse_MultiColonRedisKeyIsFound(t *testing.T) {
	raw := telegramRaw()
	for k, v := range mariadbRaw() {
		raw[k] = v
	}

	// 命名空間之內的冒號位置不該影響查值
	delete(raw, "GLOBAL:MARIADB_CONN_MAX_ELAPSED_TIME")
	raw["GLOBAL:MARIADB:CONN_MAX_ELAPSED_TIME"] = "20s"

	spec := telegramSpec()
	spec.MariaDB = true

	settings, err := parse(raw, spec)
	if err != nil {
		t.Fatalf("expected the multi-colon key to be found, got: %v", err)
	}

	if got := settings.MariaDB.MaxElapsedTime; got != 20*time.Second {
		t.Errorf("MaxElapsedTime = %v, want 20s", got)
	}
}

/*
 * 正規化是多對一，所以兩個冒號位置不同的鍵會撞成同一個名字。
 * 舊版（EnvMap）在這裡靜默取其一，取到哪一個取決於 map 的走訪順序 —— 無法重現的設定漂移。
 */
func TestParse_ColonPlacementCollisionIsReported(t *testing.T) {
	raw := telegramRaw()
	raw["GLOBAL:RABBITMQ:HOST"] = "other.host" // 與既有的 GLOBAL:RABBITMQ_HOST 同名

	_, err := parse(raw, telegramSpec())

	assertErrorMentions(t, err, "GLOBAL:RABBITMQ:HOST", "GLOBAL:RABBITMQ_HOST", "GLOBAL_RABBITMQ_HOST")
}

// 撞名是設定本身壞了，不該混在缺鍵清單裡讓人以為只是漏填
func TestParse_ColonPlacementCollision_ReturnsZeroSettings(t *testing.T) {
	raw := telegramRaw()
	raw["GLOBAL:RABBITMQ:HOST"] = "other.host"

	settings, err := parse(raw, telegramSpec())
	if err == nil {
		t.Fatal("expected an error for colliding keys")
	}

	if settings.ServiceName != "" || settings.Service != nil {
		t.Errorf("expected zero Settings alongside the error, got %+v", settings)
	}
}
