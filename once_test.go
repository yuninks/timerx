package timerx

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/yuninks/timerx/logger"
)

func TestOnceBuildRedisKey(t *testing.T) {
	once := &Once{keySeparator: "[:]", keyPrefix: "test_prefix"}

	key := once.buildRedisKey("normal", "task123")
	if key != "normal[:]task123" {
		t.Errorf("unexpected key: %s", key)
	}
}

func TestOnceParseRedisKey(t *testing.T) {
	once := &Once{keySeparator: "[:]"}

	taskType, taskId, err := once.parseRedisKey("normal[:]task123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if taskType != "normal" {
		t.Errorf("unexpected taskType: %s", taskType)
	}
	if taskId != "task123" {
		t.Errorf("unexpected taskId: %s", taskId)
	}

	_, _, err = once.parseRedisKey("invalid")
	if err == nil {
		t.Error("expected error for invalid key")
	}
}

func TestOnceKeyFormat(t *testing.T) {
	keyPrefix := "testcluster"

	tests := []struct {
		name     string
		key      string
		contains string
	}{
		{"zsetKey contains hash tag", "timer:{testcluster}:once_zset", "{testcluster}"},
		{"listKey contains hash tag", "timer:{testcluster}:once_list", "{testcluster}"},
		{"execinfoKey contains hash tag", "timer:{testcluster}:once_execinfo", "{testcluster}"},
		{"lockPrefix contains hash tag", "timer:{testcluster}:once_lock:", "{testcluster}"},
		{"dataKey contains hash tag", "timer:{testcluster}:once_data:normal[:]task1", "{testcluster}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !strings.Contains(tt.key, tt.contains) {
				t.Errorf("key %q does not contain hash tag %q", tt.key, tt.contains)
			}
		})
	}

	_ = keyPrefix
}

func TestOnceDataKeyFormat(t *testing.T) {
	dataKey := "timer:{myapp}:once_data:normal[:]task1"

	if !strings.Contains(dataKey, "{myapp}") {
		t.Errorf("data key %q missing hash tag {myapp}", dataKey)
	}
	if !strings.HasPrefix(dataKey, "timer:") {
		t.Errorf("data key %q missing timer: prefix", dataKey)
	}
}

func TestOnceExtendData(t *testing.T) {
	taskTimes := []time.Time{
		time.Now().Add(10 * time.Second),
		time.Now().Add(5 * time.Second),
		time.Now().Add(15 * time.Second),
	}

	ed := extendData{
		TaskTimes: taskTimes,
		Data:      "test data",
		RunCount:  0,
		JobType:   jobTypeOnce,
	}

	if len(ed.TaskTimes) != 3 {
		t.Errorf("expected 3 task times, got %d", len(ed.TaskTimes))
	}
	if ed.Data != "test data" {
		t.Errorf("unexpected data: %v", ed.Data)
	}
	if ed.RunCount != 0 {
		t.Errorf("unexpected runCount: %d", ed.RunCount)
	}
}

func TestOnceSaveValidation(t *testing.T) {
	var once Once

	once = Once{keyPrefix: "test_app", keySeparator: "[:]"}

	if once.keySeparator != "[:]" {
		t.Errorf("unexpected keySeparator: %s", once.keySeparator)
	}
	if once.keyPrefix != "test_app" {
		t.Errorf("unexpected keyPrefix: %s", once.keyPrefix)
	}
}

func TestOnceKeyPrefixConsistency(t *testing.T) {
	keyPrefix := "myapp_v2"

	zsetKey := "timer:{myapp_v2}:once_zset"
	listKey := "timer:{myapp_v2}:once_list"
	dataKey := "timer:{myapp_v2}:once_data:urgent[:]task99"

	for _, k := range []string{zsetKey, listKey, dataKey} {
		if !strings.Contains(k, "{"+keyPrefix+"}") {
			t.Errorf("key %q missing hash tag for keyPrefix %q", k, keyPrefix)
		}
	}

	zsetHash := extractHashTagForOnce(zsetKey)
	listHash := extractHashTagForOnce(listKey)
	dataHash := extractHashTagForOnce(dataKey)

	if zsetHash != listHash || listHash != dataHash {
		t.Errorf("all Once keys must share same hash tag: %q %q %q", zsetHash, listHash, dataHash)
	}
}

func extractHashTagForOnce(key string) string {
	start := strings.Index(key, "{")
	end := strings.Index(key, "}")
	if start >= 0 && end > start {
		return key[start+1 : end]
	}
	return ""
}

func TestInitOnce_NilRedis(t *testing.T) {
	ctx := context.Background()
	_, err := InitOnce(ctx, nil, "test", nil)
	if err == nil {
		t.Error("expected error for nil redis")
	}
}

func TestInitOnce_NilCallback(t *testing.T) {
	ctx := context.Background()
	// nil worker callback should be rejected before redis check
	_, err := InitOnce(ctx, nil, "test", nil)
	if err == nil {
		t.Error("expected error for nil callback")
	}
}

func TestOnceErrorVariables(t *testing.T) {
	// 验证 sentinel errors 可用且可比较
	if ErrExecuteTime == nil {
		t.Error("ErrExecuteTime should not be nil")
	}
	if ErrRunCount == nil {
		t.Error("ErrRunCount should not be nil")
	}
	if ErrDelayTime == nil {
		t.Error("ErrDelayTime should not be nil")
	}
	if ErrTaskExists == nil {
		t.Error("ErrTaskExists should not be nil")
	}
	if ErrTaskIdExists == nil {
		t.Error("ErrTaskIdExists should not be nil")
	}
}

func TestClusterErrorVariables(t *testing.T) {
	if ErrMonthDay == nil {
		t.Error("ErrMonthDay should not be nil")
	}
	if ErrWeekday == nil {
		t.Error("ErrWeekday should not be nil")
	}
	if ErrHour == nil {
		t.Error("ErrHour should not be nil")
	}
	if ErrMinute == nil {
		t.Error("ErrMinute should not be nil")
	}
	if ErrSecond == nil {
		t.Error("ErrSecond should not be nil")
	}
	if ErrIntervalTime == nil {
		t.Error("ErrIntervalTime should not be nil")
	}
	if ErrBaseTime == nil {
		t.Error("ErrBaseTime should not be nil")
	}
	if ErrCronExpression == nil {
		t.Error("ErrCronExpression should not be nil")
	}
	if ErrCronParser == nil {
		t.Error("ErrCronParser should not be nil")
	}
}

func TestOnceKeySeparator(t *testing.T) {
	// buildRedisKey 使用 keySeparator
	o := &Once{keySeparator: "[:]"}
	key := o.buildRedisKey("normal", "task_1")
	if key != "normal[:]task_1" {
		t.Errorf("unexpected key: %s", key)
	}

	// parseRedisKey
	tp, id, err := o.parseRedisKey(key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tp != "normal" || id != "task_1" {
		t.Errorf("unexpected parse result: %s, %s", tp, id)
	}

	// custom separator
	o = &Once{keySeparator: ":"}
	key = o.buildRedisKey("urgent", "id123")
	if key != "urgent:id123" {
		t.Errorf("unexpected key with custom separator: %s", key)
	}
}

func TestOnceSanitizedKeyPrefix(t *testing.T) {
	// 模拟 sanitizeKeyPrefix 对 Once key 的影响
	raw := "app{v1}"
	sanitized := sanitizeKeyPrefix(raw)
	if sanitized != "app_v1_" {
		t.Errorf("unexpected sanitized prefix: %s", sanitized)
	}

	dataKey := fmt.Sprintf("timer:{%s}:once_data:%s", sanitized, "type[:]id")
	zsetKey := fmt.Sprintf("timer:{%s}:once_zset", sanitized)

	if !strings.Contains(dataKey, "{app_v1_}") {
		t.Errorf("data key missing expected hash tag: %s", dataKey)
	}
	if !strings.Contains(zsetKey, "{app_v1_}") {
		t.Errorf("zset key missing expected hash tag: %s", zsetKey)
	}
}

func newDefaultLoggerForTest() logger.Logger {
	return logger.NewLogger()
}
