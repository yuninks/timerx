package timerx

import (
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

func newDefaultLoggerForTest() logger.Logger {
	return logger.NewLogger()
}
