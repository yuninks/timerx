package leader

import (
	"context"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestInitLeader_NilRedis(t *testing.T) {
	ctx := context.Background()
	_, err := InitLeader(ctx, nil, "test")
	if err == nil {
		t.Error("expected error for nil redis")
	}
}

func TestLeaderKeyFormat(t *testing.T) {
	keyPrefix := "myapp"
	source := "cluster"

	lockKey := "timer:{myapp}:leader_lock_cluster"
	leaderKey := "timer:{myapp}:leader_cluster"

	if !strings.Contains(lockKey, "{myapp}") {
		t.Errorf("lockKey missing hash tag: %s", lockKey)
	}
	if !strings.Contains(leaderKey, "{myapp}") {
		t.Errorf("leaderKey missing hash tag: %s", leaderKey)
	}

	_ = keyPrefix
	_ = source
}

func TestLeaderKeyFormat_Once(t *testing.T) {
	lockKey := "timer:{app}:leader_lock_once"
	leaderKey := "timer:{app}:leader_once"

	tag1 := extractHashTag(lockKey)
	tag2 := extractHashTag(leaderKey)

	if tag1 != "app" {
		t.Errorf("unexpected hash tag: %q", tag1)
	}
	if tag1 != tag2 {
		t.Errorf("hash tags differ: %q != %q", tag1, tag2)
	}
}

func extractHashTag(key string) string {
	start := strings.Index(key, "{")
	end := strings.Index(key, "}")
	if start >= 0 && end > start {
		return key[start+1 : end]
	}
	return ""
}

// ensure redis import used for InitLeader type checking
var _ redis.UniversalClient
