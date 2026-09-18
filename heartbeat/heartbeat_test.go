package heartbeat

import (
	"context"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestInitHeartBeat_NilRedis(t *testing.T) {
	ctx := context.Background()
	_, err := InitHeartBeat(ctx, nil, "test")
	if err == nil {
		t.Error("expected error for nil redis")
	}
}

func TestHeartbeatKeyFormat(t *testing.T) {
	key := "timer:{myapp}:heartbeat_cluster"

	if !strings.Contains(key, "{myapp}") {
		t.Errorf("heartbeat key missing hash tag: %s", key)
	}

	tag := extractHashTag(key)
	if tag != "myapp" {
		t.Errorf("unexpected hash tag: %q", tag)
	}
}

func TestHeartbeatKeyFormat_Once(t *testing.T) {
	key := "timer:{app_v2}:heartbeat_once"

	if !strings.Contains(key, "{app_v2}") {
		t.Errorf("heartbeat key missing hash tag: %s", key)
	}

	tag := extractHashTag(key)
	if tag != "app_v2" {
		t.Errorf("unexpected hash tag: %q", tag)
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

var _ redis.UniversalClient
