package timerx

import "strings"

// sanitizeKeyPrefix 移除 keyPrefix 中的 {} 字符，避免破坏 Redis Cluster hash tag
// Redis Cluster 使用 first {..} 配对作为 hash tag，keyPrefix 中包含的 {} 会干扰此机制
func sanitizeKeyPrefix(keyPrefix string) string {
	keyPrefix = strings.ReplaceAll(keyPrefix, "{", "_")
	keyPrefix = strings.ReplaceAll(keyPrefix, "}", "_")
	return keyPrefix
}
