package timerx

import (
	"testing"
)

func TestSanitizeKeyPrefix(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"normal prefix", "myapp", "myapp"},
		{"with left brace", "my{app", "my_app"},
		{"with right brace", "my}app", "my_app"},
		{"with both braces", "my{app}env", "my_app_env"},
		{"only left brace", "{", "_"},
		{"only right brace", "}", "_"},
		{"empty string", "", ""},
		{"multiple braces", "a{b{c}d}e", "a_b_c_d_e"},
		{"braces at edges", "{prefix}", "_prefix_"},
		{"already clean", "clean_key_123", "clean_key_123"},
		{"special chars no braces", "my:app/ver", "my:app/ver"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeKeyPrefix(tt.input)
			if got != tt.expected {
				t.Errorf("sanitizeKeyPrefix(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestSanitizeKeyPrefixConsistency(t *testing.T) {
	// 两个仅含 {} 不同的 keyPrefix 应该 sanitize 成相同值
	a := sanitizeKeyPrefix("foo{bar")
	b := sanitizeKeyPrefix("foo_bar")
	if a != b {
		t.Errorf("expected consistent sanitization: %q != %q", a, b)
	}
}
