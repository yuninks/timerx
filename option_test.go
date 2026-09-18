package timerx

import (
	"testing"
	"time"

	"github.com/yuninks/timerx/logger"
)

func TestDefaultOptions(t *testing.T) {
	o := defaultOptions()

	if o.timeout != time.Hour {
		t.Errorf("default timeout = %v, want %v", o.timeout, time.Hour)
	}
	if o.location != time.Local {
		t.Errorf("default location = %v, want Local", o.location)
	}
	if o.priorityType != priorityTypeNone {
		t.Errorf("default priorityType = %v, want %v", o.priorityType, priorityTypeNone)
	}
	if o.batchSize != 100 {
		t.Errorf("default batchSize = %d, want 100", o.batchSize)
	}
	if o.maxRunCount != 0 {
		t.Errorf("default maxRunCount = %d, want 0", o.maxRunCount)
	}
	if o.maxWorkers != 100 {
		t.Errorf("default maxWorkers = %d, want 100", o.maxWorkers)
	}
	if o.cronParser == nil {
		t.Error("default cronParser should not be nil")
	}
}

func TestNewOptions(t *testing.T) {
	o := newOptions()
	// newOptions 应用默认值
	if o.batchSize != 100 {
		t.Errorf("expected default batchSize 100, got %d", o.batchSize)
	}
}

func TestNewEmptyOptions(t *testing.T) {
	o := newEmptyOptions()
	// newEmptyOptions 返回零值，不应用默认值
	if o.batchSize != 0 {
		t.Errorf("expected empty batchSize 0, got %d", o.batchSize)
	}
	if o.maxWorkers != 0 {
		t.Errorf("expected empty maxWorkers 0, got %d", o.maxWorkers)
	}
}

func TestWithBatchSize(t *testing.T) {
	tests := []struct {
		input    int
		expected int
	}{
		{0, 1},
		{1, 1},
		{-1, 1},
		{-100, 1},
		{50, 50},
		{1000, 1000},
	}

	for _, tt := range tests {
		o := newOptions(WithBatchSize(tt.input))
		if o.batchSize != tt.expected {
			t.Errorf("WithBatchSize(%d) = %d, want %d", tt.input, o.batchSize, tt.expected)
		}
	}
}

func TestWithMaxWorkers(t *testing.T) {
	tests := []struct {
		input    int
		expected int
	}{
		{0, 100},
		{-1, 100},
		{-100, 100},
		{1, 1},
		{50, 50},
		{200, 200},
	}

	for _, tt := range tests {
		o := newOptions(WithMaxWorkers(tt.input))
		if o.maxWorkers != tt.expected {
			t.Errorf("WithMaxWorkers(%d) = %d, want %d", tt.input, o.maxWorkers, tt.expected)
		}
	}
}

func TestWithMaxRetryCount(t *testing.T) {
	tests := []struct {
		input    int
		expected int
	}{
		{-1, 0},
		{-100, 0},
		{0, 0},
		{3, 3},
		{100, 100},
	}

	for _, tt := range tests {
		o := newOptions(WithMaxRetryCount(tt.input))
		if o.maxRunCount != tt.expected {
			t.Errorf("WithMaxRetryCount(%d) = %d, want %d", tt.input, o.maxRunCount, tt.expected)
		}
	}
}

func TestWithTimeout(t *testing.T) {
	d := 30 * time.Second
	o := newOptions(WithTimeout(d))
	if o.timeout != d {
		t.Errorf("WithTimeout(%v) = %v", d, o.timeout)
	}
}

func TestWithPriority(t *testing.T) {
	o := newOptions(WithPriority(42))
	if o.priorityType != priorityTypePriority {
		t.Errorf("priorityType = %v, want %v", o.priorityType, priorityTypePriority)
	}
	if o.priorityVal != 42 {
		t.Errorf("priorityVal = %d, want 42", o.priorityVal)
	}
}

func TestWithPriorityByVersion(t *testing.T) {
	o := newOptions(WithPriorityByVersion("v2.3.4"))
	if o.priorityType != priorityTypeVersion {
		t.Errorf("priorityType = %v, want %v", o.priorityType, priorityTypeVersion)
	}
	if o.priorityVersion != "v2.3.4" {
		t.Errorf("priorityVersion = %s, want v2.3.4", o.priorityVersion)
	}
}

func TestWithLogger(t *testing.T) {
	original := defaultOptions().logger
	custom := logger.NewLogger()
	o := newOptions(WithLogger(custom))
	// logger 不是 comparable，但地址应该不同
	if o.logger == nil {
		t.Error("logger should not be nil")
	}
	_ = original
}

func TestWithLocation(t *testing.T) {
	loc := time.UTC
	o := newOptions(WithLocation(loc))
	if o.location != loc {
		t.Errorf("location = %v, want %v", o.location, loc)
	}
}

func TestWithCronParserOptions(t *testing.T) {
	// WithCronParserSecond
	o := newOptions(WithCronParserSecond())
	if o.cronParser == nil {
		t.Error("cronParser should not be nil after WithCronParserSecond")
	}

	// WithCronParserLinux
	o = newOptions(WithCronParserLinux())
	if o.cronParser == nil {
		t.Error("cronParser should not be nil after WithCronParserLinux")
	}

	// WithCronParserDescriptor
	o = newOptions(WithCronParserDescriptor())
	if o.cronParser == nil {
		t.Error("cronParser should not be nil after WithCronParserDescriptor")
	}
}
