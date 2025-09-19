package logger

import (
	"context"
	"log"
)

type Logger interface {
	Infof(ctx context.Context, format string, v ...any)
	// Warnf(ctx context.Context, format string, v ...any)
	Errorf(ctx context.Context, format string, v ...any)
}

type defaultLogger struct{}

func NewLogger() *defaultLogger {
	return &defaultLogger{}
}

func (l *defaultLogger) Infof(ctx context.Context, format string, v ...any) {
	log.Printf("[INFO] "+format, v...)
}

// func (l *defaultLogger) Warnf(ctx context.Context, format string, v ...any) {
// 	log.Printf("[WARN] "+format, v...)
// }

func (l *defaultLogger) Errorf(ctx context.Context, format string, v ...any) {
	log.Printf("[ERROR] "+format, v...)
}
