package logger

import (
	"context"
	"log"
)

type Logger interface {
	Infof(ctx context.Context, format string, v ...interface{})
	Errorf(ctx context.Context, format string, v ...interface{})
}

type defaultLogger struct{}

func NewLogger() *defaultLogger {
	return &defaultLogger{}
}

func (l *defaultLogger) Infof(ctx context.Context, format string, v ...interface{}) {
	log.Printf("[INFO] "+format, v...)
}

func (l *defaultLogger) Errorf(ctx context.Context, format string, v ...interface{}) {
	log.Printf("[ERROR] "+format, v...)
}
