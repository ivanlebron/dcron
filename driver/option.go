package driver

import (
	"github.com/ivanlebron/dcron/logger"
	"time"
)

const (
	OptionTypeTimeout = 0x600
	OptionTypeLogger  = 0x601
)

type Option interface {
	Type() int
}

type TimeoutOption struct{ timeout time.Duration }

func (to TimeoutOption) Type() int                         { return OptionTypeTimeout }
func NewTimeoutOption(timeout time.Duration) TimeoutOption { return TimeoutOption{timeout: timeout} }

type LoggerOption struct{ logger logger.Logger }

func (to LoggerOption) Type() int                       { return OptionTypeLogger }
func NewLoggerOption(logger logger.Logger) LoggerOption { return LoggerOption{logger: logger} }
