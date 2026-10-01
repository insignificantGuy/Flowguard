// Package logger wraps zerolog.
//
// Structured JSON logging to stdout is the contract: in production the JSON
// is ingested by a log aggregator; in development it pretty-prints.
package logger

import (
	"os"
	"time"

	"github.com/rs/zerolog"
)

// Log is the package-level structured logger.
var Log = zerolog.New(os.Stdout).
	With().
	Timestamp().
	Str("service", "flowguard").
	Logger().
	Level(level())

func level() zerolog.Level {
	switch os.Getenv("LOG_LEVEL") {
	case "trace":
		return zerolog.TraceLevel
	case "debug":
		return zerolog.DebugLevel
	case "warn":
		return zerolog.WarnLevel
	case "error":
		return zerolog.ErrorLevel
	case "fatal":
		return zerolog.FatalLevel
	default:
		return zerolog.InfoLevel
	}
}

// Now returns the current UTC time for response envelopes.
func Now() string { return time.Now().UTC().Format(time.RFC3339Nano) }