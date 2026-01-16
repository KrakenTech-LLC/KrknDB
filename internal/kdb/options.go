package kdb

import (
	"fmt"
	"runtime"
	"time"

	"github.com/dgraph-io/badger/v4/options"
)

type Severity int

const (
	Debug Severity = iota
	Info
	Warning
	Error
	Fatal
)

type Logger = func(string, Severity)

var DefaultLogger = func(msg string, severity Severity) {
	line := ""
	now := time.Now().Format("2006-01-02 15:04:05")
	switch severity {
	case Debug:
		line = fmt.Sprintf("%s; severity=debug; %s\n", now, msg)
	case Info:
		line = fmt.Sprintf("%s; severity=info; %s\n", now, msg)
	case Warning:
		line = fmt.Sprintf("%s; severity=warning; %s\n", now, msg)
	case Error:
		line = fmt.Sprintf("%s; severity=error; %s\n", now, msg)
	case Fatal:
		line = fmt.Sprintf("%s; severity=fatal; %s\n", now, msg)
	}
	fmt.Print(line)
}

/*
Options represents the options for the KDB

ValueDir: The directory where the value log files will be stored

Compression: The compression type to use for the value log files

EncryptionKeyRotationDuration: The duration after which the encryption key will be rotated

NumVersionsToKeep: The number of versions to keep for each key

IndexCacheSize: The size of the index krkn in bytes (should be large enough to hold the entire index)

ValueThreshold: The threshold size for values below which they will be inlined in the data block

ValueLogFileSize: The size of the value log file in bytes

MemTableSize: The size of the in-memory table in bytes

NumMemTables: The number of in-memory tables

NumCompactors: The number of compaction threads

NumLevelZeroTables: The maximum number of Level 0 tables before compaction starts

NumLevelZeroTablesStall: The number of Level 0 tables after which writes will be stalled

BaseLevelSize: The size of the base level in bytes

MaxLevels: The maximum number of levels of compaction

BloomFalsePositive: The false positive rate of the bloom filter
*/
type Options struct {
	ValueDir                      string
	Compression                   options.CompressionType
	EncryptionKeyRotationDuration time.Duration
	NumVersionsToKeep             int
	IndexCacheSize                int64
	ValueThreshold                int64
	ValueLogFileSize              int64
	MemTableSize                  int64
	NumMemTables                  int
	NumCompactors                 int
	NumLevelZeroTables            int
	NumLevelZeroTablesStall       int
	BaseLevelSize                 int64
	MaxLevels                     int
	BloomFalsePositive            float64
	Logger                        Logger
}

/*
DefaultOptions returns the default options for the KDB

	Compression: ZSTD - ZSTD compression

	EncryptionKeyRotationDuration: 24 hours - Rotate encryption keys daily

	NumVersionsToKeep: 1 - Only keep the latest version of each key

	IndexCacheSize: 10GB - 10GB index krkn

	ValueThreshold: 64KB - Values below this size will be inlined in the data block

	ValueLogFileSize: 2GB - Size of the value log file

	MemTableSize: 512MB - Size of the in-memory table

	NumMemTables: 10 - Number of in-memory tables

	NumCompactors: NumCPU() - Number of compaction threads

	NumLevelZeroTables: 20 - Maximum number of L0 tables before compaction

	NumLevelZeroTablesStall: 40 - Stop writes when there are 40 L0 tables

	BaseLevelSize: 20GB - Base level size is 20GB

	MaxLevels: 7 - Maximum number of levels of compaction

	BloomFalsePositive: 0.01 - Describes the false positive rate of the bloom filter.
*/
func DefaultOptions() *Options {
	return &Options{
		ValueDir:                      "",
		Compression:                   options.ZSTD,
		EncryptionKeyRotationDuration: 24 * time.Hour,
		NumVersionsToKeep:             1,
		IndexCacheSize:                10 << 30,
		ValueThreshold:                1 << 16,
		ValueLogFileSize:              (2 << 30) - 1,
		MemTableSize:                  512 << 20,
		NumMemTables:                  10,
		NumCompactors:                 runtime.NumCPU(),
		NumLevelZeroTables:            20,
		NumLevelZeroTablesStall:       40,
		BaseLevelSize:                 20 << 30,
		MaxLevels:                     7,
		BloomFalsePositive:            0.01,
		Logger:                        DefaultLogger,
	}
}
