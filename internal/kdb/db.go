package kdb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/KrakenTech-LLC/KrknDB/internal/util"
	"github.com/dgraph-io/badger/v3"
)

var (
	initOnce sync.Once
	krkn     *KDB
	logger   Logger
)

const (
	storedHashPrefix     = "krkn:%d:%v" // hash_type:stored_hash.Key
	hashTypeLookupPrefix = "krkn:%d"    // hash_type

	// Counters
	totalHashesKey      = "krkn:total_hashes"
	hashTypeCountPrefix = "krkn:num:%d" // hash_type
)

// KDB represents the key-value database
type KDB struct {
	encryptionKey []byte
	c             *badger.DB // badger database
	mu            sync.Mutex // mutex for concurrent access
	isNew         bool       // true if the krkn is new
	absPath       string     // absolute path to the database file
	parentFolder  string     // absolute path to the parent folder
	logger        Logger
}

// New creates a new KDB instance
// dbFolder is the folder to store the database in
// encryptionKey is the 32-byte encryption key
// opts is an optional set of KDBOptions
func New(dbFolder string, encryptionKey []byte, opts ...*Options) (*KDB, error) {
	var (
		err       error
		absPath   string
		dbOptions *Options
	)

	if len(opts) > 0 {
		dbOptions = DefaultOptions()
	} else {
		dbOptions = opts[0]
	}

	logger = dbOptions.Logger

	// Get the absolute path for the parent folder
	absPath, err = filepath.Abs(dbFolder)
	if err != nil {
		logger(fmt.Sprintf("failed to get absolute path for '%s': %v", dbFolder, err), Error)
		return nil, fmt.Errorf("failed to get absolute path for '%s': %w", dbFolder, err)
	}

	// Get the absolute path for the database file
	dbFile := filepath.Join(absPath, "krkn.db")

	if dbOptions.ValueDir == "" {
		dbOptions.ValueDir = absPath
	} else {
		dbOptions.ValueDir, err = filepath.Abs(dbOptions.ValueDir)
		if err != nil {
			logger(fmt.Sprintf("failed to get absolute path for '%s': %v", dbOptions.ValueDir, err), Error)
			return nil, fmt.Errorf("failed to get absolute path for '%s': %w", dbOptions.ValueDir, err)
		}
	}

	if len(encryptionKey) == 0 || len(encryptionKey) != 32 {
		logger("encryption key must be 32 bytes", Error)
		return nil, fmt.Errorf("encryption key must be 32 bytes")
	}

	initOnce.Do(func() {
		// Check if the krkn database already exists
		isNewDB := !util.PathExists(absPath)

		if isNewDB {
			// Create the krkn database directory
			if err = os.MkdirAll(absPath, 0700); err != nil {
				logger(fmt.Sprintf("failed to create krkn database directory: %v", err), Error)
				err = fmt.Errorf("failed to create krkn database directory: %w", err)
				return
			}
		}

		// Configure BadgerDB options
		opts := badger.DefaultOptions(absPath).
			WithValueDir(absPath).                                                      // Use the same directory for data and value files
			WithEncryptionKey(encryptionKey).                                           // Enable encryption
			WithCompression(dbOptions.Compression).                                     // Use ZSTD compression
			WithEncryptionKeyRotationDuration(dbOptions.EncryptionKeyRotationDuration). // Rotate keys daily
			WithNumVersionsToKeep(dbOptions.NumVersionsToKeep).                         // Only keep the latest version of each key
			// WithBlockCacheSize(8 << 30).                       						// 8GB block krkn
			WithIndexCacheSize(dbOptions.IndexCacheSize).                   // 10GB index krkn
			WithValueThreshold(dbOptions.ValueThreshold).                   // 64KB inline threshold
			WithValueLogFileSize(dbOptions.ValueLogFileSize).               // 2GB log files
			WithMemTableSize(dbOptions.MemTableSize).                       // 512MB memtables
			WithNumMemtables(dbOptions.NumMemTables).                       // More in-RAM tables
			WithNumCompactors(dbOptions.NumCompactors).                     // More compaction threads
			WithNumLevelZeroTables(dbOptions.NumLevelZeroTables).           // 20 L0 tables before compaction
			WithNumLevelZeroTablesStall(dbOptions.NumLevelZeroTablesStall). // 40 L0 tables before stalling
			WithBaseLevelSize(dbOptions.BaseLevelSize).                     // 20GB base level
			WithMaxLevels(dbOptions.MaxLevels).                             // 7 levels
			WithBloomFalsePositive(dbOptions.BloomFalsePositive).           // 1% false positive rate
			WithLogger(nil)                                                 // Disable logging for speed

		// Try to open the database with retries
		var db *badger.DB
		maxRetries := 3
		retryDelay := 3 * time.Second
		for i := 0; i < maxRetries; i++ {
			db, err = badger.Open(opts)
			if err == nil {
				// Successfully opened
				logger("Successfully opened database", Info)
				break
			}

			if i < maxRetries-1 {
				logger(fmt.Sprintf("Failed to open database: %v. Retrying in %v...", err, retryDelay), Warning)
				time.Sleep(retryDelay)
			}
		}

		if err != nil {
			logger(fmt.Sprintf("Failed to open database after %d retries: %v", maxRetries, err), Error)
			err = fmt.Errorf("failed to open krkn database after %d retries: %w", maxRetries, err)
			return
		}

		krkn = &KDB{
			encryptionKey: encryptionKey,
			c:             db,
			mu:            sync.Mutex{},
			isNew:         isNewDB,
			absPath:       dbFile,
			parentFolder:  absPath,
		}

		go runPeriodicCompaction()
	})

	if err != nil {
		logger(fmt.Sprintf("Failed to create database: %v", err), Error)
		return nil, err
	}

	logger("Successfully created database", Info)
	return krkn, nil
}

// Get returns the global KDB instance
func Get() *KDB {
	return krkn
}

// IsNew returns true if this is a freshly created database
func (kc *KDB) IsNew() bool {
	return kc.isNew
}

// Close closes the database
func (kc *KDB) Close() error {
	kc.mu.Lock()
	defer kc.mu.Unlock()

	return kc.c.Close()
}

// Nil returns true if the database is nil
func (kc *KDB) Nil() bool {
	return kc.c == nil
}

// ParentFolder returns the parent folder of the database
func (kc *KDB) ParentFolder() string {
	return kc.parentFolder
}

// DBPath returns the absolute path to the database file
func (kc *KDB) DBPath() string {
	return kc.absPath
}

// TotalHashes returns the total number of hashes in the database
func (kc *KDB) TotalHashes() (int, error) {
	return kc.getCount(totalHashesKey)
}

// HashesByType returns the number of hashes of a specific type in the database
func (kc *KDB) HashesByType(hashType uint64) (int, error) {
	return kc.getCount(fmt.Sprintf(hashTypeCountPrefix, hashType))
}

// SetLogger sets the logger.
// Can also be set in the options
func (kc *KDB) SetLogger(l Logger) {
	logger = l
}

// incrementTotalHashCount increments the total hash count.
// Creates the key if it doesn't exist
func (kc *KDB) incrementTotalHashCount() {
	err := krkn.updateCount(totalHashesKey, 1)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			logger("total hash count not found, initializing", Info)
			err = krkn.initializeCounter(totalHashesKey, 1)
			if err != nil {
				logger(fmt.Sprintf("failed to initialize total hash count: %v", err), Error)
			}
			return
		}
		logger(fmt.Sprintf("failed to update total hash count: %v", err), Error)
	}
}

// incrementHashTypeCount increments the hash type count.
// Creates the key if it doesn't exist
func (kc *KDB) incrementHashTypeCount(hashType uint64) {
	prefix := fmt.Sprintf(hashTypeLookupPrefix, hashType)
	err := kc.updateCount(prefix, 1)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			logger("hash type count not found, initializing", Info)
			err = krkn.initializeCounter(prefix, 1)
			if err != nil {
				logger(fmt.Sprintf("failed to initialize hash type count: %v", err), Error)
			}
			return
		}
		logger(fmt.Sprintf("failed to update hash type count: %v", err), Error)
		fmt.Printf("failed to update hash type count: %v\n", err)
	}
}

// runPeriodicCompaction runs periodic compaction on the database
func runPeriodicCompaction() {
	if krkn == nil {
		logger("krkn database is nil, skipping compaction", Warning)
		return
	}

	// Run compaction immediately on startup
	if err := krkn.c.RunValueLogGC(0.5); err != nil && err != badger.ErrNoRewrite {
		logger(fmt.Sprintf("failed to run value log GC: %v", err), Error)
		fmt.Printf("failed to run value log GC: %v\n", err)
	}

	// Then run every 6 hours
	ticker := time.NewTicker(6 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		logger("running periodic compaction", Info)
		if err := krkn.c.RunValueLogGC(0.5); err != nil && err != badger.ErrNoRewrite {
			logger(fmt.Sprintf("failed to run value log GC: %v", err), Error)
			fmt.Printf("failed to run value log GC: %v\n", err)
		}
	}
}
