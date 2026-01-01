package kdb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"iter"

	"github.com/KrakenTech-LLC/KrknDB/internal/util"
	"github.com/dgraph-io/badger/v4"
)

// StoreHash stores a hash in the database
func (kc *KDB) StoreHash(sh *Hash) error {
	kc.mu.Lock()
	defer kc.mu.Unlock()
	err := kc.c.Update(func(txn *badger.Txn) error {
		// Marshal the hash to JSON
		data, err := json.Marshal(sh)
		if err != nil {
			return fmt.Errorf("failed to marshal hash: %w", err)
		}

		// Store the hash with the generated key
		return txn.Set(sh.Key, data)
	})

	if err != nil {
		return fmt.Errorf("failed to store hash: %w", err)
	}

	// Increment the total hash count (outside the mutex lock to avoid deadlock)
	kc.incrementTotalHashCount()
	kc.incrementHashTypeCount(sh.HashType)

	return nil
}

// GetHashBySum retrieves a hash by its hex-encoded SHA256 sum and hash type
// This is the most efficient method for finding a single hash by exact sum (O(1) lookup)
func (kc *KDB) GetHashBySum(hexSum string, hashType uint64) (*Hash, error) {
	kc.mu.Lock()
	defer kc.mu.Unlock()

	key := []byte(fmt.Sprintf(storedHashPrefix, hashType, hexSum))
	var hash *Hash

	err := kc.c.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			hash = &Hash{}
			return json.Unmarshal(val, hash)
		})
	})

	if err != nil {
		return nil, err
	}

	return hash, nil
}

// GetHashByOriginalHash retrieves a hash by the original hash string and hash type
// This computes the SHA256 sum and does a direct lookup (O(1))
func (kc *KDB) GetHashByOriginalHash(originalHash string, hashType uint64) (*Hash, error) {
	kc.mu.Lock()
	defer kc.mu.Unlock()

	// Compute the SHA256 sum of the original hash
	hexSum := string(util.SHA256Sum(originalHash))
	key := []byte(fmt.Sprintf(storedHashPrefix, hashType, hexSum))
	var hash *Hash

	err := kc.c.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			hash = &Hash{}
			return json.Unmarshal(val, hash)
		})
	})

	if err != nil {
		return nil, err
	}

	return hash, nil
}

// GetHashesByHashType returns an iterator that yields all hashes of a specific hash type
// This is a generator function that allows efficient iteration over large datasets
func (kc *KDB) GetHashesByHashType(hashType uint64) iter.Seq[*Hash] {
	return func(yield func(*Hash) bool) {
		kc.mu.Lock()
		defer kc.mu.Unlock()

		// Create the prefix for this hash type
		prefix := []byte(fmt.Sprintf(hashTypeLookupPrefix, hashType))

		// Start a read transaction
		err := kc.c.View(func(txn *badger.Txn) error {
			// Create iterator options with prefix
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefix

			// Create a new iterator
			it := txn.NewIterator(opts)
			defer it.Close()

			// Iterate over all keys with this prefix
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()

				// Get the value
				err := item.Value(func(val []byte) error {
					var hash Hash
					if err := json.Unmarshal(val, &hash); err != nil {
						return err
					}

					// Yield the hash to the caller
					// If yield returns false, stop iteration
					if !yield(&hash) {
						return fmt.Errorf("iteration stopped")
					}
					return nil
				})

				if err != nil {
					// If iteration was stopped, break
					if err.Error() == "iteration stopped" {
						break
					}
					// For other errors, continue to next item
					continue
				}
			}
			return nil
		})

		// Log error if any (in production, you might want to handle this differently)
		if err != nil && err.Error() != "iteration stopped" {
			// Error occurred during iteration
			_ = err
		}
	}
}

// FindHashes finds hashes by their hex-encoded SHA256 sum
//
// PERFORMANCE: This efficiently scans all hashes of the given type ONCE and filters by the possible hashes.
//
// Single prefix scan + hashmap filter = O(m) where m = hashes of this type
//
// Example: If you have 1 million total hashes, 100k of type 0, and searching for 1000 hashes:
// - Individual lookups: 1000 * log(1M) ≈ 20,000 operations
// - Prefix scan + filter: 100k scans + 1000 O(1) hashmap checks ≈ 100k operations
//
// The prefix scan becomes MORE efficient as the number of search hashes increases.
// Break-even point is typically around 10-50 hashes depending on dataset size.
//
// For single hash lookups, use GetHashByOriginalHash() instead (O(1) direct lookup).
func (kc *KDB) FindHashes(possibleHashes []string, hashType uint64) iter.Seq[*Hash] {
	return func(yield func(*Hash) bool) {
		if len(possibleHashes) == 0 {
			return
		}

		kc.mu.Lock()
		defer kc.mu.Unlock()

		// Create a map of hex sums for O(1) lookup
		sumMap := make(map[string]bool, len(possibleHashes))
		for _, hashStr := range possibleHashes {
			hexSum := util.SHA256Sum(hashStr)
			sumMap[string(hexSum)] = true
		}

		// Create the prefix for this hash type (scan all hashes of this type)
		prefix := []byte(fmt.Sprintf(hashTypeLookupPrefix, hashType))

		err := kc.c.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefix
			opts.PrefetchValues = true

			it := txn.NewIterator(opts)
			defer it.Close()

			// Iterate over all hashes of this type
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()

				err := item.Value(func(val []byte) error {
					var hash Hash
					if err := json.Unmarshal(val, &hash); err != nil {
						return err
					}

					// Check if this hash's sum is in our search set
					if sumMap[string(hash.Sum)] {
						if !yield(&hash) {
							return fmt.Errorf("iteration stopped")
						}
					}
					return nil
				})

				if err != nil && err.Error() == "iteration stopped" {
					break
				}
			}
			return nil
		})

		if err != nil && err.Error() != "iteration stopped" {
			_ = err
		}
	}
}

// SearchHashesByPrefix searches for hashes where the hex sum starts with the given prefix
// This is useful for partial hash lookups
func (kc *KDB) SearchHashesByPrefix(hexPrefix string, hashType uint64) iter.Seq[*Hash] {
	return func(yield func(*Hash) bool) {
		kc.mu.Lock()
		defer kc.mu.Unlock()

		// Create the search prefix
		searchPrefix := []byte(fmt.Sprintf(storedHashPrefix, hashType, hexPrefix))

		err := kc.c.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = true

			it := txn.NewIterator(opts)
			defer it.Close()

			// Seek to the prefix
			for it.Seek(searchPrefix); it.Valid(); it.Next() {
				item := it.Item()
				key := item.Key()

				// Check if the key still matches our prefix
				if !bytes.HasPrefix(key, searchPrefix) {
					break
				}

				err := item.Value(func(val []byte) error {
					var hash Hash
					if err := json.Unmarshal(val, &hash); err != nil {
						return err
					}

					if !yield(&hash) {
						return fmt.Errorf("iteration stopped")
					}
					return nil
				})

				if err != nil && err.Error() == "iteration stopped" {
					break
				}
			}
			return nil
		})

		if err != nil && err.Error() != "iteration stopped" {
			_ = err
		}
	}
}

func (kc *KDB) getCount(key string) (int, error) {
	var count int

	err := kc.c.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			count = int(binary.BigEndian.Uint64(val))
			return nil
		})
	})

	if err != nil {
		return 0, err
	}

	return count, nil
}

func (kc *KDB) updateCount(key string, delta int) error {
	return kc.c.Update(func(txn *badger.Txn) error {
		var count int
		item, err := txn.Get([]byte(key))
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}

		if err == nil {
			err = item.Value(func(val []byte) error {
				count = int(binary.BigEndian.Uint64(val))
				return nil
			})
			if err != nil {
				return err
			}
		}

		count += delta
		return txn.Set([]byte(key), []byte(fmt.Sprintf("%d", count)))
	})
}

func (kc *KDB) initializeCounter(key string, initial int) error {
	return kc.c.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), []byte(fmt.Sprintf("%d", initial)))
	})
}

func (kc *KDB) checkCounter(key string) error {
	var exists bool

	err := kc.c.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			exists = false
			return nil
		}

		if err != nil {
			logger(fmt.Sprintf("failed to check counter: %v", err), Error)
			return err
		}

		exists = true
		return nil
	})

	if err != nil {
		logger(fmt.Sprintf("failed to check counter: %v", err), Error)
		return err
	}

	if !exists {
		err = kc.initializeCounter(key, 0)
		if err != nil {
			logger(fmt.Sprintf("failed to initialize counter: %v", err), Error)
			return err
		}
	}

	return err
}
