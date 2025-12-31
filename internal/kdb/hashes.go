package kdb

import (
	"fmt"

	"github.com/KrakenTech-LLC/KrknDB/internal/util"
)

// Hash represents a cryptographic hash and its cracked value
type Hash struct {
	Hash     string `json:"hash"`
	Sum      []byte `json:"sum"`       // Hex Encoded SHA256 Sum of the Hash
	Value    string `json:"value"`     // The Password or Secret
	HashType uint64 `json:"hash_type"` // The hashcat code for the hash (0 - 99999)
	Key      []byte `json:"key"`       // The key used to store the hash
}

// NewHash creates a new Hash object
func NewHash(hash, value string, hashType uint64) *Hash {
	sh := &Hash{
		Hash:     hash,
		Value:    value,
		HashType: hashType,
	}
	sh.generateKey()
	return sh
}

// generateKey computes the SHA256 sum and generates the key
func (sh *Hash) generateKey() {
	sh.Sum = util.SHA256Sum(sh.Hash)
	sh.Key = []byte(fmt.Sprintf(storedHashPrefix, sh.HashType, string(sh.Sum)))
}

// Store stores the hash in the database
func (sh *Hash) Store() error {
	return krkn.StoreHash(sh)
}
