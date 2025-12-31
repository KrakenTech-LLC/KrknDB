package KrknDB

import "github.com/KrakenTech-LLC/KrknDB/internal/kdb"

type KDB = kdb.KDB
type Hash = kdb.Hash
type Options = kdb.Options

type Logger = kdb.Logger

type Severity = kdb.Severity

const DEBUG = kdb.Debug
const INFO = kdb.Info
const WARNING = kdb.Warning
const ERROR = kdb.Error
const FATAL = kdb.Fatal

func NewDB(dbFolder string, encryptionKey []byte) (*kdb.KDB, error) {
	return kdb.New(dbFolder, encryptionKey)
}

func GetDB() *kdb.KDB {
	return kdb.Get()
}

func NewHash(hash, value string, hashType uint64) *kdb.Hash {
	return kdb.NewHash(hash, value, hashType)
}

func DefaultOptions() *kdb.Options {
	return kdb.DefaultOptions()
}
