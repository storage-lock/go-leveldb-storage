package leveldb_storage

import (
	"github.com/storage-lock/go-storage"
	"github.com/syndtr/goleveldb/leveldb"
)

// LevelDBStorageOptions LevelDB 的存储选项
type LevelDBStorageOptions struct {

	// 获取连接
	ConnectionManager storage.ConnectionManager[*leveldb.DB]

	// KeyPrefix LevelDB 中 key 的前缀，用于隔离不同的锁实例
	// 实际的 key 格式为：<KeyPrefix>:lock:<lockId>:version 和 <KeyPrefix>:lock:<lockId>:json
	KeyPrefix string
}

func NewLevelDBStorageOptions() *LevelDBStorageOptions {
	return &LevelDBStorageOptions{
		KeyPrefix: storage.DefaultStorageTableName,
	}
}

func NewLevelDBStorageOptionsWithPath(path string) *LevelDBStorageOptions {
	return NewLevelDBStorageOptions().SetConnectionManager(NewLevelDBConnectionManagerFromPath(path))
}

func (x *LevelDBStorageOptions) SetKeyPrefix(keyPrefix string) *LevelDBStorageOptions {
	x.KeyPrefix = keyPrefix
	return x
}

func (x *LevelDBStorageOptions) SetConnectionManager(connectionManager storage.ConnectionManager[*leveldb.DB]) *LevelDBStorageOptions {
	x.ConnectionManager = connectionManager
	return x
}

func (x *LevelDBStorageOptions) Check() error {
	if x.KeyPrefix == "" {
		return ErrKeyPrefixEmpty
	}
	if x.ConnectionManager == nil {
		return ErrConnectionManagerNil
	}
	return nil
}
