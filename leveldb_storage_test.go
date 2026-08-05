package leveldb_storage

import (
	"context"
	storage_test_helper "github.com/storage-lock/go-storage-test-helper"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestLevelDBStorage(t *testing.T) {
	// 创建临时目录作为 LevelDB 数据库路径
	tmpDir, err := os.MkdirTemp("", "storage-lock-leveldb-test-*")
	assert.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	connectionManager := NewLevelDBConnectionManagerFromPath(tmpDir)
	s, err := NewLevelDBStorage(context.Background(), &LevelDBStorageOptions{
		ConnectionManager: connectionManager,
		KeyPrefix:         storage_test_helper.TestTableName,
	})
	assert.Nil(t, err)
	storage_test_helper.TestStorage(t, s)
}
