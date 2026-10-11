package leveldb_storage

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/storage-lock/go-storage"
	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
)

func newLevelDBTestStorage(t *testing.T) (*LevelDBStorage, func()) {
	tmpDir, err := os.MkdirTemp("", "storage-lock-leveldb-cov-*")
	assert.Nil(t, err)
	cm := NewLevelDBConnectionManagerFromPath(tmpDir)
	s, err := NewLevelDBStorage(context.Background(), &LevelDBStorageOptions{
		ConnectionManager: cm,
		KeyPrefix:         "covtest",
	})
	assert.Nil(t, err)
	return s, func() { _ = cm.Shutdown(context.Background()); os.RemoveAll(tmpDir) }
}

func TestCoverageOptions(t *testing.T) {
	// NewLevelDBStorageOptions 默认值
	opts := NewLevelDBStorageOptions()
	assert.Equal(t, storage.DefaultStorageTableName, opts.KeyPrefix)
	// SetKeyPrefix
	opts.SetKeyPrefix("p1")
	assert.Equal(t, "p1", opts.KeyPrefix)
	// SetConnectionManager
	tmpDir, err := os.MkdirTemp("", "storage-lock-leveldb-cov-opt-*")
	assert.Nil(t, err)
	defer os.RemoveAll(tmpDir)
	cm := NewLevelDBConnectionManagerFromPath(tmpDir)
	ret := opts.SetConnectionManager(cm)
	assert.Equal(t, opts, ret)
	assert.Equal(t, cm, opts.ConnectionManager)
	// Check 正常
	assert.Nil(t, opts.Check())

	// NewLevelDBStorageOptionsWithPath
	opts2 := NewLevelDBStorageOptionsWithPath(tmpDir)
	assert.NotNil(t, opts2.ConnectionManager)
	assert.Nil(t, opts2.Check())

	// Check: KeyPrefix 为空
	bad1 := &LevelDBStorageOptions{ConnectionManager: cm, KeyPrefix: ""}
	assert.Equal(t, ErrKeyPrefixEmpty, bad1.Check())
	// Check: ConnectionManager 为空
	bad2 := &LevelDBStorageOptions{KeyPrefix: "p"}
	assert.Equal(t, ErrConnectionManagerNil, bad2.Check())
}

func TestCoverageConnectionManagerFromDB(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-lock-leveldb-cov-cm-*")
	assert.Nil(t, err)
	defer os.RemoveAll(tmpDir)
	db, err := leveldb.OpenFile(tmpDir, nil)
	assert.Nil(t, err)
	defer db.Close()

	cm := NewLevelDBConnectionManagerFromDB(db)
	assert.Equal(t, LevelDBConnectionManagerName, cm.Name())
	// 预设 db 的 Take 直接返回
	got, err := cm.Take(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, db, got)
	// Return
	assert.Nil(t, cm.Return(context.Background(), db))
	// Shutdown（关闭外部 db）
	assert.Nil(t, cm.Shutdown(context.Background()))
}

func TestCoverageConnectionManagerTakeFailureAndRetry(t *testing.T) {
	// 无效路径：Take 失败且不缓存，可重试
	cm := NewLevelDBConnectionManagerFromPath("/proc/storage-lock-nonexistent-dir/db")
	_, err := cm.Take(context.Background())
	assert.NotNil(t, err)
	_, err = cm.Take(context.Background())
	assert.NotNil(t, err)
	// 从未成功初始化，Shutdown 返回 nil
	assert.Nil(t, cm.Shutdown(context.Background()))
	// 修复路径后可重试成功
	tmpDir, err := os.MkdirTemp("", "storage-lock-leveldb-cov-retry-*")
	assert.Nil(t, err)
	defer os.RemoveAll(tmpDir)
	cm.Path = tmpDir
	db, err := cm.Take(context.Background())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	// 再次 Take 走 initialized 分支
	db2, err := cm.Take(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, db, db2)
	assert.Nil(t, cm.Shutdown(context.Background()))
}

func TestCoverageNewStorageCheckFailure(t *testing.T) {
	_, err := NewLevelDBStorage(context.Background(), &LevelDBStorageOptions{KeyPrefix: ""})
	assert.Equal(t, ErrKeyPrefixEmpty, err)
}

func TestCoverageNewStorageInitFailure(t *testing.T) {
	cm := NewLevelDBConnectionManagerFromPath("/proc/storage-lock-nonexistent-dir/db")
	_, err := NewLevelDBStorage(context.Background(), &LevelDBStorageOptions{
		ConnectionManager: cm,
		KeyPrefix:         "p",
	})
	assert.NotNil(t, err)
}

func TestCoverageInitFailure(t *testing.T) {
	s := &LevelDBStorage{options: &LevelDBStorageOptions{
		ConnectionManager: NewLevelDBConnectionManagerFromPath("/proc/storage-lock-nonexistent-dir/db"),
		KeyPrefix:         "p",
	}}
	assert.NotNil(t, s.Init(context.Background()))
}

func TestCoverageCapabilitiesAndWatch(t *testing.T) {
	s, cleanup := newLevelDBTestStorage(t)
	defer cleanup()
	caps := s.Capabilities()
	assert.Contains(t, caps, storage.CapabilityCAS)
	assert.Contains(t, caps, storage.CapabilityReliableTime)
	assert.Contains(t, caps, storage.CapabilityAtomicDelete)
	assert.Contains(t, caps, storage.CapabilityWatch)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := s.Watch(ctx, "lock-watch-cov")
	assert.Nil(t, err)
	assert.NotNil(t, ch)
	_, err = s.Watch(ctx, "")
	assert.NotNil(t, err)
}

func TestCoverageCreateExists(t *testing.T) {
	s, cleanup := newLevelDBTestStorage(t)
	defer cleanup()
	ctx := context.Background()
	info := newCoverageLockInfo()
	assert.Nil(t, s.CreateWithVersion(ctx, "lock-dup", 1, info))
	err := s.CreateWithVersion(ctx, "lock-dup", 1, info)
	assert.NotNil(t, err)
}

func TestCoverageUpdateNotExists(t *testing.T) {
	s, cleanup := newLevelDBTestStorage(t)
	defer cleanup()
	err := s.UpdateWithVersion(context.Background(), "lock-missing", 1, 2, newCoverageLockInfo())
	assert.NotNil(t, err)
}

func TestCoverageUpdateVersionMismatch(t *testing.T) {
	s, cleanup := newLevelDBTestStorage(t)
	defer cleanup()
	ctx := context.Background()
	assert.Nil(t, s.CreateWithVersion(ctx, "lock-mm", 1, newCoverageLockInfo()))
	err := s.UpdateWithVersion(ctx, "lock-mm", 999, 2, newCoverageLockInfo())
	assert.NotNil(t, err)
}

func TestCoverageUpdateBadVersionData(t *testing.T) {
	s, cleanup := newLevelDBTestStorage(t)
	defer cleanup()
	ctx := context.Background()
	// 注入无法解析的版本号，覆盖 ParseUint 错误分支
	assert.Nil(t, s.db.Put([]byte(s.versionKey("lock-badver")), []byte("not-a-number"), nil))
	err := s.UpdateWithVersion(ctx, "lock-badver", 1, 2, newCoverageLockInfo())
	assert.NotNil(t, err)
	// Delete 同样覆盖解析错误分支
	err = s.DeleteWithVersion(ctx, "lock-badver", 1, newCoverageLockInfo())
	assert.NotNil(t, err)
}

func TestCoverageDeleteNotExists(t *testing.T) {
	s, cleanup := newLevelDBTestStorage(t)
	defer cleanup()
	err := s.DeleteWithVersion(context.Background(), "lock-missing", 1, newCoverageLockInfo())
	assert.NotNil(t, err)
}

func TestCoverageDeleteVersionMismatch(t *testing.T) {
	s, cleanup := newLevelDBTestStorage(t)
	defer cleanup()
	ctx := context.Background()
	assert.Nil(t, s.CreateWithVersion(ctx, "lock-dmm", 1, newCoverageLockInfo()))
	err := s.DeleteWithVersion(ctx, "lock-dmm", 999, newCoverageLockInfo())
	assert.NotNil(t, err)
}

func TestCoverageGetNotExists(t *testing.T) {
	s, cleanup := newLevelDBTestStorage(t)
	defer cleanup()
	_, err := s.Get(context.Background(), "lock-missing")
	assert.NotNil(t, err)
}

func TestCoverageListSkipsCorrupted(t *testing.T) {
	s, cleanup := newLevelDBTestStorage(t)
	defer cleanup()
	ctx := context.Background()
	assert.Nil(t, s.CreateWithVersion(ctx, "lock-good", 1, newCoverageLockInfo()))
	// 注入损坏的 json，覆盖 continue 分支
	assert.Nil(t, s.db.Put([]byte(s.jsonKey("lock-bad")), []byte("{invalid"), nil))
	it, err := s.List(ctx)
	assert.Nil(t, err)
	count := 0
	for it.Next() {
		_ = it.Value()
		count++
	}
	// 只有好的那条被列出
	assert.Equal(t, 1, count)
}

func TestCoverageClosedDBErrorPaths(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-lock-leveldb-cov-closed-*")
	assert.Nil(t, err)
	defer os.RemoveAll(tmpDir)
	cm := NewLevelDBConnectionManagerFromPath(tmpDir)
	s, err := NewLevelDBStorage(context.Background(), &LevelDBStorageOptions{
		ConnectionManager: cm,
		KeyPrefix:         "covtest",
	})
	assert.Nil(t, err)
	ctx := context.Background()
	// 直接关闭底层 db，使后续 Get/Write 返回 ErrClosed，覆盖：
	// Create(116) db 错误、Update(145) db 错误、Delete(179) db 错误、Get(208) db 错误
	assert.Nil(t, cm.Shutdown(ctx))
	assert.NotNil(t, s.CreateWithVersion(ctx, "lock-x", 1, newCoverageLockInfo()))
	assert.NotNil(t, s.UpdateWithVersion(ctx, "lock-x", 1, 2, newCoverageLockInfo()))
	assert.NotNil(t, s.DeleteWithVersion(ctx, "lock-x", 1, newCoverageLockInfo()))
	_, err = s.Get(ctx, "lock-x")
	assert.NotNil(t, err)
	// List(251) iter.Error 错误分支：关闭后 NewIterator 返回带 ErrClosed 的空迭代器
	_, err = s.List(ctx)
	assert.NotNil(t, err)
}

func newCoverageLockInfo() *storage.LockInformation {
	return &storage.LockInformation{
		OwnerId:         "owner-cov",
		Version:         1,
		LockCount:       1,
		LockBeginTime:   time.Now(),
		LeaseExpireTime: time.Now().Add(time.Minute),
	}
}
