package leveldb_storage

import (
	"context"
	"encoding/json"
	"fmt"
	go_iterator "github.com/golang-infrastructure/go-iterator"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ------------------------------------------------ ---------------------------------------------------------------------

// LevelDBStorage 基于 LevelDB 的存储引擎实现
//
// LevelDB 是嵌入式 KV 存储，单进程使用。CAS 通过进程内互斥锁 + 版本号检查实现
// （与 MemoryStorage 类似，但数据持久化到磁盘）。时间源使用本地 time.Now()
// （单进程时钟天然单调，满足 CapabilityReliableTime）。
type LevelDBStorage struct {
	options *LevelDBStorageOptions
	db      *leveldb.DB

	// casMu 保护 CAS 操作的互斥锁
	// LevelDB 没有条件写语义（不像 Redis Lua 或 SQL WHERE version=?），
	// 因此用进程内互斥锁保护"读版本+检查+写"的原子性。
	// 这是单机进程级互斥，不跨进程——LevelDB 本身就是单进程数据库，
	// 跨进程场景应使用 Redis/etcd 等支持服务端原子性的介质。
	casMu sync.Mutex
}

var _ storage.Storage = &LevelDBStorage{}

// NewLevelDBStorage 创建一个基于 LevelDB 的存储引擎
func NewLevelDBStorage(ctx context.Context, options *LevelDBStorageOptions) (*LevelDBStorage, error) {

	if err := options.Check(); err != nil {
		return nil, err
	}

	s := &LevelDBStorage{
		options: options,
	}

	err := s.Init(ctx)
	if err != nil {
		return nil, err
	}

	return s, nil
}

const LevelDBStorageName = "leveldb-storage"

func (x *LevelDBStorage) GetName() string {
	return LevelDBStorageName
}

// Capabilities 声明 LevelDB 存储支持的能力
// LevelDB 通过进程内互斥锁保护 CAS，使用本地 time.Now() 作为时间源
func (x *LevelDBStorage) Capabilities() []storage.StorageCapability {
	return []storage.StorageCapability{
		storage.CapabilityCAS,
		storage.CapabilityReliableTime,
		storage.CapabilityAtomicDelete,
		storage.CapabilityWatch,
	}
}

func (x *LevelDBStorage) Watch(ctx context.Context, lockId string) (<-chan *storage.LockInformation, error) {
	return storage.NewPollingLockWatcher(x, 500*time.Millisecond).Watch(ctx, lockId)
}

func (x *LevelDBStorage) Init(ctx context.Context) error {
	db, err := x.options.ConnectionManager.Take(ctx)
	if err != nil {
		return err
	}
	x.db = db
	return nil
}

// versionKey 返回存储锁版本的 key
func (x *LevelDBStorage) versionKey(lockId string) string {
	return fmt.Sprintf("%s:lock:%s:version", x.options.KeyPrefix, lockId)
}

// jsonKey 返回存储锁 JSON 信息的 key
func (x *LevelDBStorage) jsonKey(lockId string) string {
	return fmt.Sprintf("%s:lock:%s:json", x.options.KeyPrefix, lockId)
}

// lockPrefix 返回锁 key 的前缀，用于 List 扫描
func (x *LevelDBStorage) lockPrefix() string {
	return fmt.Sprintf("%s:lock:", x.options.KeyPrefix)
}

// CreateWithVersion 原子创建锁记录
// LevelDB 无条件写，通过进程内互斥锁保护"检查不存在+写入"的原子性
func (x *LevelDBStorage) CreateWithVersion(ctx context.Context, lockId string, version storage.Version, lockInformation *storage.LockInformation) error {
	x.casMu.Lock()
	defer x.casMu.Unlock()

	// 检查锁是否已存在
	vKey := x.versionKey(lockId)
	_, err := x.db.Get([]byte(vKey), nil)
	if err == nil {
		// key 已存在，创建失败
		return storage_lock.ErrVersionMiss
	}
	if err != leveldb.ErrNotFound {
		return err
	}

	// 不存在，批量写入 version 和 json
	batch := new(leveldb.Batch)
	batch.Put([]byte(vKey), []byte(strconv.FormatUint(uint64(version), 10)))
	batch.Put([]byte(x.jsonKey(lockId)), []byte(lockInformation.ToJsonString()))
	return x.db.Write(batch, nil)
}

// UpdateWithVersion 原子更新锁记录
// LevelDB 无条件写，通过进程内互斥锁保护"读版本+检查+写入"的原子性
func (x *LevelDBStorage) UpdateWithVersion(ctx context.Context, lockId string, exceptedVersion, newVersion storage.Version, lockInformation *storage.LockInformation) error {
	x.casMu.Lock()
	defer x.casMu.Unlock()

	// 读取当前版本
	vKey := x.versionKey(lockId)
	// 注意：key 不存在时映射为 ErrVersionMiss 而不是 ErrLockNotFound——core 的
	// lockExpired/rlockReclaim 等抢占路径只把 ErrVersionMiss 当作"CAS 失败，可重试"，
	// 其它错误会被当作硬错误直接返回。若真正的持有者恰好在这中间 DeleteWithVersion
	// 真删了记录（与本次抢占竞态，casMu 只在单次调用内持锁，两次调用之间仍可能被其它
	// goroutine 抢先），"记录不存在"和"版本不匹配"对调用方而言是同一件事，应该重试。
	currentVersionBytes, err := x.db.Get([]byte(vKey), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return storage_lock.ErrVersionMiss
		}
		return err
	}

	// 解析版本号
	currentVersion, err := strconv.ParseUint(string(currentVersionBytes), 10, 64)
	if err != nil {
		return err
	}

	// 检查版本是否匹配
	if storage.Version(currentVersion) != exceptedVersion {
		return storage_lock.ErrVersionMiss
	}

	// 版本匹配，批量写入新 version 和 json
	batch := new(leveldb.Batch)
	batch.Put([]byte(vKey), []byte(strconv.FormatUint(uint64(newVersion), 10)))
	batch.Put([]byte(x.jsonKey(lockId)), []byte(lockInformation.ToJsonString()))
	return x.db.Write(batch, nil)
}

// DeleteWithVersion 原子删除锁记录
// LevelDB 无条件写，通过进程内互斥锁保护"读版本+检查+删除"的原子性
func (x *LevelDBStorage) DeleteWithVersion(ctx context.Context, lockId string, exceptedVersion storage.Version, lockInformation *storage.LockInformation) error {
	x.casMu.Lock()
	defer x.casMu.Unlock()

	// 读取当前版本
	vKey := x.versionKey(lockId)
	currentVersionBytes, err := x.db.Get([]byte(vKey), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return storage_lock.ErrVersionMiss
		}
		return err
	}

	// 解析版本号
	currentVersion, err := strconv.ParseUint(string(currentVersionBytes), 10, 64)
	if err != nil {
		return err
	}

	// 检查版本是否匹配
	if storage.Version(currentVersion) != exceptedVersion {
		return storage_lock.ErrVersionMiss
	}

	// 版本匹配，批量删除 version 和 json
	batch := new(leveldb.Batch)
	batch.Delete([]byte(vKey))
	batch.Delete([]byte(x.jsonKey(lockId)))
	return x.db.Write(batch, nil)
}

// Get 获取锁信息
func (x *LevelDBStorage) Get(ctx context.Context, lockId string) (string, error) {
	jKey := x.jsonKey(lockId)
	data, err := x.db.Get([]byte(jKey), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return "", storage_lock.ErrLockNotFound
		}
		return "", err
	}
	return string(data), nil
}

// GetTime 获取当前时间
// LevelDB 是嵌入式单进程数据库，使用本地 time.Now() 作为时间源
// 单进程时钟天然单调递增，满足 CapabilityReliableTime
func (x *LevelDBStorage) GetTime(ctx context.Context) (time.Time, error) {
	return time.Now(), nil
}

// Close 关闭存储引擎
// 连接由 ConnectionManager 管理，Storage 只清空引用
func (x *LevelDBStorage) Close(ctx context.Context) error {
	x.db = nil
	return nil
}

// List 列出当前所有的锁信息
// 使用 LevelDB 的范围扫描功能收集所有锁
func (x *LevelDBStorage) List(ctx context.Context) (go_iterator.Iterator[*storage.LockInformation], error) {
	prefix := x.lockPrefix()
	slice := make([]*storage.LockInformation, 0)

	iter := x.db.NewIterator(&util.Range{Start: []byte(prefix)}, nil)
	defer iter.Release()

	// 遍历所有 :json 后缀的 key
	for iter.Next() {
		key := string(iter.Key())
		// 只取 json key（跳过 version key）
		if !strings.HasSuffix(key, ":json") {
			continue
		}
		jsonStr := string(iter.Value())
		info := &storage.LockInformation{}
		if err := json.Unmarshal([]byte(jsonStr), info); err != nil {
			// 数据损坏，跳过
			continue
		}
		slice = append(slice, info)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}

	return go_iterator.FromSlice(slice), nil
}

// ------------------------------------------------ ---------------------------------------------------------------------
