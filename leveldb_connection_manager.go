package leveldb_storage

import (
	"context"
	"github.com/storage-lock/go-storage"
	"github.com/syndtr/goleveldb/leveldb"
	"sync"
)

// LevelDBConnectionManager 负责维护与 LevelDB 的连接
type LevelDBConnectionManager struct {

	// LevelDB 数据库文件路径
	Path string

	// initMu 保护下面的初始化状态。原用 sync.Once，Open 首次失败后 err 永久缓存，
	// 后续 Take 永远返回同一 err 无法重试（漏洞4/liveness）。改为 mutex + initialized 标志：
	// 成功后缓存 db 不再重复 Open；失败不缓存 err，下次 Take 可重试。
	initMu      sync.Mutex
	initialized bool
	db          *leveldb.DB
}

var _ storage.ConnectionManager[*leveldb.DB] = &LevelDBConnectionManager{}

// NewLevelDBConnectionManagerFromPath 从 LevelDB 数据库文件路径创建连接管理器
func NewLevelDBConnectionManagerFromPath(path string) *LevelDBConnectionManager {
	return &LevelDBConnectionManager{
		Path: path,
	}
}

// NewLevelDBConnectionManagerFromDB 复用已经存在的 LevelDB 实例，从其创建连接管理器
func NewLevelDBConnectionManagerFromDB(db *leveldb.DB) *LevelDBConnectionManager {
	return &LevelDBConnectionManager{
		db:          db,
		initialized: true, // 外部预设 db，Take 不再 Open
	}
}

const LevelDBConnectionManagerName = "leveldb-connection-manager"

func (x *LevelDBConnectionManager) Name() string {
	return LevelDBConnectionManagerName
}

// Take 获取 LevelDB 实例。
//
// 漏洞4 修复：原 sync.Once 在 Open 失败时永久缓存 err，无法重试。
// 改为 mutex + initialized：成功后缓存 db 不重复 Open；失败不缓存 err 可重试。
func (x *LevelDBConnectionManager) Take(ctx context.Context) (*leveldb.DB, error) {
	x.initMu.Lock()
	defer x.initMu.Unlock()
	if x.initialized {
		return x.db, nil
	}
	db, err := leveldb.OpenFile(x.Path, nil)
	if err != nil {
		// 失败不缓存，下次 Take 可重试
		return nil, err
	}
	x.db = db
	x.initialized = true
	return x.db, nil
}

func (x *LevelDBConnectionManager) Return(ctx context.Context, connection *leveldb.DB) error {
	return nil
}

func (x *LevelDBConnectionManager) Shutdown(ctx context.Context) error {
	x.initMu.Lock()
	db := x.db
	x.initMu.Unlock()
	if db != nil {
		return db.Close()
	}
	return nil
}
