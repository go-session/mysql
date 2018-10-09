package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/go-session/session"
	"github.com/json-iterator/go"
	"gopkg.in/gorp.v2"
)

var (
	_             session.ManagerStore = &managerStore{}
	_             session.Store        = &store{}
	jsonMarshal                        = jsoniter.Marshal
	jsonUnmarshal                      = jsoniter.Unmarshal
)

// NewConfig create mysql configuration instance
func NewConfig(dsn string) *Config {
	return &Config{
		DSN:             dsn,
		ConnMaxLifetime: time.Hour * 2,
		MaxOpenConns:    50,
		MaxIdleConns:    25,
	}
}

// Config mysql configuration
type Config struct {
	DSN             string
	ConnMaxLifetime time.Duration
	MaxOpenConns    int
	MaxIdleConns    int
}

// NewStore Create an instance of a mysql store,
// tableName Specify the stored table name (default go_session),
// gcInterval Time interval for executing GC (in seconds, default 600)
func NewStore(config *Config, tableName string, gcInterval int) session.ManagerStore {
	db, err := sql.Open("mysql", config.DSN)
	if err != nil {
		panic(err)
	}

	db.SetMaxOpenConns(config.MaxOpenConns)
	db.SetMaxIdleConns(config.MaxIdleConns)
	db.SetConnMaxLifetime(config.ConnMaxLifetime)

	return NewStoreWithDB(db, tableName, gcInterval)
}

// NewStoreWithDB Create an instance of a mysql store,
// tableName Specify the stored table name (default go_session),
// gcInterval Time interval for executing GC (in seconds, default 600)
func NewStoreWithDB(db *sql.DB, tableName string, gcInterval int) session.ManagerStore {
	store := &managerStore{
		db:        &gorp.DbMap{Db: db, Dialect: gorp.MySQLDialect{Encoding: "UTF8", Engine: "MyISAM"}},
		tableName: "go_session",
		stdout:    os.Stderr,
	}

	if tableName != "" {
		store.tableName = tableName
	}

	interval := 600
	if gcInterval > 0 {
		interval = gcInterval
	}
	store.ticker = time.NewTicker(time.Second * time.Duration(interval))

	store.pool = sync.Pool{
		New: func() interface{} {
			return newStore(store.db, store.tableName)
		},
	}

	store.db.AddTableWithName(SessionItem{}, store.tableName)

	err := store.db.CreateTablesIfNotExists()
	if err != nil {
		panic(err)
	}
	store.db.Exec(fmt.Sprintf("CREATE INDEX `idx_expired_at` ON %s (`expired_at`);", store.tableName))

	go store.gc()
	return store
}

type managerStore struct {
	ticker    *time.Ticker
	pool      sync.Pool
	db        *gorp.DbMap
	tableName string
	stdout    io.Writer
}

func (s *managerStore) errorf(format string, args ...interface{}) {
	if s.stdout != nil {
		buf := fmt.Sprintf(format, args...)
		s.stdout.Write([]byte(buf))
	}
}

func (s *managerStore) gc() {
	for range s.ticker.C {
		now := time.Now().Unix()
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE expired_at<=?", s.tableName)
		n, err := s.db.SelectInt(query, now)
		if err != nil {
			s.errorf("[ERROR]:%s", err.Error())
			return
		} else if n > 0 {
			_, err = s.db.Exec(fmt.Sprintf("DELETE FROM %s WHERE expired_at<=?", s.tableName), now)
			if err != nil {
				s.errorf("[ERROR]:%s", err.Error())
			}
		}
	}
}

func (s *managerStore) getValue(sid string) (string, error) {
	var item SessionItem

	err := s.db.SelectOne(&item, fmt.Sprintf("SELECT * FROM %s WHERE id=?", s.tableName), sid)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", nil
	} else if time.Now().Unix() >= item.ExpiredAt {
		return "", nil
	}

	return item.Value, nil
}

func (s *managerStore) parseValue(value string) (map[string]interface{}, error) {
	var values map[string]interface{}
	if len(value) > 0 {
		err := jsonUnmarshal([]byte(value), &values)
		if err != nil {
			return nil, err
		}
	}

	return values, nil
}

func (s *managerStore) Check(_ context.Context, sid string) (bool, error) {
	val, err := s.getValue(sid)
	if err != nil {
		return false, err
	}
	return val != "", nil
}

func (s *managerStore) Create(ctx context.Context, sid string, expired int64) (session.Store, error) {
	store := s.pool.Get().(*store)
	store.reset(ctx, sid, expired, nil)
	return store, nil
}

func (s *managerStore) Update(ctx context.Context, sid string, expired int64) (session.Store, error) {
	store := s.pool.Get().(*store)

	value, err := s.getValue(sid)
	if err != nil {
		return nil, err
	} else if value == "" {
		store.reset(ctx, sid, expired, nil)
		return store, nil
	}

	_, err = s.db.Exec(fmt.Sprintf("UPDATE %s SET expired_at=? WHERE id=?", s.tableName),
		time.Now().Add(time.Duration(expired)*time.Second).Unix(),
		sid)
	if err != nil {
		return nil, err
	}

	values, err := s.parseValue(value)
	if err != nil {
		return nil, err
	}

	store.reset(ctx, sid, expired, values)
	return store, nil
}

func (s *managerStore) Delete(_ context.Context, sid string) error {
	_, err := s.db.Exec(fmt.Sprintf("DELETE FROM %s WHERE id=?", s.tableName), sid)
	return err
}

func (s *managerStore) Refresh(ctx context.Context, oldsid, sid string, expired int64) (session.Store, error) {
	store := s.pool.Get().(*store)

	value, err := s.getValue(oldsid)
	if err != nil {
		return nil, err
	} else if value == "" {
		store.reset(ctx, sid, expired, nil)
		return store, nil
	}

	err = s.db.Insert(&SessionItem{
		ID:        sid,
		Value:     value,
		ExpiredAt: time.Now().Add(time.Duration(expired) * time.Second).Unix(),
	})
	if err != nil {
		return nil, err
	}

	err = s.Delete(nil, oldsid)
	if err != nil {
		return nil, err
	}

	values, err := s.parseValue(value)
	if err != nil {
		return nil, err
	}

	store.reset(ctx, sid, expired, values)
	return store, nil
}

func (s *managerStore) Close() error {
	s.ticker.Stop()
	s.db.Db.Close()
	return nil
}

func newStore(db *gorp.DbMap, tableName string) *store {
	return &store{
		db:        db,
		tableName: tableName,
	}
}

type store struct {
	sync.RWMutex
	ctx       context.Context
	db        *gorp.DbMap
	tableName string
	sid       string
	expired   int64
	values    map[string]interface{}
}

func (s *store) reset(ctx context.Context, sid string, expired int64, values map[string]interface{}) {
	if values == nil {
		values = make(map[string]interface{})
	}
	s.ctx = ctx
	s.sid = sid
	s.expired = expired
	s.values = values
}

func (s *store) Context() context.Context {
	return s.ctx
}

func (s *store) SessionID() string {
	return s.sid
}

func (s *store) Set(key string, value interface{}) {
	s.Lock()
	s.values[key] = value
	s.Unlock()
}

func (s *store) Get(key string) (interface{}, bool) {
	s.RLock()
	val, ok := s.values[key]
	s.RUnlock()
	return val, ok
}

func (s *store) Delete(key string) interface{} {
	s.RLock()
	v, ok := s.values[key]
	s.RUnlock()
	if ok {
		s.Lock()
		delete(s.values, key)
		s.Unlock()
	}
	return v
}

func (s *store) Flush() error {
	s.Lock()
	s.values = make(map[string]interface{})
	s.Unlock()
	return s.Save()
}

func (s *store) Save() error {
	var value string

	s.RLock()
	if len(s.values) > 0 {
		buf, err := jsonMarshal(s.values)
		if err != nil {
			s.RUnlock()
			return err
		}
		value = string(buf)
	}
	s.RUnlock()

	n, err := s.db.SelectInt(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE id=?", s.tableName), s.sid)
	if err != nil {
		return err
	} else if n == 0 {
		return s.db.Insert(&SessionItem{
			ID:        s.sid,
			Value:     value,
			ExpiredAt: time.Now().Add(time.Duration(s.expired) * time.Second).Unix(),
		})
	}

	_, err = s.db.Exec(fmt.Sprintf("UPDATE %s SET value=?,expired_at=? WHERE id=?", s.tableName),
		value,
		time.Now().Add(time.Duration(s.expired)*time.Second).Unix(),
		s.sid)

	return err
}

// SessionItem Data items stored in mysql
type SessionItem struct {
	ID        string `db:"id,primarykey,size:255"`
	Value     string `db:"value,size:2048"`
	ExpiredAt int64  `db:"expired_at"`
}
