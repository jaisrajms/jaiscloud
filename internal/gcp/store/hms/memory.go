package hms

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"

	"jaiscloud/internal/gcp/storeutil"
)

// MemoryStore is an in-memory Store.
type MemoryStore struct {
	mu         sync.RWMutex
	databases  map[string]Database
	tables     map[string]map[string]Table                // db -> name -> table
	partitions map[string]map[string]map[string]Partition // db -> table -> partKey -> partition
	locks      map[int64]lockRecord
	lockSeq    atomic.Int64
}

type lockRecord struct {
	state LockState
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		databases:  make(map[string]Database),
		tables:     make(map[string]map[string]Table),
		partitions: make(map[string]map[string]map[string]Partition),
		locks:      make(map[int64]lockRecord),
	}
}

// --- Databases ---

func (s *MemoryStore) CreateDatabase(_ context.Context, db Database) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.databases[db.Name]; ok {
		return ErrDatabaseExists
	}
	if db.Parameters == nil {
		db.Parameters = map[string]string{}
	}
	s.databases[db.Name] = db
	return nil
}

func (s *MemoryStore) GetDatabase(_ context.Context, name string) (Database, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	db, ok := s.databases[name]
	if !ok {
		return Database{}, ErrDatabaseNotFound
	}
	return db, nil
}

func (s *MemoryStore) ListDatabases(_ context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.databases))
	for name := range s.databases {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func (s *MemoryStore) DropDatabase(_ context.Context, name string, cascade bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.databases[name]; !ok {
		return ErrDatabaseNotFound
	}
	if !cascade && len(s.tables[name]) > 0 {
		return ErrDatabaseNotEmpty
	}
	delete(s.partitions, name)
	delete(s.tables, name)
	delete(s.databases, name)
	return nil
}

func (s *MemoryStore) AlterDatabase(_ context.Context, name string, db Database) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.databases[name]; !ok {
		return ErrDatabaseNotFound
	}
	if db.Parameters == nil {
		db.Parameters = map[string]string{}
	}
	db.Name = name
	s.databases[name] = db
	return nil
}

// --- Tables ---

func (s *MemoryStore) CreateTable(_ context.Context, dbName, tableName string, t Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.databases[dbName]; !ok {
		return ErrDatabaseNotFound
	}
	if s.tables[dbName] == nil {
		s.tables[dbName] = make(map[string]Table)
	}
	if _, ok := s.tables[dbName][tableName]; ok {
		return ErrTableExists
	}
	t.DBName = dbName
	t.TableName = tableName
	s.tables[dbName][tableName] = t
	return nil
}

func (s *MemoryStore) GetTable(_ context.Context, dbName, tableName string) (Table, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tables[dbName][tableName]
	if !ok {
		return Table{}, ErrTableNotFound
	}
	return t, nil
}

func (s *MemoryStore) ListTables(_ context.Context, dbName string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := s.tables[dbName]
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func (s *MemoryStore) DropTable(_ context.Context, dbName, tableName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tables[dbName][tableName]; !ok {
		return ErrTableNotFound
	}
	delete(s.tables[dbName], tableName)
	if tp := s.partitions[dbName]; tp != nil {
		delete(tp, tableName)
		if len(tp) == 0 {
			delete(s.partitions, dbName)
		}
	}
	return nil
}

// --- Partitions ---

func (s *MemoryStore) CreatePartition(_ context.Context, dbName, tableName string, p Partition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tables[dbName][tableName]; !ok {
		return ErrTableNotFound
	}
	if s.partitions[dbName] == nil {
		s.partitions[dbName] = make(map[string]map[string]Partition)
	}
	if s.partitions[dbName][tableName] == nil {
		s.partitions[dbName][tableName] = make(map[string]Partition)
	}
	key := PartitionKey(p.Values)
	if _, ok := s.partitions[dbName][tableName][key]; ok {
		return ErrPartitionExists
	}
	p.DBName = dbName
	p.TableName = tableName
	if p.Values == nil {
		p.Values = []string{}
	}
	s.partitions[dbName][tableName][key] = p
	return nil
}

func (s *MemoryStore) GetPartition(_ context.Context, dbName, tableName string, values []string) (Partition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.tables[dbName][tableName]; !ok {
		return Partition{}, ErrTableNotFound
	}
	p, ok := s.partitions[dbName][tableName][PartitionKey(values)]
	if !ok {
		return Partition{}, ErrPartitionNotFound
	}
	return p, nil
}

func (s *MemoryStore) ListPartitions(_ context.Context, dbName, tableName string) ([]Partition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.tables[dbName][tableName]; !ok {
		return nil, ErrTableNotFound
	}
	m := s.partitions[dbName][tableName]
	out := make([]Partition, 0, len(m))
	for _, p := range m {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return lessValues(out[i].Values, out[j].Values) })
	return out, nil
}

func (s *MemoryStore) AlterPartition(_ context.Context, dbName, tableName string, values []string, mutate func(Partition) (Partition, error)) (Partition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tables[dbName][tableName]; !ok {
		return Partition{}, ErrTableNotFound
	}
	key := PartitionKey(values)
	tbl := s.partitions[dbName][tableName]
	cur, ok := tbl[key]
	if !ok {
		return Partition{}, ErrPartitionNotFound
	}
	next, err := mutate(cur)
	if err != nil {
		return Partition{}, err
	}
	next.DBName = dbName
	next.TableName = tableName
	next.Values = cur.Values
	tbl[key] = next
	return next, nil
}

func (s *MemoryStore) DropPartition(_ context.Context, dbName, tableName string, values []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := PartitionKey(values)
	tbl := s.partitions[dbName][tableName]
	if _, ok := tbl[key]; !ok {
		return ErrPartitionNotFound
	}
	delete(tbl, key)
	return nil
}

func (s *MemoryStore) RenamePartition(_ context.Context, dbName, tableName string, oldValues, newValues []string, p Partition) (Partition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tbl := s.partitions[dbName][tableName]
	oldKey := PartitionKey(oldValues)
	if _, ok := tbl[oldKey]; !ok {
		return Partition{}, ErrPartitionNotFound
	}
	newKey := PartitionKey(newValues)
	if _, ok := tbl[newKey]; ok {
		return Partition{}, ErrPartitionExists
	}
	delete(tbl, oldKey)
	p.DBName = dbName
	p.TableName = tableName
	p.Values = newValues
	if p.Values == nil {
		p.Values = []string{}
	}
	tbl[newKey] = p
	return p, nil
}

// lessValues orders value tuples elementwise, shorter tuples first on a tie,
// so ListPartitions is deterministic across restarts.
func lessValues(a, b []string) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return len(a) < len(b)
}

func (s *MemoryStore) AlterTable(_ context.Context, dbName, tableName string, mutate func(Table) (Table, error)) (Table, error) {
	return storeutil.AtomicUpdate(&s.mu,
		func() (Table, bool) { t, ok := s.tables[dbName][tableName]; return t, ok },
		func(current Table, exists bool) (Table, error) {
			if !exists {
				return Table{}, ErrTableNotFound
			}
			return mutate(current)
		},
		func(next Table) {
			next.DBName = dbName
			next.TableName = tableName
			s.tables[dbName][tableName] = next
		},
	)
}

func (s *MemoryStore) RenameTable(_ context.Context, srcDB, srcName, dstDB, dstName string, t Table) (Table, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tables[srcDB][srcName]; !ok {
		return Table{}, ErrTableNotFound
	}
	if _, ok := s.databases[dstDB]; !ok {
		return Table{}, ErrDatabaseNotFound
	}
	if _, exists := s.tables[dstDB][dstName]; exists {
		return Table{}, ErrTableExists
	}
	delete(s.tables[srcDB], srcName)
	if s.tables[dstDB] == nil {
		s.tables[dstDB] = make(map[string]Table)
	}
	t.DBName = dstDB
	t.TableName = dstName
	s.tables[dstDB][dstName] = t

	// Hive preserves partition metadata across a table rename, so re-key the
	// source table's partitions onto the destination table.
	if tp := s.partitions[srcDB][srcName]; tp != nil {
		delete(s.partitions[srcDB], srcName)
		if len(s.partitions[srcDB]) == 0 {
			delete(s.partitions, srcDB)
		}
		if s.partitions[dstDB] == nil {
			s.partitions[dstDB] = make(map[string]map[string]Partition)
		}
		if s.partitions[dstDB][dstName] == nil {
			s.partitions[dstDB][dstName] = make(map[string]Partition)
		}
		for k, p := range tp {
			p.DBName = dstDB
			p.TableName = dstName
			s.partitions[dstDB][dstName][k] = p
		}
	}
	return t, nil
}

// --- Locks ---

func (s *MemoryStore) Lock(_ context.Context, _ Lock) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.lockSeq.Add(1)
	s.locks[id] = lockRecord{state: LockStateAcquired}
	return id, nil
}

func (s *MemoryStore) CheckLock(_ context.Context, id int64) (LockState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.locks[id]
	if !ok {
		return LockStateNotAcquired, nil
	}
	return rec.state, nil
}

func (s *MemoryStore) Unlock(_ context.Context, id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.locks, id)
	return nil
}

func (s *MemoryStore) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.databases = make(map[string]Database)
	s.tables = make(map[string]map[string]Table)
	s.partitions = make(map[string]map[string]map[string]Partition)
	s.locks = make(map[int64]lockRecord)
	s.lockSeq.Store(0)
}
