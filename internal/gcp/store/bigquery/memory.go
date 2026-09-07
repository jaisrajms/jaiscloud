package bigquery

import (
	"context"
	"sort"
	"strings"
	"sync"
)

// MemoryStore is an in-memory Store.
type MemoryStore struct {
	mu       sync.RWMutex
	datasets map[string]Dataset // projectID+"/"+datasetID → dataset
	tables   map[string]Table   // projectID+"/"+datasetID+"/"+tableID → table
	jobs     map[string]Job     // projectID+"/"+jobID → job
	rows     map[string][]Row   // projectID+"/"+datasetID+"/"+tableID → rows (seq order)
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		datasets: make(map[string]Dataset),
		tables:   make(map[string]Table),
		jobs:     make(map[string]Job),
		rows:     make(map[string][]Row),
	}
}

func datasetScope(projectID, datasetID string) string { return projectID + "/" + datasetID }

func tableScope(projectID, datasetID, tableID string) string {
	return projectID + "/" + datasetID + "/" + tableID
}

func jobScope(projectID, jobID string) string { return projectID + "/" + jobID }

func (s *MemoryStore) CreateDataset(_ context.Context, projectID string, d Dataset) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := datasetScope(projectID, d.DatasetID)
	if _, ok := s.datasets[key]; ok {
		return ErrAlreadyExists
	}
	d.ProjectID = projectID
	s.datasets[key] = d
	return nil
}

func (s *MemoryStore) GetDataset(_ context.Context, projectID, datasetID string) (Dataset, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.datasets[datasetScope(projectID, datasetID)]
	if !ok {
		return Dataset{}, ErrNoSuchDataset
	}
	return d, nil
}

func (s *MemoryStore) UpdateDataset(_ context.Context, projectID string, d Dataset) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := datasetScope(projectID, d.DatasetID)
	if _, ok := s.datasets[key]; !ok {
		return ErrNoSuchDataset
	}
	d.ProjectID = projectID
	s.datasets[key] = d
	return nil
}

func (s *MemoryStore) UpdateDatasetAtomic(_ context.Context, projectID, datasetID string, mutate func(Dataset) (Dataset, error)) (Dataset, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := datasetScope(projectID, datasetID)
	current, ok := s.datasets[key]
	if !ok {
		return Dataset{}, ErrNoSuchDataset
	}
	next, err := mutate(current)
	if err != nil {
		return Dataset{}, err
	}
	next.ProjectID = projectID
	s.datasets[key] = next
	return next, nil
}

func (s *MemoryStore) DeleteDataset(_ context.Context, projectID, datasetID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := datasetScope(projectID, datasetID)
	if _, ok := s.datasets[key]; !ok {
		return ErrNoSuchDataset
	}
	delete(s.datasets, key)
	prefix := key + "/"
	for k := range s.tables {
		if strings.HasPrefix(k, prefix) {
			delete(s.tables, k)
		}
	}
	for k := range s.rows {
		if strings.HasPrefix(k, prefix) {
			delete(s.rows, k)
		}
	}
	return nil
}

func (s *MemoryStore) ListDatasets(_ context.Context, projectID string) ([]Dataset, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := projectID + "/"
	result := make([]Dataset, 0, len(s.datasets))
	for k, d := range s.datasets {
		if strings.HasPrefix(k, prefix) {
			result = append(result, d)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].DatasetID < result[j].DatasetID })
	return result, nil
}

func (s *MemoryStore) CreateTable(_ context.Context, projectID, datasetID string, t Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := tableScope(projectID, datasetID, t.TableID)
	if _, ok := s.tables[key]; ok {
		return ErrAlreadyExists
	}
	t.ProjectID = projectID
	t.DatasetID = datasetID
	s.tables[key] = t
	return nil
}

func (s *MemoryStore) GetTable(_ context.Context, projectID, datasetID, tableID string) (Table, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tables[tableScope(projectID, datasetID, tableID)]
	if !ok {
		return Table{}, ErrNoSuchTable
	}
	return t, nil
}

func (s *MemoryStore) UpdateTable(_ context.Context, projectID, datasetID string, t Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := tableScope(projectID, datasetID, t.TableID)
	if _, ok := s.tables[key]; !ok {
		return ErrNoSuchTable
	}
	t.ProjectID = projectID
	t.DatasetID = datasetID
	s.tables[key] = t
	return nil
}

func (s *MemoryStore) UpdateTableAtomic(_ context.Context, projectID, datasetID, tableID string, mutate func(Table) (Table, error)) (Table, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := tableScope(projectID, datasetID, tableID)
	current, ok := s.tables[key]
	if !ok {
		return Table{}, ErrNoSuchTable
	}
	next, err := mutate(current)
	if err != nil {
		return Table{}, err
	}
	next.ProjectID = projectID
	next.DatasetID = datasetID
	s.tables[key] = next
	return next, nil
}

func (s *MemoryStore) DeleteTable(_ context.Context, projectID, datasetID, tableID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := tableScope(projectID, datasetID, tableID)
	if _, ok := s.tables[key]; !ok {
		return ErrNoSuchTable
	}
	delete(s.tables, key)
	delete(s.rows, key)
	return nil
}

func (s *MemoryStore) ListTables(_ context.Context, projectID, datasetID string) ([]Table, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := projectID + "/" + datasetID + "/"
	result := make([]Table, 0, len(s.tables))
	for k, t := range s.tables {
		if strings.HasPrefix(k, prefix) {
			result = append(result, t)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].TableID < result[j].TableID })
	return result, nil
}

func (s *MemoryStore) CreateJob(_ context.Context, projectID string, j Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := jobScope(projectID, j.JobID)
	if _, ok := s.jobs[key]; ok {
		return ErrAlreadyExists
	}
	j.ProjectID = projectID
	s.jobs[key] = j
	return nil
}

func (s *MemoryStore) GetJob(_ context.Context, projectID, jobID string) (Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[jobScope(projectID, jobID)]
	if !ok {
		return Job{}, ErrNoSuchJob
	}
	return j, nil
}

func (s *MemoryStore) DeleteJob(_ context.Context, projectID, jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := jobScope(projectID, jobID)
	if _, ok := s.jobs[key]; !ok {
		return ErrNoSuchJob
	}
	delete(s.jobs, key)
	return nil
}

func (s *MemoryStore) ListJobs(_ context.Context, projectID string) ([]Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := projectID + "/"
	result := make([]Job, 0, len(s.jobs))
	for k, j := range s.jobs {
		if strings.HasPrefix(k, prefix) {
			result = append(result, j)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].JobID < result[j].JobID })
	return result, nil
}

func (s *MemoryStore) InsertRows(_ context.Context, projectID, datasetID, tableID string, rows []Row) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := tableScope(projectID, datasetID, tableID)
	t, ok := s.tables[key]
	if !ok {
		return ErrNoSuchTable
	}
	existing := s.rows[key]
	var next int64
	for _, r := range existing {
		if r.Seq > next {
			next = r.Seq
		}
	}
	for i := range rows {
		next++
		rows[i].ProjectID = projectID
		rows[i].DatasetID = datasetID
		rows[i].TableID = tableID
		rows[i].Seq = next
		existing = append(existing, rows[i])
	}
	s.rows[key] = existing
	t.NumRows += int64(len(rows))
	s.tables[key] = t
	return nil
}

func (s *MemoryStore) ListRows(_ context.Context, projectID, datasetID, tableID string) ([]Row, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows := s.rows[tableScope(projectID, datasetID, tableID)]
	result := make([]Row, len(rows))
	copy(result, rows)
	return result, nil
}

func (s *MemoryStore) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.datasets = make(map[string]Dataset)
	s.tables = make(map[string]Table)
	s.jobs = make(map[string]Job)
	s.rows = make(map[string][]Row)
}
