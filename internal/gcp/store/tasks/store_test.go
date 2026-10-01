package tasks

import (
	"bytes"
	"context"
	"testing"
)

func TestMemoryQueueRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	q := Queue{Name: "q1", RateLimits: &RateLimits{MaxDispatchesPerSecond: 10}}
	if err := s.CreateQueue(ctx, "p", "l", q); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if err := s.CreateQueue(ctx, "p", "l", q); err != ErrAlreadyExists {
		t.Fatalf("duplicate CreateQueue err = %v", err)
	}
	got, err := s.GetQueue(ctx, "p", "l", "q1")
	if err != nil {
		t.Fatalf("GetQueue: %v", err)
	}
	if got.ProjectID != "p" || got.Location != "l" || got.RateLimits.MaxDispatchesPerSecond != 10 {
		t.Fatalf("got = %+v", got)
	}
	if _, err := s.GetQueue(ctx, "p", "l", "nope"); err != ErrNoSuchQueue {
		t.Fatalf("missing GetQueue err = %v", err)
	}
	qs, err := s.ListQueues(ctx, "p", "l")
	if err != nil || len(qs) != 1 {
		t.Fatalf("ListQueues = %v, %v", qs, err)
	}
}

func TestMemoryTaskRoundTripAndPurge(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.CreateQueue(ctx, "p", "l", Queue{Name: "q1"})
	tk := Task{Name: "t1", Target: TargetHTTP, HTTP: &HttpRequest{URL: "http://x"}}
	if err := s.CreateTask(ctx, "p", "l", "q1", tk); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := s.CreateTask(ctx, "p", "l", "q1", tk); err != ErrAlreadyExists {
		t.Fatalf("duplicate CreateTask err = %v", err)
	}
	got, err := s.GetTask(ctx, "p", "l", "q1", "t1")
	if err != nil || got.HTTP.URL != "http://x" {
		t.Fatalf("GetTask = %+v, %v", got, err)
	}
	if _, err := s.GetTask(ctx, "p", "l", "q1", "nope"); err != ErrNoSuchTask {
		t.Fatalf("missing GetTask err = %v", err)
	}
	if _, err := s.UpdateTaskAtomic(ctx, "p", "l", "q1", "t1", func(cur Task) (Task, error) {
		cur.DispatchCount++
		return cur, nil
	}); err != nil {
		t.Fatalf("UpdateTaskAtomic: %v", err)
	}
	got, _ = s.GetTask(ctx, "p", "l", "q1", "t1")
	if got.DispatchCount != 1 {
		t.Fatalf("DispatchCount = %d", got.DispatchCount)
	}
	tasks, err := s.ListTasks(ctx, "p", "l", "q1")
	if err != nil || len(tasks) != 1 {
		t.Fatalf("ListTasks = %v, %v", tasks, err)
	}
	if n, err := s.DeleteTasks(ctx, "p", "l", "q1"); err != nil || n != 1 {
		t.Fatalf("DeleteTasks = %d, %v", n, err)
	}
	if _, err := s.GetTask(ctx, "p", "l", "q1", "t1"); err != ErrNoSuchTask {
		t.Fatalf("after purge GetTask err = %v", err)
	}
}

func TestMemoryListAllQueues(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.CreateQueue(ctx, "p1", "l1", Queue{Name: "b"})
	_ = s.CreateQueue(ctx, "p1", "l1", Queue{Name: "a"})
	_ = s.CreateQueue(ctx, "p2", "l2", Queue{Name: "c"})
	all, err := s.ListAllQueues(ctx)
	if err != nil {
		t.Fatalf("ListAllQueues: %v", err)
	}
	got := make([]string, 0, len(all))
	for _, q := range all {
		got = append(got, q.ProjectID+"/"+q.Location+"/"+q.Name)
	}
	want := []string{"p1/l1/a", "p1/l1/b", "p2/l2/c"}
	if len(got) != len(want) {
		t.Fatalf("ListAllQueues = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ListAllQueues = %v, want %v", got, want)
		}
	}
}

func TestMemoryDeleteQueueCascadesTasks(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.CreateQueue(ctx, "p", "l", Queue{Name: "q1"})
	_ = s.CreateTask(ctx, "p", "l", "q1", Task{Name: "t1"})
	if err := s.DeleteQueue(ctx, "p", "l", "q1"); err != nil {
		t.Fatalf("DeleteQueue: %v", err)
	}
	if _, err := s.GetTask(ctx, "p", "l", "q1", "t1"); err != ErrNoSuchTask {
		t.Fatalf("task survived queue delete: %v", err)
	}
}

func TestMemorySnapshotRestore(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.CreateQueue(ctx, "p", "l", Queue{Name: "q1"})
	_ = s.CreateTask(ctx, "p", "l", "q1", Task{Name: "t1", Target: TargetHTTP})
	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	dst := NewMemoryStore()
	if err := dst.Restore(ctx, &buf); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if _, err := dst.GetTask(ctx, "p", "l", "q1", "t1"); err != nil {
		t.Fatalf("restored task: %v", err)
	}
}

func TestMemoryAtomicQueueUpdateConcurrent(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.CreateQueue(ctx, "p", "l", Queue{Name: "q1", RateLimits: &RateLimits{MaxDispatchesPerSecond: 1}})
	done := make(chan struct{})
	for i := 0; i < 20; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			_, _ = s.UpdateQueueAtomic(ctx, "p", "l", "q1", func(cur Queue) (Queue, error) {
				cur.RateLimits.MaxDispatchesPerSecond++
				return cur, nil
			})
		}()
	}
	for i := 0; i < 20; i++ {
		<-done
	}
	got, _ := s.GetQueue(ctx, "p", "l", "q1")
	if got.RateLimits.MaxDispatchesPerSecond != 21 {
		t.Fatalf("lost updates: got %v", got.RateLimits.MaxDispatchesPerSecond)
	}
}
