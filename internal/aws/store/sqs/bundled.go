package sqs

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"time"

	"jaiscloud/internal/aws/store/bundle"
)

// BundledSQSStore wraps LocalBundle[MemoryMessageStore] to provide per-(account,region)
// isolation. It implements SQSMessageStore and is used in memory mode.
type BundledSQSStore struct {
	b *bundle.LocalBundle[MemoryMessageStore]
}

func NewBundledSQSStore() *BundledSQSStore {
	return &BundledSQSStore{
		b: bundle.NewLocal(func(_, _ string) *MemoryMessageStore {
			return NewMemoryMessageStore()
		}),
	}
}

func (s *BundledSQSStore) inner(account, region string) (*MemoryMessageStore, error) {
	return s.b.Get(account, region)
}

func (s *BundledSQSStore) Send(ctx context.Context, account, region string, msg SQSMessage) (dedupMessageID, sequenceNumber string, err error) {
	st, err := s.inner(account, region)
	if err != nil {
		return "", "", err
	}
	return st.Send(ctx, account, region, msg)
}

func (s *BundledSQSStore) Receive(ctx context.Context, account, region, queueURL string, maxMessages int, now time.Time) ([]SQSMessage, error) {
	st, err := s.inner(account, region)
	if err != nil {
		return nil, err
	}
	return st.Receive(ctx, account, region, queueURL, maxMessages, now)
}

func (s *BundledSQSStore) Delete(ctx context.Context, account, region, queueURL, receiptHandle string) error {
	st, err := s.inner(account, region)
	if err != nil {
		return err
	}
	return st.Delete(ctx, account, region, queueURL, receiptHandle)
}

func (s *BundledSQSStore) ChangeVisibility(ctx context.Context, account, region, queueURL, receiptHandle string, timeoutSec int, now time.Time) error {
	st, err := s.inner(account, region)
	if err != nil {
		return err
	}
	return st.ChangeVisibility(ctx, account, region, queueURL, receiptHandle, timeoutSec, now)
}

func (s *BundledSQSStore) Purge(ctx context.Context, account, region, queueURL string) error {
	st, err := s.inner(account, region)
	if err != nil {
		return err
	}
	return st.Purge(ctx, account, region, queueURL)
}

func (s *BundledSQSStore) GetApproximateCounts(ctx context.Context, account, region, queueURL string, now time.Time) (visible, notVisible, delayed int, err error) {
	st, e := s.inner(account, region)
	if e != nil {
		return 0, 0, 0, e
	}
	return st.GetApproximateCounts(ctx, account, region, queueURL, now)
}

func (s *BundledSQSStore) Peek(ctx context.Context, account, region, queueURL string, offset, limit int) ([]SQSMessage, int, error) {
	st, err := s.inner(account, region)
	if err != nil {
		return nil, 0, err
	}
	return st.Peek(ctx, account, region, queueURL, offset, limit)
}

func (s *BundledSQSStore) SetQueueRetention(ctx context.Context, account, region, queueURL string, retentionSecs int) error {
	st, err := s.inner(account, region)
	if err != nil {
		return err
	}
	return st.SetQueueRetention(ctx, account, region, queueURL, retentionSecs)
}

func (s *BundledSQSStore) Reset(ctx context.Context)         { s.b.Reset(ctx) }
func (s *BundledSQSStore) ResetScope(account, region string) { s.b.ResetScope(account, region) }
func (s *BundledSQSStore) ResetAccount(account string)       { s.b.ResetAccount(account) }

// ─── Snapshotter ──────────────────────────────────────────────────────────────

type bundledSQSScopeSnap struct {
	Account string          `json:"account"`
	Region  string          `json:"region"`
	Data    json.RawMessage `json:"data"`
}

func (s *BundledSQSStore) IsEmpty(_ context.Context) (bool, error) {
	empty := true
	s.b.Iter(func(_, _ string, st *MemoryMessageStore) {
		if !empty {
			return
		}
		ok, _ := st.IsEmpty(context.Background())
		if !ok {
			empty = false
		}
	})
	return empty, nil
}

func (s *BundledSQSStore) Snapshot(_ context.Context, w io.Writer) error {
	scopes := make([]bundledSQSScopeSnap, 0)
	var iterErr error
	s.b.Iter(func(account, region string, st *MemoryMessageStore) {
		if iterErr != nil {
			return
		}
		var buf bytes.Buffer
		if err := st.Snapshot(context.Background(), &buf); err != nil {
			iterErr = err
			return
		}
		scopes = append(scopes, bundledSQSScopeSnap{
			Account: account,
			Region:  region,
			Data:    json.RawMessage(buf.Bytes()),
		})
	})
	if iterErr != nil {
		return iterErr
	}
	return json.NewEncoder(w).Encode(map[string]any{"kind": "local", "scopes": scopes})
}

func (s *BundledSQSStore) Restore(_ context.Context, r io.Reader) error {
	var envelope struct {
		Scopes []bundledSQSScopeSnap `json:"scopes"`
	}
	if err := json.NewDecoder(r).Decode(&envelope); err != nil {
		return err
	}
	s.b.ResetAndDiscard()
	for _, scope := range envelope.Scopes {
		st, err := s.b.Get(scope.Account, scope.Region)
		if err != nil {
			return err
		}
		if err := st.Restore(context.Background(), bytes.NewReader(scope.Data)); err != nil {
			return err
		}
	}
	return nil
}
