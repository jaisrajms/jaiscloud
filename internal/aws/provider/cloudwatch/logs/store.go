package logs

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"time"
)

// LogQuery represents a CloudWatch Logs Insights query execution.
type LogQuery struct {
	QueryID       string
	QueryString   string
	LogGroupNames []string
	StartTime     int64
	EndTime       int64
	Status        string // "Scheduled","Running","Complete","Failed","Cancelled","Timeout","Unknown"
	CreatedAt     time.Time
	Results       [][]map[string]string
	Statistics    map[string]float64
}

// QueryDefinition is a saved query definition for reuse.
type QueryDefinition struct {
	QueryDefinitionID string
	Name              string
	QueryString       string
	LogGroupNames     []string
	LastModified      time.Time
}

// ExportTask represents a CWL export to S3 (stub — no actual export).
type ExportTask struct {
	TaskID            string
	TaskName          string
	LogGroupName      string
	From              int64
	To                int64
	Destination       string
	DestinationPrefix string
	StatusCode        string // "CANCELLED","COMPLETED","FAILED","PENDING","PENDING_CANCEL","RUNNING"
	StatusMessage     string
	CreationTime      int64
	CompletionTime    int64
}

// LogGroup holds metadata for a CloudWatch Logs log group.
type LogGroup struct {
	LogGroupName    string
	CreationTime    int64
	RetentionInDays *int
	Arn             string
	StoredBytes     int64
}

// LogStream holds metadata for a CloudWatch Logs log stream.
type LogStream struct {
	LogStreamName       string
	CreationTime        int64
	FirstEventTimestamp int64
	LastEventTimestamp  int64
	LastIngestionTime   int64
	UploadSequenceToken string
	StoredBytes         int64
	Arn                 string
}

// LogEvent is a single log entry stored in a stream.
type LogEvent struct {
	Timestamp     int64
	Message       string
	IngestionTime int64
}

const maxEventsPerStream = 10_000

// eventRing is a bounded ring buffer of LogEvents.
type eventRing struct {
	buf  []LogEvent
	head int // index of the oldest event
	size int // number of events currently stored
	cap  int
}

func newEventRing() *eventRing {
	return &eventRing{
		buf: make([]LogEvent, maxEventsPerStream),
		cap: maxEventsPerStream,
	}
}

// Append adds events to the ring, overwriting the oldest if full.
func (r *eventRing) Append(events []LogEvent) {
	for _, e := range events {
		if r.size < r.cap {
			// Not yet full — write at head+size
			idx := (r.head + r.size) % r.cap
			r.buf[idx] = e
			r.size++
		} else {
			// Full — overwrite oldest (head) and advance head
			r.buf[r.head] = e
			r.head = (r.head + 1) % r.cap
		}
	}
}

// Slice returns all stored events in insertion order (oldest first).
func (r *eventRing) Slice() []LogEvent {
	if r.size == 0 {
		return nil
	}
	out := make([]LogEvent, r.size)
	for i := 0; i < r.size; i++ {
		out[i] = r.buf[(r.head+i)%r.cap]
	}
	return out
}

// SubscriptionFilter holds a CloudWatch Logs subscription filter.
type SubscriptionFilter struct {
	LogGroupName   string `json:"logGroupName"`
	FilterName     string `json:"filterName"`
	FilterPattern  string `json:"filterPattern"`
	DestinationArn string `json:"destinationArn"`
	Distribution   string `json:"distribution"`
	CreationTime   int64  `json:"creationTime"`
}

// memStore is the in-memory backing store for CloudWatch Logs.
type memStore struct {
	mu sync.RWMutex

	// groups maps groupName → *LogGroup
	groups map[string]*LogGroup
	// streams maps groupName → streamName → *LogStream
	streams map[string]map[string]*LogStream
	// events maps groupName → streamName → *eventRing
	events map[string]map[string]*eventRing
	// tags maps groupArn → map[string]string
	tags map[string]map[string]string
	// seqToken maps groupName → streamName → counter
	seqToken map[string]map[string]int64
	// subscriptionFilters maps groupName → filterName → *SubscriptionFilter
	subscriptionFilters map[string]map[string]*SubscriptionFilter
	// queries maps queryID → *LogQuery
	queries map[string]*LogQuery
	// queryDefinitions maps queryDefinitionID → *QueryDefinition
	queryDefinitions map[string]*QueryDefinition
	// exportTasks maps taskID → *ExportTask
	exportTasks map[string]*ExportTask
	// metricFilters maps logGroupName → filterName → *MetricFilter
	metricFilters map[string]map[string]*MetricFilter
}

// MetricFilter holds a CloudWatch Logs metric filter.
type MetricFilter struct {
	FilterName            string           `json:"filterName"`
	LogGroupName          string           `json:"logGroupName"`
	FilterPattern         string           `json:"filterPattern"`
	MetricTransformations []map[string]any `json:"metricTransformations"`
}

func newMemStore() *memStore {
	s := &memStore{}
	s.reset()
	return s
}

func (s *memStore) reset() {
	s.groups = make(map[string]*LogGroup)
	s.streams = make(map[string]map[string]*LogStream)
	s.events = make(map[string]map[string]*eventRing)
	s.tags = make(map[string]map[string]string)
	s.seqToken = make(map[string]map[string]int64)
	s.subscriptionFilters = make(map[string]map[string]*SubscriptionFilter)
	s.queries = make(map[string]*LogQuery)
	s.queryDefinitions = make(map[string]*QueryDefinition)
	s.exportTasks = make(map[string]*ExportTask)
	s.metricFilters = make(map[string]map[string]*MetricFilter)
}

// Reset wipes all state (called on POST /_jaiscloud/reset).
func (s *memStore) Reset(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reset()
}

// cwlSnapshot is the JSON payload written/read by Snapshot/Restore.
type cwlSnapshot struct {
	Groups              map[string]*LogGroup                      `json:"groups"`
	Streams             map[string]map[string]*LogStream          `json:"streams"`
	Events              map[string]map[string][]LogEvent          `json:"events"`
	Tags                map[string]map[string]string              `json:"tags"`
	SeqToken            map[string]map[string]int64               `json:"seq_token"`
	SubscriptionFilters map[string]map[string]*SubscriptionFilter `json:"subscription_filters"`
	MetricFilters       map[string]map[string]*MetricFilter       `json:"metric_filters"`
	QueryDefinitions    map[string]*QueryDefinition               `json:"query_definitions"`
}

// IsEmpty returns true when no log groups have been created.
func (s *memStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.groups) == 0, nil
}

// Snapshot serialises all persistent CWL state to w as JSON.
func (s *memStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Flatten eventRings into plain slices so they are JSON-serialisable.
	eventsFlat := make(map[string]map[string][]LogEvent, len(s.events))
	for grp, streams := range s.events {
		eventsFlat[grp] = make(map[string][]LogEvent, len(streams))
		for stream, ring := range streams {
			eventsFlat[grp][stream] = ring.Slice()
		}
	}

	snap := cwlSnapshot{
		Groups:              s.groups,
		Streams:             s.streams,
		Events:              eventsFlat,
		Tags:                s.tags,
		SeqToken:            s.seqToken,
		SubscriptionFilters: s.subscriptionFilters,
		MetricFilters:       s.metricFilters,
		QueryDefinitions:    s.queryDefinitions,
	}
	return json.NewEncoder(w).Encode(snap)
}

// Restore replaces the store's contents atomically from JSON produced by Snapshot.
func (s *memStore) Restore(_ context.Context, r io.Reader) error {
	var snap cwlSnapshot
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.reset()

	if snap.Groups != nil {
		s.groups = snap.Groups
	}
	if snap.Streams != nil {
		s.streams = snap.Streams
	}
	// Rebuild eventRings from flat slices.
	for grp, streams := range snap.Events {
		if s.events[grp] == nil {
			s.events[grp] = make(map[string]*eventRing)
		}
		for stream, evs := range streams {
			ring := newEventRing()
			ring.Append(evs)
			s.events[grp][stream] = ring
		}
	}
	if snap.Tags != nil {
		s.tags = snap.Tags
	}
	if snap.SeqToken != nil {
		s.seqToken = snap.SeqToken
	}
	if snap.SubscriptionFilters != nil {
		s.subscriptionFilters = snap.SubscriptionFilters
	}
	if snap.MetricFilters != nil {
		s.metricFilters = snap.MetricFilters
	}
	if snap.QueryDefinitions != nil {
		s.queryDefinitions = snap.QueryDefinitions
	}
	return nil
}
