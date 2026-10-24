// Package tasksui serves the Cloud Tasks UI API. Handlers call the
// transport-neutral Cloud Tasks core directly (in-process) rather than over the
// wire.
//
// The console is location-optional: the queue list aggregates every queue in the
// project across all locations (no location picker) and shows the location as a
// read-only field. Detail/actions link back with the location carried from the
// list row, so a queue id never has to be disambiguated by hand.
package tasksui

// Queue is the console rendering of a Cloud Tasks queue, flattened across
// locations for the list page. Rate-limit and retry fields are the queue's
// effective configuration.
type Queue struct {
	Name                    string  `json:"name"`
	Location                string  `json:"location"`
	State                   string  `json:"state"`
	PurgeTime               string  `json:"purgeTime,omitempty"`
	MaxDispatchesPerSecond  float64 `json:"maxDispatchesPerSecond,omitempty"`
	MaxBurstSize            int32   `json:"maxBurstSize,omitempty"`
	MaxConcurrentDispatches int32   `json:"maxConcurrentDispatches,omitempty"`
	MaxAttempts             int32   `json:"maxAttempts,omitempty"`
	MaxRetryDuration        string  `json:"maxRetryDuration,omitempty"`
	MinBackoff              string  `json:"minBackoff,omitempty"`
	MaxBackoff              string  `json:"maxBackoff,omitempty"`
	MaxDoublings            int32   `json:"maxDoublings,omitempty"`
}

// ListQueuesResponse is the response for GET /queues.
type ListQueuesResponse struct {
	Queues []Queue `json:"queues"`
	Total  int     `json:"total"`
}

// QueueInput is the create/update form body. Location is required on create and
// ignored on update (the path carries it).
type QueueInput struct {
	Name                    string  `json:"name"`
	Location                string  `json:"location"`
	MaxDispatchesPerSecond  float64 `json:"maxDispatchesPerSecond"`
	MaxBurstSize            int32   `json:"maxBurstSize"`
	MaxConcurrentDispatches int32   `json:"maxConcurrentDispatches"`
	MaxAttempts             int32   `json:"maxAttempts"`
	MaxRetryDuration        string  `json:"maxRetryDuration"`
	MinBackoff              string  `json:"minBackoff"`
	MaxBackoff              string  `json:"maxBackoff"`
	MaxDoublings            int32   `json:"maxDoublings"`
}

// TaskStatus is the result of the last delivery attempt.
type TaskStatus struct {
	Code    int32  `json:"code"`
	Message string `json:"message,omitempty"`
}

// Task is the console rendering of a Cloud Tasks task. Target fields are only
// populated for the target kind in use.
type Task struct {
	Name              string            `json:"name"`
	Location          string            `json:"location"`
	Queue             string            `json:"queue"`
	Target            string            `json:"target,omitempty"`
	HTTPURL           string            `json:"httpUrl,omitempty"`
	HTTPMethod        string            `json:"httpMethod,omitempty"`
	HTTPHeaders       map[string]string `json:"httpHeaders,omitempty"`
	HTTPBody          string            `json:"httpBody,omitempty"`
	AppEngineURI      string            `json:"appEngineUri,omitempty"`
	AppEngineMethod   string            `json:"appEngineMethod,omitempty"`
	ScheduleTime      string            `json:"scheduleTime,omitempty"`
	CreateTime        string            `json:"createTime,omitempty"`
	DispatchDeadline  string            `json:"dispatchDeadline,omitempty"`
	DispatchCount     int32             `json:"dispatchCount"`
	ResponseCount     int32             `json:"responseCount"`
	LastAttemptStatus *TaskStatus       `json:"lastAttemptStatus,omitempty"`
}

// ListTasksResponse is the response for GET /queues/{location}/{queue}/tasks.
type ListTasksResponse struct {
	Tasks []Task `json:"tasks"`
	Total int    `json:"total"`
}

// TaskInput is the create-task form body.
type TaskInput struct {
	Name             string            `json:"name"`
	Target           string            `json:"target"`
	HTTPURL          string            `json:"httpUrl"`
	HTTPMethod       string            `json:"httpMethod"`
	HTTPHeaders      map[string]string `json:"httpHeaders"`
	HTTPBody         string            `json:"httpBody"`
	AppEngineURI     string            `json:"appEngineUri"`
	AppEngineMethod  string            `json:"appEngineMethod"`
	ScheduleTime     string            `json:"scheduleTime"`
	DispatchDeadline string            `json:"dispatchDeadline"`
}
