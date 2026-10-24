// Package schedulerui serves the Cloud Scheduler UI API. Handlers call the
// transport-neutral Cloud Scheduler core directly (in-process) rather than over
// the wire.
//
// The console is location-optional: the list aggregates every job in the
// project across all locations (no location picker) and shows the location as a
// read-only field. Detail/actions link back with the location carried from the
// list row, so a job id never has to be disambiguated by hand.
package schedulerui

// Job is the console rendering of a Cloud Scheduler job, flattened across
// locations for the list page. Target fields are only populated for the target
// kind in use.
type Job struct {
	Name            string            `json:"name"`
	Location        string            `json:"location"`
	State           string            `json:"state"`
	Description     string            `json:"description,omitempty"`
	Schedule        string            `json:"schedule,omitempty"`
	TimeZone        string            `json:"timeZone,omitempty"`
	Target          string            `json:"target,omitempty"`
	HTTPURI         string            `json:"httpUri,omitempty"`
	HTTPMethod      string            `json:"httpMethod,omitempty"`
	HTTPBody        string            `json:"httpBody,omitempty"`
	HTTPHeaders     map[string]string `json:"httpHeaders,omitempty"`
	PubSubTopic     string            `json:"pubsubTopic,omitempty"`
	PubSubData      string            `json:"pubsubData,omitempty"`
	AppEngineURI    string            `json:"appEngineUri,omitempty"`
	AppEngineMethod string            `json:"appEngineMethod,omitempty"`
	RetryCount      int32             `json:"retryCount,omitempty"`
	AttemptDeadline string            `json:"attemptDeadline,omitempty"`
	ScheduleTime    string            `json:"scheduleTime,omitempty"`
	LastAttemptTime string            `json:"lastAttemptTime,omitempty"`
	UserUpdateTime  string            `json:"userUpdateTime,omitempty"`
	LastStatus      *JobStatus        `json:"lastStatus,omitempty"`
}

// JobStatus is the result of the last delivery attempt.
type JobStatus struct {
	Code    int32  `json:"code"`
	Message string `json:"message,omitempty"`
}

// ListJobsResponse is the response for GET /jobs.
type ListJobsResponse struct {
	Jobs  []Job `json:"jobs"`
	Total int   `json:"total"`
}

// JobInput is the create/update form body. Location is required on create and
// ignored on update (the path carries it).
type JobInput struct {
	Name            string            `json:"name"`
	Location        string            `json:"location"`
	Schedule        string            `json:"schedule"`
	TimeZone        string            `json:"timeZone"`
	Description     string            `json:"description"`
	Target          string            `json:"target"`
	HTTPURI         string            `json:"httpUri"`
	HTTPMethod      string            `json:"httpMethod"`
	HTTPBody        string            `json:"httpBody"`
	HTTPHeaders     map[string]string `json:"httpHeaders"`
	PubSubTopic     string            `json:"pubsubTopic"`
	PubSubData      string            `json:"pubsubData"`
	AppEngineURI    string            `json:"appEngineUri"`
	AppEngineMethod string            `json:"appEngineMethod"`
	RetryCount      int32             `json:"retryCount"`
	AttemptDeadline string            `json:"attemptDeadline"`
}
