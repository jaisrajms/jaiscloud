package glueui

// Database is a Glue Data Catalog database.
type Database struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	LocationURI string `json:"locationUri,omitempty"`
}

// ListDatabasesResponse is returned by GET /databases.
type ListDatabasesResponse struct {
	Items     []Database `json:"items"`
	Total     int        `json:"total"`
	NextToken string     `json:"nextToken,omitempty"`
}

// Table is a Glue Data Catalog table.
type Table struct {
	Name         string `json:"name"`
	DatabaseName string `json:"databaseName"`
	Description  string `json:"description,omitempty"`
	StorageType  string `json:"storageType,omitempty"`
	Location     string `json:"location,omitempty"`
}

// ListTablesResponse is returned by GET /databases/{db}/tables.
type ListTablesResponse struct {
	Items     []Table `json:"items"`
	Total     int     `json:"total"`
	NextToken string  `json:"nextToken,omitempty"`
}

// Job is a Glue ETL job.
type Job struct {
	Name    string `json:"name"`
	Role    string `json:"role,omitempty"`
	Command string `json:"command,omitempty"`
}

// ListJobsResponse is returned by GET /jobs.
type ListJobsResponse struct {
	Items     []Job  `json:"items"`
	Total     int    `json:"total"`
	NextToken string `json:"nextToken,omitempty"`
}

// JobRun is a single Glue job run.
type JobRun struct {
	ID          string `json:"id"`
	JobName     string `json:"jobName"`
	State       string `json:"state"`
	StartedOn   string `json:"startedOn,omitempty"`
	CompletedOn string `json:"completedOn,omitempty"`
}

// ListJobRunsResponse is returned by GET /jobs/{name}/runs.
type ListJobRunsResponse struct {
	Items     []JobRun `json:"items"`
	Total     int      `json:"total"`
	NextToken string   `json:"nextToken,omitempty"`
}

// Crawler is a Glue crawler.
type Crawler struct {
	Name    string `json:"name"`
	Role    string `json:"role,omitempty"`
	State   string `json:"state,omitempty"`
	Targets string `json:"targets,omitempty"`
}

// ListCrawlersResponse is returned by GET /crawlers.
type ListCrawlersResponse struct {
	Items     []Crawler `json:"items"`
	Total     int       `json:"total"`
	NextToken string    `json:"nextToken,omitempty"`
}
