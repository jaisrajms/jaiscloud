package cloudwatchui

// Metric is a CloudWatch metric name/namespace pair.
type Metric struct {
	Namespace  string `json:"namespace"`
	MetricName string `json:"metricName"`
}

// Datapoint is a single statistics datapoint.
type Datapoint struct {
	Timestamp   string  `json:"timestamp"`
	Sum         float64 `json:"sum,omitempty"`
	Average     float64 `json:"average,omitempty"`
	Minimum     float64 `json:"minimum,omitempty"`
	Maximum     float64 `json:"maximum,omitempty"`
	SampleCount float64 `json:"sampleCount,omitempty"`
	Unit        string  `json:"unit,omitempty"`
}

// ListMetricsResponse is returned by GET /metrics.
type ListMetricsResponse struct {
	Items     []Metric `json:"items"`
	NextToken string   `json:"nextToken,omitempty"`
	Total     int      `json:"total"`
}

// MetricStatisticsResponse is returned by GET /metrics/statistics.
type MetricStatisticsResponse struct {
	Label      string      `json:"label"`
	Datapoints []Datapoint `json:"datapoints"`
}

// Alarm is a CloudWatch alarm.
type Alarm struct {
	AlarmName          string  `json:"alarmName"`
	AlarmARN           string  `json:"alarmArn,omitempty"`
	AlarmDescription   string  `json:"alarmDescription,omitempty"`
	Namespace          string  `json:"namespace,omitempty"`
	MetricName         string  `json:"metricName,omitempty"`
	Statistic          string  `json:"statistic,omitempty"`
	Period             int     `json:"period,omitempty"`
	Threshold          float64 `json:"threshold,omitempty"`
	ComparisonOperator string  `json:"comparisonOperator,omitempty"`
	EvaluationPeriods  int     `json:"evaluationPeriods,omitempty"`
	StateValue         string  `json:"stateValue,omitempty"`
	StateReason        string  `json:"stateReason,omitempty"`
	ActionsEnabled     bool    `json:"actionsEnabled"`
	UpdatedAt          string  `json:"updatedAt,omitempty"`
}

// ListAlarmsResponse is returned by GET /alarms.
type ListAlarmsResponse struct {
	Items     []Alarm `json:"items"`
	NextToken string  `json:"nextToken,omitempty"`
	Total     int     `json:"total"`
}

// PutAlarmRequest is the body for POST /alarms.
type PutAlarmRequest struct {
	AlarmName          string  `json:"alarmName"`
	AlarmDescription   string  `json:"alarmDescription,omitempty"`
	Namespace          string  `json:"namespace,omitempty"`
	MetricName         string  `json:"metricName,omitempty"`
	Statistic          string  `json:"statistic,omitempty"`
	Period             int     `json:"period,omitempty"`
	Threshold          float64 `json:"threshold,omitempty"`
	ComparisonOperator string  `json:"comparisonOperator,omitempty"`
	EvaluationPeriods  int     `json:"evaluationPeriods,omitempty"`
	ActionsEnabled     bool    `json:"actionsEnabled"`
}

// Dashboard is a CloudWatch dashboard summary.
type Dashboard struct {
	DashboardName string `json:"dashboardName"`
	DashboardARN  string `json:"dashboardArn,omitempty"`
	LastModified  string `json:"lastModified,omitempty"`
	DashboardBody string `json:"dashboardBody,omitempty"`
}

// ListDashboardsResponse is returned by GET /dashboards.
type ListDashboardsResponse struct {
	Items []Dashboard `json:"items"`
	Total int         `json:"total"`
}
