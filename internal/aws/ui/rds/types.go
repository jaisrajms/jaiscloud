package rdsui

// DBInstance is an RDS database instance.
type DBInstance struct {
	ID       string `json:"id"`
	Status   string `json:"status,omitempty"`
	Engine   string `json:"engine,omitempty"`
	Class    string `json:"class,omitempty"`
	Endpoint string `json:"endpoint,omitempty"`
	Port     int    `json:"port,omitempty"`
}

// ListDBInstancesResponse is returned by GET /instances.
type ListDBInstancesResponse struct {
	Items []DBInstance `json:"items"`
	Total int          `json:"total"`
}
