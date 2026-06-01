package ec2ui

// Instance is an EC2 instance.
type Instance struct {
	ID           string `json:"id"`
	State        string `json:"state,omitempty"`
	ImageID      string `json:"imageId,omitempty"`
	InstanceType string `json:"instanceType,omitempty"`
	PublicIP     string `json:"publicIp,omitempty"`
	PrivateIP    string `json:"privateIp,omitempty"`
	LaunchTime   string `json:"launchTime,omitempty"`
}

// ListInstancesResponse is returned by GET /instances.
type ListInstancesResponse struct {
	Items     []Instance `json:"items"`
	Total     int        `json:"total"`
	NextToken string     `json:"nextToken,omitempty"`
}
