package elbv2ui

// LoadBalancer is an ELBv2 load balancer summary.
type LoadBalancer struct {
	ARN     string `json:"arn"`
	Name    string `json:"name"`
	DNSName string `json:"dnsName,omitempty"`
	Scheme  string `json:"scheme,omitempty"`
	Type    string `json:"type,omitempty"`
	State   string `json:"state,omitempty"`
}

// ListLoadBalancersResponse is returned by GET /load-balancers.
type ListLoadBalancersResponse struct {
	Items []LoadBalancer `json:"items"`
	Total int            `json:"total"`
}
