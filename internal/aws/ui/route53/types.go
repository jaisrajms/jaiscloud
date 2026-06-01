package route53ui

// HostedZone is a Route53 hosted zone.
type HostedZone struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	RecordCount int    `json:"recordCount,omitempty"`
	Private     bool   `json:"private,omitempty"`
}

// ListHostedZonesResponse is returned by GET /zones.
type ListHostedZonesResponse struct {
	Items     []HostedZone `json:"items"`
	Total     int          `json:"total"`
	NextToken string       `json:"nextToken,omitempty"`
}

// RecordSet is a DNS record set.
type RecordSet struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	TTL     int    `json:"ttl,omitempty"`
	Records []string `json:"records,omitempty"`
}

// ListRecordSetsResponse is returned by GET /zones/{id}/records.
type ListRecordSetsResponse struct {
	Items     []RecordSet `json:"items"`
	Total     int         `json:"total"`
	NextToken string      `json:"nextToken,omitempty"`
}
