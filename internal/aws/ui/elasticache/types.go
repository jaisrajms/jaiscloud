package elasticacheui

// CacheCluster is an ElastiCache cache cluster.
type CacheCluster struct {
	ID          string `json:"id"`
	Status      string `json:"status,omitempty"`
	Engine      string `json:"engine,omitempty"`
	NodeType    string `json:"nodeType,omitempty"`
	NumNodes    int    `json:"numNodes,omitempty"`
	Endpoint    string `json:"endpoint,omitempty"`
}

// ListCacheClustersResponse is returned by GET /clusters.
type ListCacheClustersResponse struct {
	Items []CacheCluster `json:"items"`
	Total int            `json:"total"`
}
