package sesui

// Identity is an SES verified identity (email or domain).
type Identity struct {
	Identity string `json:"identity"`
	Type     string `json:"type,omitempty"`
}

// ListIdentitiesResponse is returned by GET /identities.
type ListIdentitiesResponse struct {
	Items []Identity `json:"items"`
	Total int        `json:"total"`
}
