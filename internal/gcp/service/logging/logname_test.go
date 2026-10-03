package logging

import "testing"

func TestParseLogNameScopes(t *testing.T) {
	cases := []struct {
		name  string
		scope string
		logID string
	}{
		{"projects/my-project/logs/syslog", "projects/my-project", "syslog"},
		{"organizations/123/logs/syslog", "organizations/123", "syslog"},
		{"folders/456/logs/syslog", "folders/456", "syslog"},
		{"billingAccounts/0X0X0X-0X0X0X-0X0X0X/logs/syslog", "billingAccounts/0X0X0X-0X0X0X-0X0X0X", "syslog"},
		// LOG_ID is URL-encoded; %2F decodes to a slash.
		{"projects/p/logs/cloudaudit.googleapis.com%2Factivity", "projects/p", "cloudaudit.googleapis.com/activity"},
		{"organizations/1234567890/logs/cloudresourcemanager.googleapis.com%2Factivity", "organizations/1234567890", "cloudresourcemanager.googleapis.com/activity"},
		{"folders/9/logs/a%2Fb%2Fc", "folders/9", "a/b/c"},
		// Leading slash is stripped for backward compatibility.
		{"/projects/p/logs/leading", "projects/p", "leading"},
	}
	for _, tc := range cases {
		scope, logID, err := ParseLogName(tc.name)
		if err != nil {
			t.Errorf("ParseLogName(%q) error = %v", tc.name, err)
			continue
		}
		if scope != tc.scope || logID != tc.logID {
			t.Errorf("ParseLogName(%q) = (%q, %q), want (%q, %q)", tc.name, scope, logID, tc.scope, tc.logID)
		}
	}
}

func TestParseLogNameRejectsMalformed(t *testing.T) {
	bad := []string{
		"",
		"projects//logs/",
		"projects/p/logs/",
		"projects//logs/x",
		"projects/p/logs",
		"projects/p/logs/a/b",
		"projects/p/notlogs/x",
		"projects/p/logs/%zz",
		"bots/x/logs/y",
		"logs/y",
		"projects/p",
		"organizations//logs/x",
		"folders/1/notlogs/x",
		"billingAccounts/1/logs/x/y",
	}
	for _, name := range bad {
		if _, _, err := ParseLogName(name); err == nil {
			t.Errorf("ParseLogName(%q) succeeded, want InvalidArgument", name)
		}
	}
}

func TestCanonicalLogName(t *testing.T) {
	cases := []struct {
		scope, logID, want string
	}{
		{"projects/p", "syslog", "projects/p/logs/syslog"},
		{"organizations/123", "cloudresourcemanager.googleapis.com/activity", "organizations/123/logs/cloudresourcemanager.googleapis.com%2Factivity"},
		{"folders/9", "a/b", "folders/9/logs/a%2Fb"},
	}
	for _, tc := range cases {
		if got := CanonicalLogName(tc.scope, tc.logID); got != tc.want {
			t.Errorf("CanonicalLogName(%q, %q) = %q, want %q", tc.scope, tc.logID, got, tc.want)
		}
	}
}

func TestParseScopeParent(t *testing.T) {
	good := map[string]string{
		"projects/my-project":    "projects/my-project",
		"organizations/123":      "organizations/123",
		"folders/456":            "folders/456",
		"billingAccounts/0X0X":   "billingAccounts/0X0X",
		"projects/p/logs/syslog": "projects/p",
		"organizations/1/logs/x": "organizations/1",
		"legacy-bare-project":    "projects/legacy-bare-project",
	}
	for in, want := range good {
		got, err := ParseScopeParent(in)
		if err != nil {
			t.Errorf("ParseScopeParent(%q) error = %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("ParseScopeParent(%q) = %q, want %q", in, got, want)
		}
	}

	bad := []string{
		"",
		"projects/",
		"projects/p/logs",
		"projects//logs/x",
		"bots/x",
		"projects/p/logs/a/b",
		"a/b/c",
	}
	for _, in := range bad {
		if _, err := ParseScopeParent(in); err == nil {
			t.Errorf("ParseScopeParent(%q) succeeded, want InvalidArgument", in)
		}
	}
}
