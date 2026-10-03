package serviceaccount

import "testing"

func TestProjectFromEmail(t *testing.T) {
	cases := map[string]string{
		"sa@proj.iam.gserviceaccount.com": "proj",
		"sa@a-b.iam.gserviceaccount.com":  "a-b",
		"sa@example.com":                  "",
		"not-an-email":                    "",
		"@proj.iam.gserviceaccount.com":   "",
		"sa@.iam.gserviceaccount.com":     "",
		"other.iam.gserviceaccount.com":   "",
	}
	for email, want := range cases {
		if got := ProjectFromEmail(email); got != want {
			t.Errorf("ProjectFromEmail(%q) = %q, want %q", email, got, want)
		}
	}
}

func TestAccountForSA(t *testing.T) {
	cases := []struct {
		account, email, want string
	}{
		{"real-project", "sa@other.iam.gserviceaccount.com", "real-project"},
		{"-", "sa@proj.iam.gserviceaccount.com", "proj"},
		{"", "sa@proj.iam.gserviceaccount.com", "proj"},
		{"-", "opaque", "-"},
	}
	for _, tc := range cases {
		if got := AccountForSA(tc.account, tc.email); got != tc.want {
			t.Errorf("AccountForSA(%q, %q) = %q, want %q", tc.account, tc.email, got, tc.want)
		}
	}
}
