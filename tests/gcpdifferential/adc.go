//go:build gcp_differential

package gcpdifferential

import (
	"fmt"
	"os/exec"
	"strings"
)

// ADCToken returns a fresh Application Default Credentials access token by
// shelling out to the gcloud CLI. The token is returned in memory only. It is
// never logged, printed, or written to disk by this package; record mode
// failures deliberately surface only the command's exit error, never its
// stdout.
func ADCToken() (string, error) {
	cmd := exec.Command("gcloud", "auth", "application-default", "print-access-token")
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("obtain ADC token (is `gcloud auth application-default login` done?): %w", err)
	}
	token := strings.TrimSpace(string(out))
	if token == "" {
		return "", fmt.Errorf("application-default print-access-token returned an empty token")
	}
	return token, nil
}
