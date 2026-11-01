package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"jaiscloud/internal/admin"
	projectsui "jaiscloud/internal/gcp/ui/projects"
)

// projectLister adapts the console's project-discovery snapshot to the admin
// plane's lister, so `/_jaiscloud/projects` and `/api/ui/v1/gcp/projects`
// serve exactly the same rows.
type projectLister struct{ handler *projectsui.Handler }

func (l projectLister) ListProjectRows(ctx context.Context) ([]admin.ProjectRow, error) {
	snapshot, err := l.handler.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return snapshot.Projects, nil
}

// projectsCmd mirrors `doctor`'s host convention: it talks to an already
// running emulator and is read-only.
func projectsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "projects",
		Short: "List configured, registered and observed projects",
		Long: "List every project the emulator knows about: configured at startup,\n" +
			"registered through Resource Manager, or observed on the wire because a\n" +
			"workload addressed it without the console knowing it. The observed set is\n" +
			"per emulator store: instances sharing a --dsn share it, a workload pointed\n" +
			"at a different emulator stays invisible here.",
		RunE: func(cmd *cobra.Command, args []string) error {
			host, _ := cmd.Flags().GetString("host")
			observedOnly, _ := cmd.Flags().GetBool("observed")
			registeredOnly, _ := cmd.Flags().GetBool("registered")
			all, _ := cmd.Flags().GetBool("all")
			since, _ := cmd.Flags().GetDuration("since")
			asJSON, _ := cmd.Flags().GetBool("json")

			rows, err := fetchProjectRows(host)
			if err != nil {
				return err
			}
			if all {
				observedOnly, registeredOnly = false, false
			}
			now := time.Now()
			rows = filterProjectRows(rows, observedOnly, registeredOnly, since, now)
			sortProjectRows(rows)

			if asJSON {
				return writeProjectsJSON(os.Stdout, rows)
			}
			return writeProjectsTable(os.Stdout, rows, now)
		},
	}
	cmd.Flags().String("host", defaultHost, "Emulator host URL")
	cmd.Flags().Bool("observed", false, "Only projects observed on the wire (not configured or registered)")
	cmd.Flags().Bool("registered", false, "Only configured and registered projects")
	cmd.Flags().Bool("all", false, "Every project (the default)")
	cmd.Flags().Duration("since", 0, "Only projects with activity within this duration, e.g. 10m")
	cmd.Flags().Bool("json", false, "Emit the rows as JSON")
	return cmd
}

func fetchProjectRows(host string) ([]admin.ProjectRow, error) {
	url := strings.TrimRight(host, "/") + "/_jaiscloud/projects"
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("cannot reach %s: %w", host, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("projects: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var payload struct {
		Projects []admin.ProjectRow `json:"projects"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode projects: %w", err)
	}
	if payload.Projects == nil {
		payload.Projects = []admin.ProjectRow{}
	}
	return payload.Projects, nil
}

// filterProjectRows applies the kind filters (observed vs everything else) and
// the --since activity window. A row with no LastSeen drop out under --since:
// configured and registered projects carry no activity timestamp.
func filterProjectRows(rows []admin.ProjectRow, observedOnly, registeredOnly bool, since time.Duration, now time.Time) []admin.ProjectRow {
	out := make([]admin.ProjectRow, 0, len(rows))
	for _, row := range rows {
		if observedOnly && row.Kind != "observed" {
			continue
		}
		if registeredOnly && row.Kind == "observed" {
			continue
		}
		if since > 0 {
			last, err := time.Parse(time.RFC3339, row.LastSeen)
			if err != nil || now.Sub(last) > since {
				continue
			}
		}
		out = append(out, row)
	}
	return out
}

// sortProjectRows puts the actionable rows first: observed projects by newest
// activity, then configured, then registered (each by id).
func sortProjectRows(rows []admin.ProjectRow) {
	rank := func(kind string) int {
		switch kind {
		case "observed":
			return 0
		case "configured":
			return 1
		default:
			return 2
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		ri, rj := rank(rows[i].Kind), rank(rows[j].Kind)
		if ri != rj {
			return ri < rj
		}
		if ri == 0 {
			ti, _ := time.Parse(time.RFC3339, rows[i].LastSeen)
			tj, _ := time.Parse(time.RFC3339, rows[j].LastSeen)
			if !ti.Equal(tj) {
				return ti.After(tj)
			}
		}
		return rows[i].ID < rows[j].ID
	})
}

func writeProjectsJSON(w io.Writer, rows []admin.ProjectRow) error {
	if rows == nil {
		rows = []admin.ProjectRow{}
	}
	return json.NewEncoder(w).Encode(rows)
}

func writeProjectsTable(w io.Writer, rows []admin.ProjectRow, now time.Time) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "PROJECT\tKIND\tLAST SEEN\tSERVICES")
	for _, row := range rows {
		last := "—"
		if t, err := time.Parse(time.RFC3339, row.LastSeen); err == nil {
			last = relativeAge(t, now)
		}
		services := "—"
		if len(row.Services) > 0 {
			services = strings.Join(row.Services, ", ")
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", row.ID, row.Kind, last, services)
	}
	return tw.Flush()
}

// relativeAge renders an activity timestamp the way the console does.
func relativeAge(t, now time.Time) string {
	d := now.Sub(t)
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}
