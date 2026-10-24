// Package docker is the shared Docker Engine API client used by the emulator's
// container executors. Every executor dials the LOCAL /var/run/docker.sock and
// ignores DOCKER_HOST (the console's runtime health view surfaces that), so this
// client owns the socket dialer, the API version, the container lifecycle
// calls, label-filtered listing/reaping, and log demultiplexing once. A
// per-service executor keeps only its own container template and lifecycle
// policy on top of it.
package docker

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"jaiscloud/internal/platform"
)

// DefaultSocket is the local Docker Engine API socket the executors dial.
const DefaultSocket = "/var/run/docker.sock"

// APIVersion is the Docker Engine API version prefix. Docker 24+ shares this
// wire surface (the Lambda/ECS executors pin the same version).
const APIVersion = "v1.41"

// Container label keys shared by every executor. The service label value is
// executor-specific ("cloudrun", "dataproc", ...); the instance label scopes an
// emulator's containers so two instances on one daemon never reap each other's.
const (
	LabelService  = "jaiscloud.io/service"
	LabelInstance = "jaiscloud.io/instance-id"
)

// Config configures a Client.
type Config struct {
	// Logger receives best-effort teardown warnings; defaults to slog.Default.
	Logger *slog.Logger
	// Socket is the Docker API unix socket; defaults to DefaultSocket.
	Socket string
	// Client overrides the HTTP client (tests). When nil, a client that dials
	// Socket is built.
	Client *http.Client
	// Timeout bounds a non-streaming call when Client is nil; 0 keeps the
	// zero-timeout default. (The per-call context governs streaming calls.)
	Timeout time.Duration
}

// Client is a Docker Engine API client.
type Client struct {
	http   *http.Client
	logger *slog.Logger
}

// New returns a Client. An injected cfg.Client is used as-is; otherwise a
// client that dials cfg.Socket (or DefaultSocket) is built.
func New(cfg Config) *Client {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	client := cfg.Client
	if client == nil {
		socket := cfg.Socket
		if socket == "" {
			socket = DefaultSocket
		}
		client = &http.Client{
			Timeout: cfg.Timeout,
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "unix", socket)
				},
			},
		}
	}
	return &Client{http: client, logger: logger}
}

// Ping reports whether the Docker daemon at socket answers the Engine API ping.
// An empty socket uses the default local socket. It lets startup fall back to
// mock execution when no daemon is reachable, rather than failing every request.
func Ping(ctx context.Context, socket string) error {
	if socket == "" {
		socket = DefaultSocket
	}
	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		},
	}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://docker/_ping", nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("docker: ping %s = HTTP %d", socket, resp.StatusCode)
	}
	return nil
}

// Call issues an Engine API request and returns the response body and status.
func (c *Client) Call(ctx context.Context, method, path string, body []byte) ([]byte, int, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://docker/"+APIVersion+path, reader)
	if err != nil {
		return nil, 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	return rb, resp.StatusCode, nil
}

// Stream issues a GET whose body is streamed rather than buffered (the
// container log endpoint).
func (c *Client) Stream(ctx context.Context, method, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, "http://docker/"+APIVersion+path, nil)
	if err != nil {
		return nil, err
	}
	return c.http.Do(req)
}

// Create creates a container, retrying once after removing a same-named leftover
// when the daemon reports a name conflict (409), and returns its id.
func (c *Client) Create(ctx context.Context, name string, body []byte) (string, error) {
	createURL := "/containers/create?name=" + url.QueryEscape(name)
	respBody, status, err := c.Call(ctx, http.MethodPost, createURL, body)
	if err != nil {
		return "", fmt.Errorf("docker create: %w", err)
	}
	if status == http.StatusConflict {
		if rmErr := c.Remove(context.WithoutCancel(ctx), name); rmErr == nil {
			respBody, status, err = c.Call(ctx, http.MethodPost, createURL, body)
			if err != nil {
				return "", fmt.Errorf("docker create: %w", err)
			}
		}
	}
	if status >= 300 {
		return "", fmt.Errorf("docker create: HTTP %d: %s", status, strings.TrimSpace(string(respBody)))
	}
	var createResp struct {
		ID string `json:"Id"`
	}
	if err := json.Unmarshal(respBody, &createResp); err != nil || createResp.ID == "" {
		return "", fmt.Errorf("docker create: malformed response: %s", strings.TrimSpace(string(respBody)))
	}
	return createResp.ID, nil
}

// Remove stops (best-effort) and force-removes a container by id or name,
// attempting the force delete even when the stop fails. Removing a missing
// container is not an error.
func (c *Client) Remove(ctx context.Context, idOrName string) error {
	if idOrName == "" {
		return nil
	}
	if _, status, err := c.Call(ctx, http.MethodPost, "/containers/"+idOrName+"/stop", nil); err != nil {
		c.logger.Debug("docker: stop failed; forcing remove", "container", idOrName, "err", err)
	} else if status >= 300 && status != http.StatusNotFound {
		c.logger.Debug("docker: stop returned non-2xx", "status", status, "container", idOrName)
	}
	_, status, err := c.Call(ctx, http.MethodDelete, "/containers/"+idOrName+"?force=true", nil)
	if err != nil {
		return err
	}
	if status >= 300 && status != http.StatusNotFound {
		return fmt.Errorf("docker delete %s: HTTP %d", idOrName, status)
	}
	return nil
}

// Container is the subset of a container-list entry the executors use.
type Container struct {
	ID     string            `json:"Id"`
	Names  []string          `json:"Names"`
	Labels map[string]string `json:"Labels"`
}

// List returns the containers matching the label filter.
func (c *Client) List(ctx context.Context, filters map[string][]string) ([]Container, error) {
	rawFilter, err := json.Marshal(filters)
	if err != nil {
		return nil, err
	}
	body, status, err := c.Call(ctx, http.MethodGet, "/containers/json?all=true&filters="+url.QueryEscape(string(rawFilter)), nil)
	if err != nil {
		return nil, err
	}
	if status >= 300 {
		return nil, fmt.Errorf("docker list: HTTP %d", status)
	}
	var items []Container
	if err := json.Unmarshal(body, &items); err != nil {
		return nil, err
	}
	return items, nil
}

// RemoveByFilter stops and removes every container matching the label filter,
// returning how many were reaped.
func (c *Client) RemoveByFilter(ctx context.Context, filters map[string][]string) (int, error) {
	items, err := c.List(ctx, filters)
	if err != nil {
		return 0, err
	}
	var reaped int
	for _, item := range items {
		if err := c.Remove(ctx, item.ID); err != nil {
			c.logger.Warn("docker: failed to reap container", "id", item.ID, "err", err)
			continue
		}
		reaped++
	}
	return reaped, nil
}

// StreamLogs writes a container's stdout/stderr (demultiplexed) to sink. The
// container is created without a TTY, so the daemon frames each chunk as
// [stream(1)][000][size(4, big-endian)][payload].
func (c *Client) StreamLogs(ctx context.Context, id string, sink io.Writer) error {
	resp, err := c.Stream(ctx, http.MethodGet, "/containers/"+id+"/logs?stdout=1&stderr=1&follow=false")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("docker logs: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return DemuxLogs(resp.Body, sink)
}

// DemuxLogs decodes Docker's multiplexed (stdout/stderr) log stream into sink.
func DemuxLogs(r io.Reader, sink io.Writer) error {
	header := make([]byte, 8)
	for {
		if _, err := io.ReadFull(r, header); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		size := binary.BigEndian.Uint32(header[4:8])
		if size == 0 {
			continue
		}
		if _, err := io.CopyN(sink, r, int64(size)); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
	}
}

// LabelFilter builds a Docker label filter that always scopes to this emulator
// instance, so two emulators sharing a daemon never reap each other's
// containers.
func LabelFilter(service, instance string, extra ...string) map[string][]string {
	labels := []string{LabelService + "=" + service}
	if instance != "" {
		labels = append(labels, LabelInstance+"="+instance)
	}
	labels = append(labels, extra...)
	return map[string][]string{"label": labels}
}

// ShortInstance truncates an instance id for a container-name prefix.
func ShortInstance(instanceID string) string {
	if len(instanceID) >= 8 {
		return instanceID[:8]
	}
	if instanceID != "" {
		return instanceID
	}
	return "local"
}

// ShortID truncates a container id for logging.
func ShortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

// ParseTime parses a Docker RFC3339Nano timestamp, returning the zero time for
// the daemon's "never" sentinel (0001-01-01T00:00:00Z).
func ParseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// BindsAndEnv converts platform.ApplyDocker's `-v`/`-e` argv pairs into the
// HostConfig Binds list and a flat env list. It is the one place the executors
// translate the platform overlay for a container; the error is returned so the
// caller can log a bad platform config without failing the container.
func BindsAndEnv(p *platform.PlatformConfig) (binds, env []string, err error) {
	if p == nil {
		return nil, nil, nil
	}
	volArgs, envArgs, err := platform.ApplyDocker(p)
	if err != nil {
		return nil, nil, err
	}
	for i := 1; i < len(volArgs); i += 2 {
		binds = append(binds, volArgs[i])
	}
	for i := 1; i < len(envArgs); i += 2 {
		env = append(env, envArgs[i])
	}
	return binds, env, nil
}
