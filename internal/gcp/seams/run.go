package seams

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"

	"jaiscloud/internal/model"
)

// Run is Cloud Run as seen by Eventarc: a CloudEvents delivery to a
// destination.cloudRun trigger is a data-plane request to the service, sent
// through the Run.Invoke key.
type Run struct{ s *Set }

// Run returns the Cloud Run seam.
func (s *Set) Run() *Run { return &Run{s: s} }

// Invoke forwards one CloudEvents POST to a service's latest ready revision. It
// returns the service's HTTP status, or the status of the failure: 404 for a
// missing service and 503 when no runtime is ready (the default mock runtime),
// as the Cloud Run data plane answers.
func (r *Run) Invoke(ctx context.Context, project, region, service, path string, headers map[string]string, body []byte) (int, error) {
	// Built from parts, not parsed from a string: the delivery path is forwarded as
	// given, as it always was, whatever characters it carries.
	raw := (&http.Request{
		Method: http.MethodPost, URL: &url.URL{Scheme: "http", Host: "localhost", Path: path},
		Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body)),
	}).WithContext(ctx)
	for k, v := range headers {
		raw.Header.Set(k, v)
	}
	nr := r.s.request("run", "Invoke", project, map[string]any{
		"project": project, "location": region, "service": service,
		"invokePath": path, "body": body,
	}, nil)
	nr.Raw = raw
	resp, err := r.s.send(ctx, r.s.ad.ServiceToProvider("run")+".Invoke", nr)
	if err != nil {
		var pe *model.ProviderError
		if errors.As(err, &pe) && pe.HTTPStatus > 0 {
			return pe.HTTPStatus, err
		}
		return http.StatusBadGateway, err
	}
	return resp.HTTPStatus, nil
}
