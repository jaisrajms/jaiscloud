package gateway

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/admin"
	"jaiscloud/internal/certstore"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/config"
	"jaiscloud/internal/gateway/middleware"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server is the JaisCloud HTTP server.
type Server struct {
	cfg          *config.Config
	router       chi.Router
	adminHandler *admin.Handler
	registry     *provider.Registry
	cloudAdapter adapter.CloudAdapter
	certs        certstore.CertStore
	extraRoutes  []func(chi.Router)
	// corsLookup returns the stored CORS rules for a given S3 bucket name.
	// If nil, CORS preflight handling is disabled.
	corsLookup func(bucket string) []map[string]any
	// gcsCorsLookup returns the stored CORS rules for a given GCS bucket name.
	// If nil, GCS CORS preflight handling is disabled. Only the GCP binary
	// wires this, so S3/AWS behavior is unaffected.
	gcsCorsLookup func(bucket string) []map[string]any
	// barrier gates cloud requests during import/reset (503 while write-lock held).
	barrier middleware.BarrierMiddleware
	// cloudRoutesDisabled suppresses the cloud catch-all route, leaving only the
	// admin/control plane on this listener. The GCP binary sets it when the REST
	// transport is disabled (gRPC-only deployment).
	cloudRoutesDisabled bool
}

// WithBarrier wires the persistence barrier into the gateway.
// When set, all non-admin cloud requests acquire a read lock.
// Import/reset acquire the write lock, causing 503 during that window.
func WithBarrier(b middleware.BarrierMiddleware) func(*Server) {
	return func(s *Server) {
		s.barrier = b
	}
}

// WithExtraRoutes registers additional routes on the server's router before the
// wildcard catch-all. Use it to attach cloud-specific routes (e.g. AWS IMDS).
func WithExtraRoutes(attach func(chi.Router)) func(*Server) {
	return func(s *Server) {
		s.extraRoutes = append(s.extraRoutes, attach)
	}
}

// WithCloudRoutesDisabled suppresses the cloud catch-all route so this listener
// serves only the admin/control plane (/_jaiscloud/*, /metrics). The GCP binary
// uses it when the REST transport is disabled, so a gRPC-only deployment never
// exposes the GCP REST API. Default (unset) keeps the cloud routes, so the AWS
// binary is unaffected.
func WithCloudRoutesDisabled() func(*Server) {
	return func(s *Server) {
		s.cloudRoutesDisabled = true
	}
}

// WithCORSLookup wires in a function that returns stored S3 CORS rules for a
// bucket. When set, the server intercepts OPTIONS preflight requests and adds
// Access-Control-* headers to regular S3 responses that carry an Origin header.
func WithCORSLookup(fn func(bucket string) []map[string]any) func(*Server) {
	return func(s *Server) {
		s.corsLookup = fn
	}
}

// WithGCSCORSLookup wires in a function that returns stored GCS bucket CORS
// rules (GCS's Bucket.cors shape). When set, the server intercepts OPTIONS
// preflight requests on GCS paths and adds Access-Control-* headers to regular
// GCS responses that carry an Origin header. It is independent of
// WithCORSLookup, so wiring it never alters S3/AWS CORS behavior.
func WithGCSCORSLookup(fn func(bucket string) []map[string]any) func(*Server) {
	return func(s *Server) {
		s.gcsCorsLookup = fn
	}
}

func NewServer(cfg *config.Config, adminHandler *admin.Handler, registry *provider.Registry, cloudAdapter adapter.CloudAdapter, certs certstore.CertStore, opts ...func(*Server)) *Server {
	s := &Server{
		cfg:          cfg,
		adminHandler: adminHandler,
		registry:     registry,
		cloudAdapter: cloudAdapter,
		certs:        certs,
	}
	for _, o := range opts {
		o(s)
	}
	s.buildRouter()
	return s
}

func (s *Server) buildRouter() {
	r := chi.NewRouter()

	r.Use(middleware.Recovery)
	r.Use(middleware.RequestID(s.cfg.RandSource))
	r.Use(middleware.Logging(s.cfg.LogLevel))
	if s.cfg.Metrics {
		r.Use(middleware.Metrics)
	}

	r.Route("/_jaiscloud", func(r chi.Router) {
		r.Get("/health", s.adminHandler.Health)
		r.Get("/doctor", s.adminHandler.Doctor)
		r.Post("/reset", s.adminHandler.Reset)
		r.Get("/export", s.adminHandler.Export)
		r.Post("/import", s.adminHandler.Import)
		r.Get("/lambda/code/{account}/{function}/{qualifier}", s.adminHandler.LambdaCodeHandler)
		r.Get("/lambda/layer/{account}/{layer}/{version}", s.adminHandler.LambdaLayerHandler)
		r.Post("/firehose/flush", s.adminHandler.FirehoseFlushHandler)
		r.Post("/cw-evaluate", s.adminHandler.CWEvaluateHandler)
		r.Post("/clock", s.adminHandler.SetClock)
		r.Get("/clock", s.adminHandler.GetClock)
		r.Post("/ttl-sweep", s.adminHandler.TTLSweepHandler)
		r.Post("/eb-tick", s.adminHandler.EBTickHandler)
		r.Post("/scheduler-tick", s.adminHandler.SchedulerTickHandler)
		r.Post("/tasks-tick", s.adminHandler.TasksTickHandler)
		// Managed snapshot endpoints (Phase 10).
		r.Post("/snapshot", s.adminHandler.SnapshotCreate)
		r.Get("/snapshots", s.adminHandler.SnapshotList)
		r.Post("/snapshot/{name}/revert", s.adminHandler.SnapshotRevert)
		r.Delete("/snapshot/{name}", s.adminHandler.SnapshotDelete)
		r.Get("/snapshot/{name}", s.adminHandler.SnapshotInspect)
		// SNS dummy signing certificate — returned in SNS notification envelopes so
		// SDK-level certificate validation does not fail in integration tests.
		r.Get("/sns/SimpleNotificationService.pem", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/x-pem-file")
			w.WriteHeader(http.StatusOK)
			// Static self-signed dummy PEM — NOT a real SNS signing certificate.
			// JaisCloud does not sign SNS notifications; this endpoint exists solely
			// to satisfy SDK clients that fetch the cert URL before verifying.
			_, _ = io.WriteString(w, snsDummyCert)
		})
	})

	if s.cfg.Metrics {
		r.Handle("/metrics", promhttp.Handler())
	}

	// Cloud-specific extra routes (e.g. AWS IMDS). Attached before the
	// wildcard so chi prioritises them regardless of registration order.
	for _, attach := range s.extraRoutes {
		attach(r)
	}

	// Cloud catch-all: wrap with barrier middleware when configured.
	if !s.cloudRoutesDisabled {
		var cloudHandler http.Handler = http.HandlerFunc(s.handleCloudRequest)
		if s.barrier != nil {
			cloudHandler = middleware.Persistence(s.barrier)(cloudHandler)
		}
		r.Handle("/*", cloudHandler)
	}

	s.router = r
}

// isS3StreamingUpload returns true for S3 PutObject and UploadPart requests —
// the two actions whose body is raw object data that should not be buffered.
// Detection is done purely from headers/path/query so no body is read first.
// Excluded: CopyObject/UploadPartCopy (X-Amz-Copy-Source), bucket-level PUTs
// (no key segment in path), and management PUTs (tagging, acl, versioning, etc).
func isS3StreamingUpload(r *http.Request) bool {
	if r.Method != http.MethodPut {
		return false
	}
	// Detect S3 via SigV4 Authorization header or pre-signed URL query param.
	auth := r.Header.Get("Authorization")
	cred := r.URL.Query().Get("X-Amz-Credential")
	if !strings.Contains(auth, "/s3/aws4_request") && !strings.Contains(cred, "/s3/") {
		return false
	}
	// Server-side copy: no body to stream.
	if r.Header.Get("X-Amz-Copy-Source") != "" {
		return false
	}
	// Management sub-resources carry small XML bodies that must be read normally.
	q := r.URL.Query()
	for _, sub := range []string{"tagging", "acl", "versioning", "cors", "policy", "lifecycle", "notification", "website", "requestPayment", "logging"} {
		if q.Has(sub) {
			return false
		}
	}
	// Must be an object-level PUT (path has both bucket and key).
	// Virtual-hosted style: bucket is in Host, key is the full path.
	if strings.Contains(r.Host, ".s3.") {
		return strings.TrimPrefix(r.URL.Path, "/") != ""
	}
	// Path-style: /bucket/key — need a non-empty key after the bucket segment.
	path := strings.TrimPrefix(r.URL.Path, "/")
	idx := strings.IndexByte(path, '/')
	return idx >= 0 && len(path) > idx+1
}

// isGCSStreamingUpload returns true for GCS uploads whose body is raw object
// data that should be streamed, not buffered. Detection is purely from
// path/method/query so no body is read first. Both simple media uploads and
// multipart/related uploads stream (multipart metadata is small and read first;
// the media part is streamed). Resumable chunks are bounded and excluded here —
// their accumulation is handled by the provider's spill-to-file session.
func isGCSStreamingUpload(r *http.Request) bool {
	if r.Method != http.MethodPost {
		return false
	}
	if !strings.HasPrefix(r.URL.Path, "/upload/storage/v1/") {
		return false
	}
	switch r.URL.Query().Get("uploadType") {
	case "media", "multipart":
		return true
	}
	return false
}

func (s *Server) handleCloudRequest(w http.ResponseWriter, r *http.Request) {
	// P2-6: CORS preflight — intercept before DetectAndDecode since OPTIONS
	// requests have no SigV4 auth and would fail service detection.
	if s.corsLookup != nil {
		origin := r.Header.Get("Origin")
		if origin != "" && r.Method == http.MethodOptions {
			reqMethod := r.Header.Get("Access-Control-Request-Method")
			bucket := corsExtractBucket(r)
			rules := s.corsLookup(bucket)
			rule, ok := corsMatchRule(rules, origin, reqMethod)
			if !ok {
				http.Error(w, "CORS request not allowed", http.StatusForbidden)
				return
			}
			corsWritePreflightHeaders(w, rule, origin, r.Header.Get("Access-Control-Request-Headers"))
			w.WriteHeader(http.StatusOK)
			return
		}
	}
	// GCS bucket CORS preflight (independent of the S3 path above).
	if s.gcsCorsLookup != nil {
		origin := r.Header.Get("Origin")
		if origin != "" && r.Method == http.MethodOptions {
			reqMethod := r.Header.Get("Access-Control-Request-Method")
			bucket := gcsCORSExtractBucket(r)
			rule, ok := gcsCORSMatchRule(s.gcsCorsLookup(bucket), origin, reqMethod)
			if !ok {
				http.Error(w, "CORS request not allowed", http.StatusForbidden)
				return
			}
			gcsCORSPreflightHeaders(w, rule, origin, r.Header.Get("Access-Control-Request-Headers"))
			w.WriteHeader(http.StatusOK)
			return
		}
	}

	streaming := isS3StreamingUpload(r) || isGCSStreamingUpload(r)

	// For streaming S3 uploads we intentionally leave r.Body unread here so
	// the provider can stream directly from it. Always drain+close on exit so
	// the HTTP connection stays reusable regardless of what the provider does.
	// Flush the response to TCP first so the client receives it before we spend
	// time draining any remaining request bytes (otherwise Go's bufio layer holds
	// the response in memory until the handler goroutine fully returns, and if
	// the drain blocks even briefly the client never receives the 200 OK).
	if streaming {
		defer func() {
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			io.Copy(io.Discard, r.Body)
			r.Body.Close()
		}()
	}

	var body []byte
	if !streaming {
		var err error
		body, err = io.ReadAll(r.Body)
		if err != nil {
			slog.Error("failed to read request body",
				"err", err,
				"method", r.Method,
				"path", r.URL.Path,
				"request_id", middleware.GetRequestID(r.Context()),
			)
			http.Error(w, "failed to read request body", http.StatusInternalServerError)
			return
		}
	}

	// Optional cloud batch endpoint (e.g. GCP's POST /batch/{service}/{version}).
	// Intercept before service detection: the adapter parses the multipart
	// envelope and formats the multiplexed response, while the gateway runs each
	// embedded sub-request through the normal detect → dispatch → encode
	// pipeline. Clouds without a batch surface (AWS) do not implement
	// BatchHandler, so their behaviour is unchanged.
	if bh, ok := s.cloudAdapter.(BatchHandler); ok && bh.IsBatchRequest(r) {
		bh.ServeBatch(r.Context(), w, r, body, func(ctx context.Context, sr *http.Request, sb []byte) (int, http.Header, []byte) {
			status, headers, respBody, stream := s.processCloudRequest(ctx, sr, sb)
			if stream != nil {
				defer stream.Close()
				respBody, _ = io.ReadAll(io.LimitReader(stream, maxBatchSubResponseBytes))
			}
			return status, headers, respBody
		})
		return
	}

	status, headers, respBody, stream := s.processCloudRequest(r.Context(), r, body)
	// P2-6: Attach CORS headers to regular responses when Origin is present.
	if s.corsLookup != nil {
		if origin := r.Header.Get("Origin"); origin != "" {
			bucket := corsExtractBucket(r)
			rules := s.corsLookup(bucket)
			CORSAddResponseHeaders(headers, rules, origin)
		}
	}
	if s.gcsCorsLookup != nil {
		if origin := r.Header.Get("Origin"); origin != "" {
			gcsCORSAddResponseHeaders(headers, s.gcsCorsLookup(gcsCORSExtractBucket(r)), origin)
		}
	}
	if stream != nil {
		defer stream.Close()
		writeStreamResponse(w, status, headers, stream)
		return
	}
	writeResponse(w, status, headers, respBody)
}

// processCloudRequest runs one request through the cloud adapter's
// detect → enrich → dispatch → encode pipeline and returns the encoded
// response. It backs both the normal request path and each embedded sub-request
// of a cloud batch request. When the provider returned a streaming body, stream
// is non-nil and respBody is empty; the caller owns closing it. ctx carries the
// metric labels for the originating request (the labelsHolder is a pointer, so
// mutating it is visible to the metrics middleware).
func (s *Server) processCloudRequest(ctx context.Context, r *http.Request, body []byte) (status int, headers http.Header, respBody []byte, stream io.ReadCloser) {
	nr, codec, detectErr := s.cloudAdapter.DetectAndDecode(r, body)
	if detectErr != nil {
		if pe, ok := detectErr.(*model.ProviderError); ok {
			slog.Error("service detection error",
				"code", pe.Code,
				"status", pe.HTTPStatus,
				"method", r.Method,
				"path", r.URL.Path,
				"request_id", middleware.GetRequestID(r.Context()),
			)
			status, headers, respBody = encodeErrorFallback(codec, nil, pe)
			return status, headers, respBody, nil
		}
		slog.Error("service detection failed",
			"err", detectErr,
			"method", r.Method,
			"path", r.URL.Path,
			"request_id", middleware.GetRequestID(r.Context()),
		)
		status, headers, respBody = plainErrorResponse(detectErr.Error(), http.StatusBadRequest)
		return status, headers, respBody, nil
	}

	// Inject gateway context — each cloud adapter extracts identity from the request
	// and supplies its own resource-ID formatter. The gateway has no cloud-specific logic.
	region, accountID, accessKey := s.cloudAdapter.EnrichRequest(r, s.cfg.Region, s.cfg.AccountID)
	nr.Clock = s.cfg.Clock
	nr.Region = region
	nr.AccountID = accountID
	nr.AccessKey = accessKey
	nr.Port = s.cfg.Port
	nr.Cloud = s.cloudAdapter.Cloud()
	nr.ResourceID = s.cloudAdapter.ResourceIDFor(region, accountID)

	// Attach labels for Prometheus metrics middleware.
	ctx = middleware.WithRequestLabels(ctx, string(nr.Cloud), nr.Service, nr.Action)

	// Opt-in request filter (e.g. GCP throttle/quota injection). Type-asserted
	// off the adapter like BatchHandler, so clouds that do not implement it are
	// unaffected. The service's own codec encodes the error, so the envelope
	// matches the service; DecorateError then adds transport retry hints.
	if filter, ok := s.cloudAdapter.(RequestFilter); ok {
		if pe := filter.FilterRequest(nr); pe != nil {
			slog.Warn("request filtered before dispatch",
				"code", pe.Code,
				"status", pe.HTTPStatus,
				"service", nr.Service,
				"action", nr.Action,
				"request_id", middleware.GetRequestID(ctx),
			)
			status, headers, respBody = codec.EncodeError(nr, pe)
			status, headers, respBody = filter.DecorateError(nr, pe, status, headers, respBody)
			return status, headers, respBody, nil
		}
	}

	providerKey := s.cloudAdapter.ServiceToProvider(nr.Service) + "." + nr.Action

	resp, dispatchErr := s.registry.Dispatch(ctx, providerKey, nr)
	if dispatchErr != nil {
		if pe, ok := dispatchErr.(*model.ProviderError); ok {
			logFn := slog.Error
			if pe.HTTPStatus < 500 {
				logFn = slog.Warn
			}
			logFn("provider error",
				"code", pe.Code,
				"message", pe.Message,
				"status", pe.HTTPStatus,
				"service", nr.Service,
				"action", nr.Action,
				"account", nr.AccountID,
				"region", nr.Region,
				"request_id", middleware.GetRequestID(r.Context()),
			)
			status, headers, respBody = codec.EncodeError(nr, pe)
			return status, headers, respBody, nil
		}
		slog.Error("dispatch error",
			"key", providerKey,
			"service", nr.Service,
			"action", nr.Action,
			"account", nr.AccountID,
			"region", nr.Region,
			"err", dispatchErr,
			"request_id", middleware.GetRequestID(r.Context()),
		)
		status, headers, respBody = plainErrorResponse("internal error", http.StatusInternalServerError)
		return status, headers, respBody, nil
	}

	status, headers, respBody = codec.Encode(nr, resp)
	if resp != nil {
		stream, _ = resp.Data["_stream"].(io.ReadCloser)
	}
	return status, headers, respBody, stream
}

// plainErrorResponse mirrors http.Error's headers/body shape so responses
// produced by the extracted pipeline match the previous inline handler.
func plainErrorResponse(msg string, status int) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "text/plain; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	return status, h, []byte(msg + "\n")
}

func encodeErrorFallback(codec adapter.Codec, nr *model.NormalizedRequest, pe *model.ProviderError) (int, http.Header, []byte) {
	if codec != nil {
		return codec.EncodeError(nr, pe)
	}
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	body := fmt.Sprintf(`{"__type":%q,"message":%q}`, pe.Code, pe.Message)
	return pe.HTTPStatus, h, []byte(body)
}

func writeStreamResponse(w http.ResponseWriter, status int, headers http.Header, stream io.ReadCloser) {
	for k, vs := range headers {
		for _, v := range vs {
			w.Header().Set(k, v)
		}
	}
	w.WriteHeader(status)
	io.Copy(w, stream)
}

func writeResponse(w http.ResponseWriter, status int, headers http.Header, body []byte) {
	for k, vs := range headers {
		for _, v := range vs {
			w.Header().Set(k, v)
		}
	}
	if w.Header().Get("Content-Length") == "" {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	}
	w.WriteHeader(status)
	w.Write(body)
}

func (s *Server) loadOrGenerateCert(ctx context.Context) (tls.Certificate, error) {
	stored, err := s.certs.Load(ctx)
	if err == nil && !stored.NeedsRenewal() {
		cert, parseErr := tls.X509KeyPair(stored.CertPEM, stored.KeyPEM)
		if parseErr == nil {
			return cert, nil
		}
		slog.Warn("https: stored cert parse failed, regenerating", "err", parseErr)
	}

	cert, genErr := generateSelfSignedCert(string(s.cfg.Cloud), s.cfg.Region)
	if genErr != nil {
		return tls.Certificate{}, genErr
	}

	leaf, leafErr := x509.ParseCertificate(cert.Certificate[0])
	if leafErr == nil {
		certPEM := pemEncode("CERTIFICATE", cert.Certificate[0])
		keyDER, _ := x509.MarshalECPrivateKey(cert.PrivateKey.(*ecdsa.PrivateKey))
		keyPEM := pemEncode("EC PRIVATE KEY", keyDER)
		if saveErr := s.certs.Save(ctx, &certstore.StoredCert{
			CertPEM:  certPEM,
			KeyPEM:   keyPEM,
			NotAfter: leaf.NotAfter,
		}); saveErr != nil {
			slog.Warn("https: could not persist TLS cert", "err", saveErr)
		}
	}

	return cert, nil
}

func (s *Server) ListenAndServe() error {
	addr := fmt.Sprintf(":%d", s.cfg.Port)
	srv := &http.Server{Addr: addr, Handler: s.router}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mode := "memory+saves"
	if s.cfg.Ephemeral {
		mode = "ephemeral"
	} else if s.cfg.DSN != "" {
		mode = "postgres"
	}

	// Start HTTPS listener on :443.
	// httpsBound is closed exactly once: after a successful net.Listen on :443,
	// before ServeTLS blocks. The fallback goroutine uses it to decide whether
	// to emit an HTTP-only startup line.
	var tlsSrv *http.Server
	httpsBound := make(chan struct{})
	go func() {
		cert, tlsErr := tls.LoadX509KeyPair("/etc/tls/tls.crt", "/etc/tls/tls.key")
		if tlsErr != nil {
			cert, tlsErr = s.loadOrGenerateCert(ctx)
			if tlsErr != nil {
				slog.Warn("https: could not generate TLS cert, HTTPS disabled", "err", tlsErr)
				close(httpsBound)
				return
			}
		}
		tlsSrv = &http.Server{
			Addr:    ":443",
			Handler: s.router,
			TLSConfig: &tls.Config{
				Certificates: []tls.Certificate{cert},
				MinVersion:   tls.VersionTLS12,
			},
		}
		tlsLn, listenErr := net.Listen("tcp", ":443")
		if listenErr != nil {
			slog.Warn("https: could not bind :443, HTTPS disabled", "err", listenErr)
			close(httpsBound)
			return
		}
		// HTTPS is bound — log both ports and unblock the fallback goroutine.
		close(httpsBound)
		slog.Info("jaiscloud started", "http_port", s.cfg.Port, "https_port", 443, "mode", mode)
		if serveErr := tlsSrv.ServeTLS(tlsLn, "", ""); serveErr != nil && serveErr != http.ErrServerClosed {
			slog.Warn("https server error", "err", serveErr)
		}
	}()

	go func() {
		<-httpsBound
		// Only log HTTP-only if HTTPS did not bind (cert/port failure closed the channel early).
		// If HTTPS bound successfully it already logged both ports above.
		if tlsSrv == nil {
			slog.Info("jaiscloud started", "http_port", s.cfg.Port, "mode", mode)
		}
	}()

	go func() {
		if serveErr := srv.Serve(ln); serveErr != nil && serveErr != http.ErrServerClosed {
			slog.Error("server error", "err", serveErr)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if tlsSrv != nil {
		tlsSrv.Shutdown(shutdownCtx) //nolint:errcheck
	}
	return srv.Shutdown(shutdownCtx)
}

// cloudDNSNames returns cloud-specific DNS SANs for the self-signed certificate.
func cloudDNSNames(cloud, region string) []string {
	switch cloud {
	case "azure":
		return []string{
			"*.blob.core.windows.net",
			"*.table.core.windows.net",
			"*.queue.core.windows.net",
			"*.vault.azure.net",
			"management.azure.com",
			"*.management.azure.com",
			"*.documents.azure.com",
			"*.azurewebsites.net",
		}
	case "gcp":
		return []string{
			"*.googleapis.com",
			"storage.googleapis.com",
			"compute.googleapis.com",
			"cloudfunctions.googleapis.com",
			"*.cloudfunctions.net",
			"run.googleapis.com",
			"firestore.googleapis.com",
			"secretmanager.googleapis.com",
			"cloudkms.googleapis.com",
		}
	default: // aws
		regional := func(svc string) string {
			return fmt.Sprintf("%s.%s.amazonaws.com", svc, region)
		}
		return []string{
			"*.amazonaws.com",
			fmt.Sprintf("*.%s.amazonaws.com", region),
			fmt.Sprintf("*.s3.%s.amazonaws.com", region),
			"*.s3.amazonaws.com",
			regional("s3"),
			regional("sqs"),
			regional("sns"),
			regional("dynamodb"),
			regional("lambda"),
			regional("sts"),
			"iam.amazonaws.com",
			regional("kms"),
			regional("secretsmanager"),
			regional("ssm"),
			regional("elasticmapreduce"),
			regional("emr-containers"),
			regional("events"),
			regional("cloudformation"),
			regional("apigateway"),
			regional("execute-api"),
			regional("glue"),
			regional("ec2"),
			"route53.amazonaws.com",
			regional("rds"),
			regional("elasticache"),
			regional("ecs"),
		}
	}
}

// generateSelfSignedCert creates an ECDSA P-256 self-signed certificate with
// cloud-specific DNS SANs valid for 10 years.
func generateSelfSignedCert(cloud, region string) (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate key: %w", err)
	}

	hostname, _ := os.Hostname()
	dnsNames := append([]string{"localhost"}, cloudDNSNames(cloud, region)...)
	if hostname != "" {
		dnsNames = append(dnsNames, hostname)
	}

	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "jaiscloud"},
		DNSNames:     dnsNames,
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    clock.RealNow().Add(-time.Minute),
		NotAfter:     clock.RealNow().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("create certificate: %w", err)
	}

	return tls.X509KeyPair(
		pemEncode("CERTIFICATE", certDER),
		pemEncodeKey(key),
	)
}

func pemEncode(typ string, der []byte) []byte {
	return []byte(fmt.Sprintf("-----BEGIN %s-----\n%s\n-----END %s-----\n",
		typ, encodeBase64Lines(der), typ))
}

func pemEncodeKey(key *ecdsa.PrivateKey) []byte {
	der, _ := x509.MarshalECPrivateKey(key)
	return pemEncode("EC PRIVATE KEY", der)
}

func encodeBase64Lines(data []byte) string {
	const lineLen = 64
	encoded := make([]byte, ((len(data)+2)/3)*4)
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	n := 0
	for i := 0; i < len(data); i += 3 {
		var b [3]byte
		l := copy(b[:], data[i:])
		encoded[n] = alphabet[b[0]>>2]
		encoded[n+1] = alphabet[(b[0]&0x3)<<4|b[1]>>4]
		if l > 1 {
			encoded[n+2] = alphabet[(b[1]&0xF)<<2|b[2]>>6]
		} else {
			encoded[n+2] = '='
		}
		if l > 2 {
			encoded[n+3] = alphabet[b[2]&0x3F]
		} else {
			encoded[n+3] = '='
		}
		n += 4
	}
	encoded = encoded[:n]

	// Insert newlines every lineLen characters.
	var out []byte
	for len(encoded) > lineLen {
		out = append(out, encoded[:lineLen]...)
		out = append(out, '\n')
		encoded = encoded[lineLen:]
	}
	out = append(out, encoded...)
	return string(out)
}

// snsDummyCert is a static, self-signed dummy PEM certificate returned by the
// /_jaiscloud/sns/SimpleNotificationService.pem endpoint. JaisCloud does not
// actually sign SNS notifications; this placeholder satisfies SDK clients that
// fetch the SigningCertURL before attempting (optional) signature verification.
const snsDummyCert = `-----BEGIN CERTIFICATE-----
MIIBpDCCAQmgAwIBAgIUYWlzY2xvdWQtc25zLWR1bW15LTAxMAsGCSqGSIb3DQEB
CwUAMCAxHjAcBgNVBAMTFWphaXNjbG91ZC1zbnMtZHVtbXkwHhcNMjUwMTAxMDAw
MDAwWhcNMzUwMTAxMDAwMDAwWjAgMR4wHAYDVQQDExVqYWlzY2xvdWQtc25zLWR1
bW15MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQC7o4qne60TB3wolFja5sS5
S5bpgADhEFnPY5Q0w/CJtLhZfNkQ0Sf/E7IdbNL6Xe9HCp5hmqOz+HCBQ1KPYF1
nQq7qYI+6E6dFDL0qWcN3Rj3gVkJT9ZoQ+uNQGj5JRtP3r0hGkfQrBs0FuBGrRz
6UUhF+D4v8KlAM3bgQIDAQABMA0GCSqGSIb3DQEBCwUAA4GBAAAAAAAAAAAAAAAa
AAAAAAAAAAAAAAAAAAAAAAAAAAAAjaiscloudSNSdummyCertNotReal==
-----END CERTIFICATE-----
`
