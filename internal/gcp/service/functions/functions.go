package functions

import (
	"context"
	"errors"
	"time"

	lambdaexec "jaiscloud/internal/executor/lambda"
	"jaiscloud/internal/gcp/paging"
	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/model"
)

// CreateFunction creates a function and returns it with its create operation
// (done inline by default, in flight under async LRO timing).
// project/location scope the store; id is the explicit functionId parameter
// (which wins over any name in the input). The runtime is required.
func (s *Service) CreateFunction(ctx context.Context, project, location, id string, in FunctionInput, v Version) (functionsstore.Function, Operation, error) {
	if location == "" {
		return functionsstore.Function{}, Operation{}, invalidArgument("missing location")
	}
	id, err := resolveCreateID(in.Name, location, id)
	if err != nil {
		return functionsstore.Function{}, Operation{}, err
	}
	if id == "" {
		return functionsstore.Function{}, Operation{}, invalidArgument("missing functionId")
	}
	if in.Runtime == "" {
		return functionsstore.Function{}, Operation{}, invalidArgument("missing runtime")
	}
	if err := validateConfigInput(in, v); err != nil {
		return functionsstore.Function{}, Operation{}, err
	}
	f := newFunction(project, location, id, in)
	if err := validateConfigRelations(f); err != nil {
		return functionsstore.Function{}, Operation{}, err
	}
	sha, size, blobKey, serr := s.resolveSource(ctx, project, location, id, in)
	if serr != nil {
		return functionsstore.Function{}, Operation{}, serr
	}
	if blobKey != "" {
		f.SourceSHA256, f.SourceSize, f.SourceBlobKey = sha, size, blobKey
		// First deployed revision of the function.
		f.Revision = 1
	}
	if err := s.functions.CreateFunction(ctx, project, location, id, f); err != nil {
		if errors.Is(err, functionsstore.ErrAlreadyExists) {
			// A content-addressed key can collide with the existing function's
			// archive; never delete a blob the stored function still points at.
			if cur, cerr := s.functions.GetFunction(ctx, project, location, id); cerr == nil && cur.SourceBlobKey == blobKey {
				blobKey = ""
			}
			s.discardSource(ctx, blobKey)
			return functionsstore.Function{}, Operation{}, model.NewProviderError("AlreadyExists", "function already exists", 409)
		}
		s.discardSource(ctx, blobKey)
		return functionsstore.Function{}, Operation{}, err
	}
	// Materialize the backing Eventarc trigger (and its dead-letter
	// subscription) for a Pub/Sub or Cloud Storage event trigger (FD9, FP2).
	if s.triggerProvisioner != nil && isEventarcBackedTrigger(f.EventTrigger) {
		f = s.ensureTrigger(ctx, project, location, id, f, nil)
		s.persistTriggerFields(ctx, project, location, id, f)
	}
	target := resourceID(project)("cloud-function", location+"/"+id)
	op := s.newOperation(location, "create", target, &f)
	if err := s.persistOperation(ctx, project, op); err != nil {
		return functionsstore.Function{}, Operation{}, err
	}
	return f, op, nil
}

// GetFunction returns one function.
func (s *Service) GetFunction(ctx context.Context, project, location, id string) (functionsstore.Function, error) {
	if location == "" || id == "" {
		return functionsstore.Function{}, invalidArgument("missing location or function name")
	}
	f, err := s.functions.GetFunction(ctx, project, location, id)
	if err != nil {
		return functionsstore.Function{}, mapErr(err)
	}
	return f, nil
}

// requireFunction returns NotFound when the function does not exist.
func (s *Service) requireFunction(ctx context.Context, project, location, id string) error {
	_, err := s.GetFunction(ctx, project, location, id)
	return err
}

// ListFunctions returns a cursor page of functions. The location "-" is GCP's
// all-locations wildcard, aggregating across every region for the project.
func (s *Service) ListFunctions(ctx context.Context, project, location string, pageSize int, pageToken string) ([]functionsstore.Function, string, error) {
	var (
		fns []functionsstore.Function
		err error
	)
	if location == "-" {
		fns, err = s.functions.ListFunctionsAllLocations(ctx, project)
	} else {
		fns, err = s.functions.ListFunctions(ctx, project, location)
	}
	if err != nil {
		return nil, "", err
	}
	page, next := paging.Page(fns, func(f functionsstore.Function) string { return f.ID }, pageParams(pageSize, pageToken))
	return page, next, nil
}

// UpdateFunction merges the input into the stored function under the given mask
// and returns it with its update operation (done inline by default, in flight
// under async LRO timing).
func (s *Service) UpdateFunction(ctx context.Context, project, location, id string, in FunctionInput, mask []string, v Version) (functionsstore.Function, Operation, error) {
	if location == "" || id == "" {
		return functionsstore.Function{}, Operation{}, invalidArgument("missing location or function name")
	}
	old, gerr := s.GetFunction(ctx, project, location, id)
	if gerr != nil {
		return functionsstore.Function{}, Operation{}, gerr
	}
	if verr := validateConfigInput(in, v); verr != nil {
		return functionsstore.Function{}, Operation{}, verr
	}
	var sha string
	var size int64
	var blobKey string
	if maskAppliesSource(mask) {
		var serr error
		sha, size, blobKey, serr = s.resolveSource(ctx, project, location, id, in)
		if serr != nil {
			return functionsstore.Function{}, Operation{}, serr
		}
	}
	f, err := s.functions.UpdateFunctionAtomic(ctx, project, location, id, func(f functionsstore.Function) (functionsstore.Function, error) {
		if uerr := ApplyFunctionUpdate(&f, in, mask); uerr != nil {
			return functionsstore.Function{}, uerr
		}
		if verr := validateConfigRelations(f); verr != nil {
			return functionsstore.Function{}, verr
		}
		if blobKey != "" {
			f.SourceSHA256, f.SourceSize, f.SourceBlobKey = sha, size, blobKey
			// Each new source archive is a new revision.
			f.Revision++
		}
		return f, nil
	})
	if err != nil {
		s.discardSource(ctx, blobKey)
		return functionsstore.Function{}, Operation{}, mapErr(err)
	}
	if blobKey != "" && old.SourceBlobKey != "" && old.SourceBlobKey != blobKey {
		// Only the latest revision's archive is retained; the revision counter
		// and rendered revision name are persisted on the function row (FD5).
		s.discardSource(ctx, old.SourceBlobKey)
	}
	// Re-materialize (or remove) the backing Eventarc trigger to match the
	// function's current Pub/Sub or Cloud Storage event trigger (FD9, FP2).
	if s.triggerProvisioner != nil {
		if isEventarcBackedTrigger(f.EventTrigger) {
			// Only re-ensure when the trigger materially changed (or was never
			// provisioned), so a PATCH of an unrelated field does not bump the
			// backing trigger's updateTime/etag.
			if f.EventTrigger.Trigger == "" || !triggerEqual(old.EventTrigger, f.EventTrigger) {
				f = s.ensureTrigger(ctx, project, location, id, f, old.EventTrigger)
				s.persistTriggerFields(ctx, project, location, id, f)
			}
		} else if old.EventTrigger != nil {
			s.deleteTrigger(ctx, project, location, id)
		}
	}
	target := resourceID(project)("cloud-function", location+"/"+id)
	op := s.newOperation(location, "update", target, &f)
	if err := s.persistOperation(ctx, project, op); err != nil {
		return functionsstore.Function{}, Operation{}, err
	}
	return f, op, nil
}

// DeleteFunction deletes a function and returns its delete operation (done
// inline by default, in flight under async LRO timing; its response is empty).
func (s *Service) DeleteFunction(ctx context.Context, project, location, id string) (Operation, error) {
	if location == "" || id == "" {
		return Operation{}, invalidArgument("missing location or function name")
	}
	f, gerr := s.functions.GetFunction(ctx, project, location, id)
	if gerr != nil {
		return Operation{}, mapErr(gerr)
	}
	if err := s.functions.DeleteFunction(ctx, project, location, id); err != nil {
		return Operation{}, mapErr(err)
	}
	s.discardSource(ctx, f.SourceBlobKey)
	s.deleteTrigger(ctx, project, location, id)
	target := resourceID(project)("cloud-function", location+"/"+id)
	op := s.newOperation(location, "delete", target, nil)
	if err := s.persistOperation(ctx, project, op); err != nil {
		return Operation{}, err
	}
	return op, nil
}

// CallFunction invokes a function synchronously via the Lambda executor. The
// executor (mock echo by default, Docker/K8s under JAISCLOUD_EXECUTOR_MODE)
// runs the function's entryPoint and returns the result as a string. An executor
// failure is reported as invokeErr with a nil error so the wire response carries
// it (matching real Cloud Functions, which returns the function error in-band).
func (s *Service) CallFunction(ctx context.Context, project, location, id, data string) (executionID, result, invokeErr string, err error) {
	f, err := s.GetFunction(ctx, project, location, id)
	if err != nil {
		return "", "", "", err
	}
	// Admission gate (FP1): reserve an in-flight slot against the function's
	// configured instance/concurrency capacity and the project-wide account cap
	// before doing any work. Every invocation entry point (REST/gRPC
	// CallFunction, the HTTPS trigger, and event delivery) funnels through here,
	// so the two transports and the delivery engine cannot drift. Throttling is
	// RESOURCE_EXHAUSTED / HTTP 429.
	release, err := s.gate.acquire(project, project+"/"+location+"/"+id, effectiveConcurrencyLimit(f), s.accountConcurrency)
	if err != nil {
		return "", "", "", err
	}
	defer release()
	timeout, terr := time.ParseDuration(f.Timeout)
	if terr != nil || timeout <= 0 {
		timeout = defaultFunctionTimeout
	}
	invCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req := lambdaexec.InvokeRequest{
		FunctionName: f.ID,
		// CodeKey carries project+location+id to the shared executor's
		// CodeLoader (whose interface has only account+name); Image maps the
		// GCP runtime onto the executor's container image. Both are no-ops in
		// mock mode, which stays the default.
		CodeKey:     CodeKey(location, id),
		Image:       RuntimeImage(f.Runtime),
		Runtime:     f.Runtime,
		Handler:     f.EntryPoint,
		EnvVars:     f.EnvironmentVariables,
		Payload:     []byte(data),
		AccountID:   project,
		MemoryMB:    f.AvailableMemoryMB,
		TimeoutSecs: int(timeout.Seconds()),
	}
	executionID = newUUID()
	res, ierr := s.executor.Invoke(invCtx, req)
	if ierr != nil {
		return executionID, "", ierr.Error(), nil
	}
	return executionID, string(res.Payload), "", nil
}

// GenerateDownloadURL returns a fake signed download URL for a function's source
// archive. The function must exist (NotFound otherwise), matching real Cloud
// Functions.
func (s *Service) GenerateDownloadURL(ctx context.Context, project, location, id string) (string, error) {
	if err := s.requireFunction(ctx, project, location, id); err != nil {
		return "", err
	}
	return "https://storage.googleapis.com/" + project + "-cloudfunctions/" + location + "/" + id + ".zip", nil
}
