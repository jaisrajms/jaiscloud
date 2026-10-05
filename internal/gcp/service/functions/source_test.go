package functions

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/executor/container"
	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/gcp/store/gcs"
	"jaiscloud/internal/store"
)

// fakeFetcher is a SourceFetcher over an in-memory {bucket/object: bytes} map.
type fakeFetcher struct {
	objects map[string][]byte
	err     error
}

func (f *fakeFetcher) FetchObjectBytes(_ context.Context, bucket, object string) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	if b, ok := f.objects[bucket+"/"+object]; ok {
		return b, nil
	}
	return nil, gcs.ErrNoSuchObject
}

// recordingExecutor captures the last InvokeRequest and echoes the payload.
type recordingExecutor struct {
	req     container.Request
	invoked int
}

func (e *recordingExecutor) Invoke(_ context.Context, req container.Request) (container.Result, error) {
	e.req = req
	e.invoked++
	return container.Result{Payload: req.Payload}, nil
}
func (e *recordingExecutor) DeleteFunction(context.Context, string) {}
func (e *recordingExecutor) Reset(context.Context)                  {}
func (e *recordingExecutor) Close() error                           { return nil }

func newSourceTestService(t *testing.T, blobs blobfs.BlobStore, f SourceFetcher, ex container.Executor) *Service {
	t.Helper()
	opts := []Option{WithBlobs(blobs), WithSourceFetcher(f)}
	if ex != nil {
		opts = append(opts, WithExecutor(ex))
	}
	return NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore(), opts...)
}

func sha256hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func TestCreateFunctionPersistsSourceAndInvokesWithCode(t *testing.T) {
	ctx := context.Background()
	blobs := blobfs.NewMemoryBlobStore()
	zip := []byte("PK\x03\x04 fake function archive")
	fetcher := &fakeFetcher{objects: map[string][]byte{"src-bucket/code.zip": zip}}
	exec := &recordingExecutor{}
	s := newSourceTestService(t, blobs, fetcher, exec)

	f, _, err := s.CreateFunction(ctx, "proj", "us-central1", "hello",
		FunctionInput{Runtime: "python312", EntryPoint: "main.handler", SourceBucket: "src-bucket", SourceObject: "code.zip"}, V1)
	if err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	wantSHA := sha256hex(zip)
	if f.SourceSHA256 != wantSHA || f.SourceSize != int64(len(zip)) || f.SourceBlobKey == "" {
		t.Fatalf("source metadata = sha=%q size=%d key=%q", f.SourceSHA256, f.SourceSize, f.SourceBlobKey)
	}
	if got, err := blobs.Get(ctx, functionsSourceBucket, f.SourceBlobKey); err != nil || string(got) != string(zip) {
		t.Fatalf("stored source = %q, err=%v", got, err)
	}

	// The persisted archive round-trips through the CodeLoader using the
	// executor's composite key.
	loaded, err := s.LoadCode(ctx, "proj", CodeKey("us-central1", "hello"), "$LATEST")
	if err != nil || string(loaded) != string(zip) {
		t.Fatalf("LoadCode = %q, err=%v", loaded, err)
	}

	// CallFunction hands the executor the composite CodeKey and the runtime
	// image mapped from the GCP runtime.
	if _, _, _, err := s.CallFunction(ctx, "proj", "us-central1", "hello", CallInput{Data: `{"x":1}`}); err != nil {
		t.Fatalf("CallFunction: %v", err)
	}
	if exec.req.CodeKey != "us-central1.hello" {
		t.Fatalf("CodeKey = %q, want us-central1.hello", exec.req.CodeKey)
	}
	if exec.req.Image != "" {
		t.Fatalf("Image = %q, want empty (the profile resolves the image, not the core)", exec.req.Image)
	}
	if exec.req.Runtime != "python312" {
		t.Fatalf("Runtime = %q, want python312", exec.req.Runtime)
	}
	if exec.req.FunctionName != "hello" {
		t.Fatalf("FunctionName = %q, want hello", exec.req.FunctionName)
	}
}

func TestCreateFunctionInlineArchivePersistsSource(t *testing.T) {
	ctx := context.Background()
	blobs := blobfs.NewMemoryBlobStore()
	zip := []byte("PK\x03\x04 inline console archive")
	// The fetcher is wired AND a resolvable gs:// reference is present, so this
	// proves the inline archive takes precedence over the GCS fetch path.
	fetched := []byte("PK\x03\x04 fetched archive")
	fetcher := &fakeFetcher{objects: map[string][]byte{"b/o.zip": fetched}}
	s := newSourceTestService(t, blobs, fetcher, nil)

	f, _, err := s.CreateFunction(ctx, "proj", "us-central1", "inline",
		FunctionInput{
			Runtime:       "nodejs20",
			EntryPoint:    "handler",
			SourceArchive: zip,
			SourceBucket:  "b",
			SourceObject:  "o.zip",
		}, V2)
	if err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	wantSHA := sha256hex(zip)
	if f.SourceSHA256 != wantSHA || f.SourceSize != int64(len(zip)) || f.SourceBlobKey == "" {
		t.Fatalf("source metadata = sha=%q size=%d key=%q", f.SourceSHA256, f.SourceSize, f.SourceBlobKey)
	}
	if got, err := blobs.Get(ctx, functionsSourceBucket, f.SourceBlobKey); err != nil || string(got) != string(zip) {
		t.Fatalf("stored source = %q, err=%v; want the inline archive, not the fetched one", got, err)
	}
}

func TestCreateFunctionMissingSourceIsNotFound(t *testing.T) {
	ctx := context.Background()
	s := newSourceTestService(t, blobfs.NewMemoryBlobStore(), &fakeFetcher{objects: map[string][]byte{}}, nil)
	_, _, err := s.CreateFunction(ctx, "proj", "us-central1", "gone",
		FunctionInput{Runtime: "python312", SourceBucket: "b", SourceObject: "missing.zip"}, V1)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected not-found error, got %v", err)
	}
	if _, gerr := s.GetFunction(ctx, "proj", "us-central1", "gone"); gerr == nil {
		t.Fatal("function should not exist after a failed source fetch")
	}
}

func TestCreateFunctionGCsSourceOnAlreadyExists(t *testing.T) {
	ctx := context.Background()
	blobs := blobfs.NewMemoryBlobStore()
	fetcher := &fakeFetcher{objects: map[string][]byte{"b/o.zip": []byte("zip")}}
	s := newSourceTestService(t, blobs, fetcher, nil)
	in := FunctionInput{Runtime: "nodejs20", SourceBucket: "b", SourceObject: "o.zip"}
	if _, _, err := s.CreateFunction(ctx, "p", "us", "dup", in, V1); err != nil {
		t.Fatalf("first create: %v", err)
	}
	if _, _, err := s.CreateFunction(ctx, "p", "us", "dup", in, V1); err == nil {
		t.Fatal("expected AlreadyExists")
	}
	// The second (failed) create must not leave a second blob behind.
	keys, _ := blobs.List(ctx, functionsSourceBucket, "")
	if len(keys) != 1 {
		t.Fatalf("source blobs = %v, want exactly 1", keys)
	}
}

func TestUpdateFunctionReplacesSourceAndGCsPrior(t *testing.T) {
	ctx := context.Background()
	blobs := blobfs.NewMemoryBlobStore()
	fetcher := &fakeFetcher{objects: map[string][]byte{
		"b/one.zip": []byte("zip-one"),
		"b/two.zip": []byte("zip-two"),
	}}
	s := newSourceTestService(t, blobs, fetcher, nil)
	created, _, err := s.CreateFunction(ctx, "p", "us", "f",
		FunctionInput{Runtime: "nodejs20", SourceBucket: "b", SourceObject: "one.zip"}, V1)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.Revision != 1 {
		t.Fatalf("create revision = %d, want 1", created.Revision)
	}
	f, _, err := s.UpdateFunction(ctx, "p", "us", "f",
		FunctionInput{SourceBucket: "b", SourceObject: "two.zip"}, []string{"build_config.source"}, V1)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if f.SourceSHA256 != sha256hex([]byte("zip-two")) {
		t.Fatalf("revision not updated: %q", f.SourceSHA256)
	}
	if f.Revision != 2 {
		t.Fatalf("revision counter = %d, want 2 after a redeploy", f.Revision)
	}
	// The two revisions render distinct Cloud Run revision names.
	sc := FunctionJSON(V2, "p", f)["serviceConfig"].(map[string]any)
	if sc["revision"] != "projects/p/locations/us/services/f/revisions/f-00002-"+f.SourceSHA256[:8] {
		t.Fatalf("rendered revision = %v", sc["revision"])
	}
	keys, _ := blobs.List(ctx, functionsSourceBucket, "")
	if len(keys) != 1 {
		t.Fatalf("source blobs = %v, want the prior revision GC'd", keys)
	}
}

func TestDeleteFunctionRemovesSource(t *testing.T) {
	ctx := context.Background()
	blobs := blobfs.NewMemoryBlobStore()
	fetcher := &fakeFetcher{objects: map[string][]byte{"b/o.zip": []byte("zip")}}
	s := newSourceTestService(t, blobs, fetcher, nil)
	if _, _, err := s.CreateFunction(ctx, "p", "us", "f",
		FunctionInput{Runtime: "nodejs20", SourceBucket: "b", SourceObject: "o.zip"}, V1); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.DeleteFunction(ctx, "p", "us", "f"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	keys, _ := blobs.List(ctx, functionsSourceBucket, "")
	if len(keys) != 0 {
		t.Fatalf("source blobs after delete = %v, want none", keys)
	}
}

func TestResetClearsSources(t *testing.T) {
	ctx := context.Background()
	blobs := blobfs.NewMemoryBlobStore()
	fetcher := &fakeFetcher{objects: map[string][]byte{"b/o.zip": []byte("zip")}}
	s := newSourceTestService(t, blobs, fetcher, nil)
	if _, _, err := s.CreateFunction(ctx, "p", "us", "f",
		FunctionInput{Runtime: "nodejs20", SourceBucket: "b", SourceObject: "o.zip"}, V1); err != nil {
		t.Fatalf("create: %v", err)
	}
	s.Reset(ctx)
	keys, _ := blobs.List(ctx, functionsSourceBucket, "")
	if len(keys) != 0 {
		t.Fatalf("source blobs after reset = %v, want none", keys)
	}
}

func TestSourceRefAndCodeKey(t *testing.T) {
	if b, o := sourceRef(FunctionInput{SourceArchiveURL: "gs://bkt/dir/o.zip"}); b != "bkt" || o != "dir/o.zip" {
		t.Fatalf("v1 sourceRef = %q/%q", b, o)
	}
	if b, o := sourceRef(FunctionInput{SourceBucket: "b2", SourceObject: "o2"}); b != "b2" || o != "o2" {
		t.Fatalf("v2 sourceRef = %q/%q", b, o)
	}
	if b, o := sourceRef(FunctionInput{SourceArchiveURL: "https://example/x.zip"}); b != "" || o != "" {
		t.Fatalf("non-gs sourceRef = %q/%q", b, o)
	}
	for _, loc := range []string{"us-central1", "europe-west1"} {
		key := CodeKey(loc, "fn-1")
		gotLoc, gotID, ok := splitCodeKey(key)
		if !ok || gotLoc != loc || gotID != "fn-1" {
			t.Fatalf("splitCodeKey(%q) = %q/%q/%v", key, gotLoc, gotID, ok)
		}
	}
}

func TestUpdateUnrelatedFieldKeepsSource(t *testing.T) {
	ctx := context.Background()
	blobs := blobfs.NewMemoryBlobStore()
	fetcher := &fakeFetcher{objects: map[string][]byte{"b/o.zip": []byte("zip")}}
	s := newSourceTestService(t, blobs, fetcher, nil)
	created, _, err := s.CreateFunction(ctx, "p", "us", "f",
		FunctionInput{Runtime: "nodejs20", SourceBucket: "b", SourceObject: "o.zip"}, V1)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	f, _, err := s.UpdateFunction(ctx, "p", "us", "f",
		FunctionInput{Description: "d"}, []string{"description"}, V1)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if f.SourceBlobKey != created.SourceBlobKey {
		t.Fatalf("source blob key changed on an unrelated update: %q -> %q", created.SourceBlobKey, f.SourceBlobKey)
	}
	if keys, _ := blobs.List(ctx, functionsSourceBucket, ""); len(keys) != 1 {
		t.Fatalf("source blobs = %v, want the existing archive retained", keys)
	}
}

func TestLoadCodeWithoutSourceIsNil(t *testing.T) {
	ctx := context.Background()
	s := newSourceTestService(t, blobfs.NewMemoryBlobStore(), &fakeFetcher{objects: map[string][]byte{}}, nil)
	if _, _, err := s.CreateFunction(ctx, "p", "us", "f", FunctionInput{Runtime: "nodejs20"}, V1); err != nil {
		t.Fatalf("create: %v", err)
	}
	code, err := s.LoadCode(ctx, "p", "us.f", "$LATEST")
	if err != nil || code != nil {
		t.Fatalf("LoadCode = %q, %v; want nil,nil", code, err)
	}
}

// fakeBucketEnsurer records EnsureBucket calls for the upload-url tests.
type fakeBucketEnsurer struct {
	buckets []string
	err     error
}

func (f *fakeBucketEnsurer) EnsureBucket(_ context.Context, project, bucket, location string) error {
	if f.err != nil {
		return f.err
	}
	f.buckets = append(f.buckets, project+"/"+bucket+"/"+location)
	return nil
}

func TestGenerateUploadURLProvisionsBucket(t *testing.T) {
	ctx := context.Background()
	ensurer := &fakeBucketEnsurer{}
	s := NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		WithBlobs(blobfs.NewMemoryBlobStore()), WithSourceBuckets(ensurer))
	up, err := s.GenerateUploadURL(ctx, "proj", "us-central1", "http://localhost:8080/")
	if err != nil {
		t.Fatalf("GenerateUploadURL: %v", err)
	}
	wantBucket := SourceBucket("proj", "us-central1")
	if up.Bucket != wantBucket || up.Object == "" || !strings.HasSuffix(up.Object, ".zip") {
		t.Fatalf("upload target = %+v, want bucket %q", up, wantBucket)
	}
	if want := "http://localhost:8080/" + wantBucket + "/" + up.Object; up.URL != want {
		t.Fatalf("upload URL = %q, want %q", up.URL, want)
	}
	if len(ensurer.buckets) != 1 || ensurer.buckets[0] != "proj/"+wantBucket+"/us-central1" {
		t.Fatalf("ensurer calls = %v", ensurer.buckets)
	}
}

func TestGenerateUploadURLSingleBucketName(t *testing.T) {
	// A function's location names the bucket deterministically (not per upload),
	// so two uploads share one bucket and only the object id changes.
	s := NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore(), WithBlobs(blobfs.NewMemoryBlobStore()))
	first, err := s.GenerateUploadURL(context.Background(), "proj", "us-central1", "http://h")
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := s.GenerateUploadURL(context.Background(), "proj", "us-central1", "http://h")
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if first.Bucket != second.Bucket {
		t.Fatalf("bucket drifted: %q vs %q", first.Bucket, second.Bucket)
	}
	if first.Object == second.Object {
		t.Fatalf("object id not unique: %q", first.Object)
	}
}

func TestGenerateUploadURLWithoutStorage(t *testing.T) {
	s := NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore())
	if _, err := s.GenerateUploadURL(context.Background(), "proj", "us-central1", "http://h"); err == nil {
		t.Fatal("expected an error when source storage is not configured")
	}
}

func TestSourceBucketSanitizedAndCapped(t *testing.T) {
	if got := SourceBucket("My_Project", "US Central"); got != "gcf-v2-sources-my_project-us-central" {
		t.Fatalf("SourceBucket = %q", got)
	}
	long := SourceBucket(strings.Repeat("p", 80), strings.Repeat("l", 80))
	if len(long) > maxGCSCBucketName {
		t.Fatalf("bucket name %d chars exceeds %d: %q", len(long), maxGCSCBucketName, long)
	}
}

func TestStoreSourceRejectsEmptyArchive(t *testing.T) {
	s := newSourceTestService(t, blobfs.NewMemoryBlobStore(), &fakeFetcher{objects: map[string][]byte{}}, nil)
	if _, _, _, err := s.StoreSource(context.Background(), "p", "us", "f", nil); err == nil {
		t.Fatal("expected empty-archive rejection")
	}
}
