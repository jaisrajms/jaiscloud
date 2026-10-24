package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"time"
)

// --- GCS JSON API types (mirrors localgcp internal/gcs) ---

type Bucket struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Object struct {
	Kind        string `json:"kind"`
	ID          string `json:"id"`
	Name        string `json:"name"`
	Bucket      string `json:"bucket"`
	Size        string `json:"size"`
	ContentType string `json:"contentType"`
	Md5Hash     string `json:"md5Hash"`
	Crc32c      string `json:"crc32c"`
}

type BucketList struct {
	Kind  string   `json:"kind"`
	Items []Bucket `json:"items"`
}

type ObjectList struct {
	Kind     string   `json:"kind"`
	Items    []Object `json:"items"`
	Prefixes []string `json:"prefixes,omitempty"`
}

type gcpError struct {
	Error gcpErrorBody `json:"error"`
}
type gcpErrorBody struct {
	Code    int              `json:"code"`
	Message string           `json:"message"`
	Errors  []gcpErrorDetail `json:"errors"`
}
type gcpErrorDetail struct {
	Message string `json:"message"`
	Domain  string `json:"domain"`
	Reason  string `json:"reason"`
}

var base = envOr("GCP_REST_ENDPOINT", "http://localhost:8080")

// bname appends the run suffix so each run uses fresh bucket names.
func bname(n string) string { return n + suffix }

func bucketJSON(n string) string { return fmt.Sprintf(`{"name":%q}`, bname(n)) }

func gcsClient() *http.Client {
	return &http.Client{Timeout: 30 * time.Second}
}

func gcsPostJSON(url, body string) (*http.Response, error) {
	return gcsClient().Post(url, "application/json", strings.NewReader(body))
}

func gcsUpload(bucket, name, content string) (Object, error) {
	url := fmt.Sprintf("%s/upload/storage/v1/b/%s/o?uploadType=media&name=%s", base, bucket, name)
	req, _ := http.NewRequest(http.MethodPost, url, strings.NewReader(content))
	req.Header.Set("Content-Type", "text/plain")
	resp, err := gcsClient().Do(req)
	if err != nil {
		return Object{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return Object{}, fmt.Errorf("upload %s/%s: status %d: %s", bucket, name, resp.StatusCode, string(b))
	}
	var obj Object
	if err := json.NewDecoder(resp.Body).Decode(&obj); err != nil {
		return Object{}, err
	}
	return obj, nil
}

func wantStatus(resp *http.Response, want int) error {
	defer resp.Body.Close()
	if resp.StatusCode != want {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("expected status %d, got %d: %s", want, resp.StatusCode, string(b))
	}
	return nil
}

func runGCS() {
	cases := []struct {
		name string
		fn   func() error
	}{
		{"TestCreateBucket", gcsCreateBucket},
		{"TestCreateDuplicateBucket", gcsCreateDuplicateBucket},
		{"TestGetBucket", gcsGetBucket},
		{"TestGetBucketNotFound", gcsGetBucketNotFound},
		{"TestListBuckets", gcsListBuckets},
		{"TestDeleteBucket", gcsDeleteBucket},
		{"TestDeleteNonEmptyBucket", gcsDeleteNonEmptyBucket},
		{"TestSimpleUploadAndDownload", gcsSimpleUploadAndDownload},
		{"TestGetObjectMetadata", gcsGetObjectMetadata},
		{"TestGetObjectNotFound", gcsGetObjectNotFound},
		{"TestDeleteObject", gcsDeleteObject},
		{"TestListObjects", gcsListObjects},
		{"TestListObjectsWithPrefixAndDelimiter", gcsListObjectsWithPrefixAndDelimiter},
		{"TestCopyObject", gcsCopyObject},
		{"TestMultipartUpload", gcsMultipartUpload},
		{"TestResumableUpload", gcsResumableUpload},
		{"TestObjectWithSlashesInName", gcsObjectWithSlashesInName},
		{"TestLargeObject", gcsLargeObject},
		{"TestErrorEnvelopeFormat", gcsErrorEnvelopeFormat},
		{"TestSignedURLGenerate", gcsSignedURLGenerate},
		{"TestSignedURLDownload", gcsSignedURLDownload},
		{"TestSmokeEndToEnd", gcsSmokeEndToEnd},
	}
	for _, c := range cases {
		record("gcs", c.name, c.fn())
	}
}

func gcsCreateBucket() error {
	resp, err := gcsPostJSON(base+"/storage/v1/b?project=test", bucketJSON("h-create"))
	if err != nil {
		return err
	}
	if err := wantStatus(resp, 200); err != nil {
		return err
	}
	resp2, err := gcsClient().Get(base + "/storage/v1/b/" + bname("h-create"))
	if err != nil {
		return err
	}
	defer resp2.Body.Close()
	var b Bucket
	json.NewDecoder(resp2.Body).Decode(&b)
	if b.Name != bname("h-create") {
		return fmt.Errorf("name = %q, want %q", b.Name, bname("h-create"))
	}
	if b.Kind != "storage#bucket" {
		return fmt.Errorf("kind = %q, want storage#bucket", b.Kind)
	}
	return nil
}

func gcsCreateDuplicateBucket() error {
	if _, err := gcsPostJSON(base+"/storage/v1/b?project=test", bucketJSON("h-dup")); err != nil {
		return err
	}
	resp, err := gcsPostJSON(base+"/storage/v1/b?project=test", bucketJSON("h-dup"))
	if err != nil {
		return err
	}
	return wantStatus(resp, 409)
}

func gcsGetBucket() error {
	if _, err := gcsPostJSON(base+"/storage/v1/b?project=test", bucketJSON("h-get")); err != nil {
		return err
	}
	resp, err := gcsClient().Get(base + "/storage/v1/b/" + bname("h-get"))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("status %d, want 200", resp.StatusCode)
	}
	var b Bucket
	json.NewDecoder(resp.Body).Decode(&b)
	if b.Name != bname("h-get") {
		return fmt.Errorf("name = %q, want %q", b.Name, bname("h-get"))
	}
	return nil
}

func gcsGetBucketNotFound() error {
	resp, err := gcsClient().Get(base + "/storage/v1/b/" + bname("h-nope"))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		return fmt.Errorf("status %d, want 404", resp.StatusCode)
	}
	var e gcpError
	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
		return fmt.Errorf("error envelope decode: %v", err)
	}
	if e.Error.Code != 404 {
		return fmt.Errorf("error.code = %d, want 404", e.Error.Code)
	}
	return nil
}

func gcsListBuckets() error {
	if _, err := gcsPostJSON(base+"/storage/v1/b?project=test", bucketJSON("h-list-a")); err != nil {
		return err
	}
	if _, err := gcsPostJSON(base+"/storage/v1/b?project=test", bucketJSON("h-list-b")); err != nil {
		return err
	}
	resp, err := gcsClient().Get(base + "/storage/v1/b?project=test")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var list BucketList
	json.NewDecoder(resp.Body).Decode(&list)
	names := map[string]bool{}
	for _, it := range list.Items {
		names[it.Name] = true
	}
	if !names[bname("h-list-a")] || !names[bname("h-list-b")] {
		return fmt.Errorf("expected buckets %q and %q in list", bname("h-list-a"), bname("h-list-b"))
	}
	return nil
}

func gcsDeleteBucket() error {
	if _, err := gcsPostJSON(base+"/storage/v1/b?project=test", bucketJSON("h-del")); err != nil {
		return err
	}
	req, _ := http.NewRequest(http.MethodDelete, base+"/storage/v1/b/"+bname("h-del"), nil)
	resp, err := gcsClient().Do(req)
	if err != nil {
		return err
	}
	if err := wantStatus(resp, 204); err != nil {
		return err
	}
	resp2, _ := gcsClient().Get(base + "/storage/v1/b/" + bname("h-del"))
	return wantStatus(resp2, 404)
}

func gcsDeleteNonEmptyBucket() error {
	if _, err := gcsPostJSON(base+"/storage/v1/b?project=test", bucketJSON("h-full")); err != nil {
		return err
	}
	if _, err := gcsUpload(bname("h-full"), "f.txt", "hello"); err != nil {
		return err
	}
	req, _ := http.NewRequest(http.MethodDelete, base+"/storage/v1/b/"+bname("h-full"), nil)
	resp, err := gcsClient().Do(req)
	if err != nil {
		return err
	}
	return wantStatus(resp, 409)
}

func gcsSimpleUploadAndDownload() error {
	if _, err := gcsPostJSON(base+"/storage/v1/b?project=test", bucketJSON("h-up")); err != nil {
		return err
	}
	content := "hello, jaiscloud!"
	obj, err := gcsUpload(bname("h-up"), "greeting.txt", content)
	if err != nil {
		return err
	}
	if obj.Name != "greeting.txt" {
		return fmt.Errorf("obj.Name = %q, want greeting.txt", obj.Name)
	}
	if obj.Bucket != bname("h-up") {
		return fmt.Errorf("obj.Bucket = %q, want %q", obj.Bucket, bname("h-up"))
	}
	if obj.Size != fmt.Sprintf("%d", len(content)) {
		return fmt.Errorf("obj.Size = %q, want %d", obj.Size, len(content))
	}
	resp, err := gcsClient().Get(base + "/storage/v1/b/" + bname("h-up") + "/o/greeting.txt?alt=media")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("download status %d, want 200", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	if string(b) != content {
		return fmt.Errorf("download content %q, want %q", string(b), content)
	}
	return nil
}

func gcsGetObjectMetadata() error {
	if _, err := gcsPostJSON(base+"/storage/v1/b?project=test", bucketJSON("h-meta")); err != nil {
		return err
	}
	if _, err := gcsUpload(bname("h-meta"), "doc.txt", "content"); err != nil {
		return err
	}
	resp, err := gcsClient().Get(base + "/storage/v1/b/" + bname("h-meta") + "/o/doc.txt")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("status %d, want 200", resp.StatusCode)
	}
	var obj Object
	json.NewDecoder(resp.Body).Decode(&obj)
	if obj.Kind != "storage#object" {
		return fmt.Errorf("kind = %q, want storage#object", obj.Kind)
	}
	if obj.Md5Hash == "" {
		return fmt.Errorf("expected non-empty md5Hash")
	}
	return nil
}

func gcsGetObjectNotFound() error {
	if _, err := gcsPostJSON(base+"/storage/v1/b?project=test", bucketJSON("h-empty")); err != nil {
		return err
	}
	resp, err := gcsClient().Get(base + "/storage/v1/b/" + bname("h-empty") + "/o/nope.txt?alt=media")
	if err != nil {
		return err
	}
	return wantStatus(resp, 404)
}

func gcsDeleteObject() error {
	if _, err := gcsPostJSON(base+"/storage/v1/b?project=test", bucketJSON("h-delobj")); err != nil {
		return err
	}
	if _, err := gcsUpload(bname("h-delobj"), "gone.txt", "bye"); err != nil {
		return err
	}
	req, _ := http.NewRequest(http.MethodDelete, base+"/storage/v1/b/"+bname("h-delobj")+"/o/gone.txt", nil)
	resp, err := gcsClient().Do(req)
	if err != nil {
		return err
	}
	if err := wantStatus(resp, 204); err != nil {
		return err
	}
	resp2, _ := gcsClient().Get(base + "/storage/v1/b/" + bname("h-delobj") + "/o/gone.txt?alt=media")
	return wantStatus(resp2, 404)
}

func gcsListObjects() error {
	if _, err := gcsPostJSON(base+"/storage/v1/b?project=test", bucketJSON("h-listobj")); err != nil {
		return err
	}
	for _, n := range []string{"a.txt", "b.txt", "dir/c.txt"} {
		if _, err := gcsUpload(bname("h-listobj"), n, "x"); err != nil {
			return err
		}
	}
	resp, err := gcsClient().Get(base + "/storage/v1/b/" + bname("h-listobj") + "/o")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var list ObjectList
	json.NewDecoder(resp.Body).Decode(&list)
	if len(list.Items) != 3 {
		return fmt.Errorf("expected 3 objects, got %d", len(list.Items))
	}
	return nil
}

func gcsListObjectsWithPrefixAndDelimiter() error {
	if _, err := gcsPostJSON(base+"/storage/v1/b?project=test", bucketJSON("h-prefix")); err != nil {
		return err
	}
	for _, n := range []string{"photos/2024/jan.jpg", "photos/2024/feb.jpg", "photos/2025/mar.jpg", "photos/root.jpg"} {
		if _, err := gcsUpload(bname("h-prefix"), n, "x"); err != nil {
			return err
		}
	}
	resp, err := gcsClient().Get(base + "/storage/v1/b/" + bname("h-prefix") + "/o?prefix=photos/&delimiter=/")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var list ObjectList
	json.NewDecoder(resp.Body).Decode(&list)
	if len(list.Items) != 1 {
		return fmt.Errorf("expected 1 item, got %d: %+v", len(list.Items), list.Items)
	}
	if list.Items[0].Name != "photos/root.jpg" {
		return fmt.Errorf("item = %q, want photos/root.jpg", list.Items[0].Name)
	}
	if len(list.Prefixes) != 2 {
		return fmt.Errorf("expected 2 prefixes, got %d: %v", len(list.Prefixes), list.Prefixes)
	}
	if list.Prefixes[0] != "photos/2024/" || list.Prefixes[1] != "photos/2025/" {
		return fmt.Errorf("unexpected prefixes: %v", list.Prefixes)
	}
	return nil
}

func gcsCopyObject() error {
	if _, err := gcsPostJSON(base+"/storage/v1/b?project=test", bucketJSON("h-src")); err != nil {
		return err
	}
	if _, err := gcsPostJSON(base+"/storage/v1/b?project=test", bucketJSON("h-dst")); err != nil {
		return err
	}
	if _, err := gcsUpload(bname("h-src"), "original.txt", "copy me"); err != nil {
		return err
	}
	url := base + "/storage/v1/b/" + bname("h-src") + "/o/original.txt/copyTo/b/" + bname("h-dst") + "/o/copy.txt"
	resp, err := gcsPostJSON(url, "{}")
	if err != nil {
		return err
	}
	if err := wantStatus(resp, 200); err != nil {
		return err
	}
	resp2, _ := gcsClient().Get(base + "/storage/v1/b/" + bname("h-dst") + "/o/copy.txt?alt=media")
	defer resp2.Body.Close()
	b, _ := io.ReadAll(resp2.Body)
	if string(b) != "copy me" {
		return fmt.Errorf("copied content %q, want copy me", string(b))
	}
	return nil
}

func gcsMultipartUpload() error {
	if _, err := gcsPostJSON(base+"/storage/v1/b?project=test", bucketJSON("h-mp")); err != nil {
		return err
	}
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	metaHeader := map[string][]string{"Content-Type": {"application/json"}}
	metaPart, _ := w.CreatePart(metaHeader)
	metaPart.Write([]byte(`{"name":"multipart.txt","contentType":"text/plain"}`))
	dataHeader := map[string][]string{"Content-Type": {"text/plain"}}
	dataPart, _ := w.CreatePart(dataHeader)
	dataPart.Write([]byte("multipart content here"))
	w.Close()

	req, _ := http.NewRequest(http.MethodPost, base+"/upload/storage/v1/b/"+bname("h-mp")+"/o?uploadType=multipart", &body)
	req.Header.Set("Content-Type", w.FormDataContentType())
	resp, err := gcsClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("multipart status %d: %s", resp.StatusCode, string(b))
	}
	var obj Object
	json.NewDecoder(resp.Body).Decode(&obj)
	if obj.Name != "multipart.txt" {
		return fmt.Errorf("name %q, want multipart.txt", obj.Name)
	}
	resp2, _ := gcsClient().Get(base + "/storage/v1/b/" + bname("h-mp") + "/o/multipart.txt?alt=media")
	defer resp2.Body.Close()
	data, _ := io.ReadAll(resp2.Body)
	if string(data) != "multipart content here" {
		return fmt.Errorf("content %q, want multipart content here", string(data))
	}
	return nil
}

func gcsResumableUpload() error {
	if _, err := gcsPostJSON(base+"/storage/v1/b?project=test", bucketJSON("h-resume")); err != nil {
		return err
	}
	initReq, _ := http.NewRequest(http.MethodPost, base+"/upload/storage/v1/b/"+bname("h-resume")+"/o?uploadType=resumable&name=big.bin", strings.NewReader("{}"))
	initReq.Header.Set("Content-Type", "application/json")
	initReq.Header.Set("X-Upload-Content-Type", "application/octet-stream")
	initResp, err := gcsClient().Do(initReq)
	if err != nil {
		return err
	}
	if initResp.StatusCode != 200 {
		initResp.Body.Close()
		return fmt.Errorf("init status %d, want 200", initResp.StatusCode)
	}
	location := initResp.Header.Get("Location")
	initResp.Body.Close()
	if location == "" {
		return fmt.Errorf("missing Location header")
	}
	if !strings.HasPrefix(location, "http") {
		location = base + location
	}
	content := []byte("resumable upload content")
	putReq, _ := http.NewRequest(http.MethodPut, location, bytes.NewReader(content))
	putReq.Header.Set("Content-Type", "application/octet-stream")
	putResp, err := gcsClient().Do(putReq)
	if err != nil {
		return err
	}
	defer putResp.Body.Close()
	if putResp.StatusCode != 200 {
		b, _ := io.ReadAll(putResp.Body)
		return fmt.Errorf("put status %d (want 200): %s", putResp.StatusCode, string(b))
	}
	var obj Object
	json.NewDecoder(putResp.Body).Decode(&obj)
	if obj.Name != "big.bin" {
		return fmt.Errorf("name %q, want big.bin", obj.Name)
	}
	resp, _ := gcsClient().Get(base + "/storage/v1/b/" + bname("h-resume") + "/o/big.bin?alt=media")
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if string(data) != "resumable upload content" {
		return fmt.Errorf("content %q", string(data))
	}
	return nil
}

func gcsObjectWithSlashesInName() error {
	if _, err := gcsPostJSON(base+"/storage/v1/b?project=test", bucketJSON("h-slash")); err != nil {
		return err
	}
	if _, err := gcsUpload(bname("h-slash"), "path/to/deep/file.txt", "deep content"); err != nil {
		return err
	}
	resp, err := gcsClient().Get(base + "/storage/v1/b/" + bname("h-slash") + "/o/path/to/deep/file.txt?alt=media")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("status %d, want 200", resp.StatusCode)
	}
	data, _ := io.ReadAll(resp.Body)
	if string(data) != "deep content" {
		return fmt.Errorf("content %q, want deep content", string(data))
	}
	return nil
}

func gcsLargeObject() error {
	if _, err := gcsPostJSON(base+"/storage/v1/b?project=test", bucketJSON("h-large")); err != nil {
		return err
	}
	content := bytes.Repeat([]byte("x"), 1<<20)
	req, _ := http.NewRequest(http.MethodPost, base+"/upload/storage/v1/b/"+bname("h-large")+"/o?uploadType=media&name=big.bin", bytes.NewReader(content))
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := gcsClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("upload status %d, want 200", resp.StatusCode)
	}
	resp2, _ := gcsClient().Get(base + "/storage/v1/b/" + bname("h-large") + "/o/big.bin?alt=media")
	defer resp2.Body.Close()
	data, _ := io.ReadAll(resp2.Body)
	if len(data) != 1<<20 {
		return fmt.Errorf("downloaded %d bytes, want %d", len(data), 1<<20)
	}
	return nil
}

func gcsErrorEnvelopeFormat() error {
	resp, err := gcsClient().Get(base + "/storage/v1/b/" + bname("h-nonexistent"))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		return fmt.Errorf("status %d, want 404", resp.StatusCode)
	}
	var e gcpError
	json.NewDecoder(resp.Body).Decode(&e)
	if e.Error.Code != 404 {
		return fmt.Errorf("code %d, want 404", e.Error.Code)
	}
	if len(e.Error.Errors) != 1 {
		return fmt.Errorf("expected 1 error detail, got %d", len(e.Error.Errors))
	}
	if e.Error.Errors[0].Reason != "notFound" {
		return fmt.Errorf("reason %q, want notFound", e.Error.Errors[0].Reason)
	}
	if e.Error.Errors[0].Domain != "global" {
		return fmt.Errorf("domain %q, want global", e.Error.Errors[0].Domain)
	}
	return nil
}

func gcsSignedURLGenerate() error {
	if _, err := gcsPostJSON(base+"/storage/v1/b?project=test", bucketJSON("h-sign")); err != nil {
		return err
	}
	if _, err := gcsUpload(bname("h-sign"), "doc.txt", "signed content"); err != nil {
		return err
	}
	body := fmt.Sprintf(`{"bucket":%q,"object":"doc.txt","expires":600}`, bname("h-sign"))
	resp, err := gcsPostJSON(base+"/_localgcp/sign", body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("sign status %d: %s", resp.StatusCode, string(b))
	}
	var result map[string]string
	json.NewDecoder(resp.Body).Decode(&result)
	signedURL := result["signedUrl"]
	if signedURL == "" {
		return fmt.Errorf("empty signedUrl")
	}
	if !strings.Contains(signedURL, "X-Goog-Signature=localgcp") {
		return fmt.Errorf("signed URL missing localgcp marker: %s", signedURL)
	}
	if !strings.Contains(signedURL, "X-Goog-Expires=600") {
		return fmt.Errorf("signed URL missing expires: %s", signedURL)
	}
	return nil
}

func gcsSignedURLDownload() error {
	if _, err := gcsPostJSON(base+"/storage/v1/b?project=test", bucketJSON("h-signdl")); err != nil {
		return err
	}
	if _, err := gcsUpload(bname("h-signdl"), "file.txt", "hello signed"); err != nil {
		return err
	}
	url := base + "/" + bname("h-signdl") + "/file.txt?X-Goog-Signature=localgcp&X-Goog-Expires=3600"
	resp, err := gcsClient().Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("status %d, want 200", resp.StatusCode)
	}
	data, _ := io.ReadAll(resp.Body)
	if string(data) != "hello signed" {
		return fmt.Errorf("content %q, want hello signed", string(data))
	}
	return nil
}

func gcsSmokeEndToEnd() error {
	var errs []string
	addErr := func(step string, err error) {
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", step, err))
		}
	}

	bucket := bname("h-smoke")
	if _, err := gcsPostJSON(base+"/storage/v1/b?project=test", bucketJSON("h-smoke")); err != nil {
		return err
	}
	obj, err := gcsUpload(bucket, "hello.txt", "Hello from localgcp!")
	if err != nil {
		return err
	}
	if obj.Size != "20" {
		errs = append(errs, fmt.Sprintf("size %q, want 20", obj.Size))
	}
	resp, _ := gcsClient().Get(base + "/storage/v1/b/" + bucket + "/o/hello.txt?alt=media")
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		addErr("download", fmt.Errorf("status %d, want 200", resp.StatusCode))
	} else if string(body) != "Hello from localgcp!" {
		addErr("download", fmt.Errorf("content %q", string(body)))
	}
	copyResp, err := gcsPostJSON(base+"/storage/v1/b/"+bucket+"/o/hello.txt/copyTo/b/"+bucket+"/o/hello-copy.txt", "{}")
	if err != nil {
		addErr("copy", err)
	} else if copyResp.StatusCode != 200 {
		cb, _ := io.ReadAll(copyResp.Body)
		copyResp.Body.Close()
		addErr("copy", fmt.Errorf("copyTo status %d (want 200): %s", copyResp.StatusCode, string(cb)))
	} else {
		copyResp.Body.Close()
	}
	resp, _ = gcsClient().Get(base + "/storage/v1/b/" + bucket + "/o")
	var list ObjectList
	json.NewDecoder(resp.Body).Decode(&list)
	resp.Body.Close()
	if len(list.Items) != 2 {
		addErr("list-after-copy", fmt.Errorf("expected 2 objects, got %d", len(list.Items)))
	}
	req, _ := http.NewRequest(http.MethodDelete, base+"/storage/v1/b/"+bucket+"/o/hello.txt", nil)
	resp, _ = gcsClient().Do(req)
	if resp.StatusCode != 204 {
		addErr("delete", fmt.Errorf("status %d, want 204", resp.StatusCode))
	}
	resp.Body.Close()
	resp, _ = gcsClient().Get(base + "/storage/v1/b/" + bucket + "/o/hello.txt?alt=media")
	if resp.StatusCode != 404 {
		addErr("verify-delete", fmt.Errorf("status %d, want 404", resp.StatusCode))
	}
	resp.Body.Close()
	if copyResp != nil && copyResp.StatusCode == 200 {
		resp, _ = gcsClient().Get(base + "/storage/v1/b/" + bucket + "/o/hello-copy.txt?alt=media")
		cb, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			addErr("copy-exists", fmt.Errorf("status %d, want 200", resp.StatusCode))
		} else if string(cb) != "Hello from localgcp!" {
			addErr("copy-exists", fmt.Errorf("content %q", string(cb)))
		}
	}
	big := bytes.Repeat([]byte("X"), 1<<20)
	req, _ = http.NewRequest(http.MethodPost, base+"/upload/storage/v1/b/"+bucket+"/o?uploadType=media&name=big.bin", bytes.NewReader(big))
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, _ = gcsClient().Do(req)
	if resp.StatusCode != 200 {
		addErr("large-upload", fmt.Errorf("status %d, want 200", resp.StatusCode))
	}
	resp.Body.Close()
	resp, _ = gcsClient().Get(base + "/storage/v1/b/" + bucket + "/o/big.bin?alt=media")
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if len(data) != 1<<20 {
		addErr("large-download", fmt.Errorf("got %d bytes, want %d", len(data), 1<<20))
	}
	req, _ = http.NewRequest(http.MethodPost, base+"/upload/storage/v1/b/"+bname("h-no-such")+"/o?uploadType=media&name=fail.txt", strings.NewReader("fail"))
	req.Header.Set("Content-Type", "text/plain")
	resp, _ = gcsClient().Do(req)
	if resp.StatusCode != 404 {
		addErr("missing-bucket-upload", fmt.Errorf("status %d, want 404", resp.StatusCode))
	}
	resp.Body.Close()
	resp, _ = gcsPostJSON(base+"/storage/v1/b?project=test", `{}`)
	if resp.StatusCode != 400 {
		addErr("empty-bucket-name", fmt.Errorf("status %d, want 400", resp.StatusCode))
	}
	resp.Body.Close()

	if len(errs) > 0 {
		return fmt.Errorf("%d diverging step(s): %s", len(errs), strings.Join(errs, "; "))
	}
	return nil
}
