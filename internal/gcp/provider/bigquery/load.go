package bigquery

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"

	bqstore "jaiscloud/internal/gcp/store/bigquery"
	"jaiscloud/internal/gcp/store/gcs"
	"jaiscloud/internal/model"
)

// SourceReader resolves the plaintext bytes of GCS objects named by a load
// job's gs:// source URIs, and lists the objects a wildcard URI can match. It is
// implemented by the GCS provider (which owns object encryption/decryption) and
// injected by main.go; a nil reader disables load jobs (an explicit Unimplemented
// rather than a silent no-op), so the BigQuery provider never imports the
// storage provider.
type SourceReader interface {
	FetchObjectBytes(ctx context.Context, bucket, object string) ([]byte, error)
	// ListObjectNames returns the live object names in bucket whose name has the
	// given prefix, sorted ascending. An absent bucket yields no names (the
	// memory backend reports gcs.ErrNoSuchBucket; the Postgres backend an empty
	// list) — both are treated as "no match" by the caller.
	ListObjectNames(ctx context.Context, bucket, prefix string) ([]string, error)
}

// Option configures a Provider.
type Option func(*Provider)

// WithSourceReader wires the GCS object reader that resolves gs:// load sources.
func WithSourceReader(r SourceReader) Option {
	return func(p *Provider) { p.sourceReader = r }
}

// Source formats the load subset understands. DATASTORE_BACKUP and the
// self-describing ORC format fail loud — they are unscheduled follow-up work.
const (
	loadFormatNDJSON  = "NEWLINE_DELIMITED_JSON"
	loadFormatCSV     = "CSV"
	loadFormatParquet = "PARQUET"
	loadFormatAvro    = "AVRO"
)

// isSelfDescribingFormat reports whether a format carries its own schema, so an
// explicit schema is optional and `autodetect` is ignored (as in real BigQuery).
func isSelfDescribingFormat(format string) bool {
	return format == loadFormatParquet || format == loadFormatAvro
}

// maxLoadLineBytes caps a single NDJSON record; a longer line is a decode error
// rather than an unbounded allocation.
const maxLoadLineBytes = 16 << 20

// autodetectSampleRows caps how many rows autodetect reads before committing to
// an inferred schema.
const autodetectSampleRows = 100

// loadRecords is a decoded source: the rows that passed schema coercion and the
// records that did not (each with a human-readable reason). bad records are
// tolerated up to configuration.load.maxBadRecords.
type loadRecords struct {
	rows []map[string]any
	bad  []string
}

// loadSource is one decoded source file: its gs:// URI (empty for an uploaded
// media body) and the plaintext bytes.
type loadSource struct {
	uri  string
	data []byte
}

// runLoad evaluates a configuration.load job whose source is one or more gs://
// objects: it resolves each URI (a single object or a single-`*` wildcard)
// through the injected GCS reader and delegates the decode/coercion/write to
// executeLoad.
func (p *Provider) runLoad(ctx context.Context, project string, load map[string]any, now time.Time) (stats map[string]any, jobErr map[string]any, err error) {
	if p.sourceReader == nil {
		return nil, nil, model.NewProviderError("Unimplemented", "load jobs are not enabled (no GCS source reader configured)", 501)
	}
	// Validate the documented subset before touching the source so an
	// unsupported option fails loud regardless of the source's state.
	if err := validateLoadOptions(load); err != nil {
		return nil, nil, err
	}

	uris := stringSlice(load, "sourceUris")
	if len(uris) == 0 {
		return nil, nil, invalidArgument("configuration.load.sourceUris is required")
	}

	type ref struct{ bucket, object string }
	var refs []ref
	for _, uri := range uris {
		bucket, object, isGlob, perr := parseGSUri(uri)
		if perr != nil {
			return nil, nil, invalidArgument(perr.Error())
		}
		if !isGlob {
			refs = append(refs, ref{bucket, object})
			continue
		}
		names, lerr := p.sourceReader.ListObjectNames(ctx, bucket, globPrefix(object))
		if lerr != nil {
			if errors.Is(lerr, gcs.ErrNoSuchBucket) {
				return zeroLoadStats(), notFound("Not found: URI " + uri), nil
			}
			return nil, nil, model.NewProviderError("Internal", "failed to list source "+bucket+": "+lerr.Error(), 500)
		}
		matched := make([]string, 0, len(names))
		for _, n := range names {
			if globMatch(object, n) {
				matched = append(matched, n)
			}
		}
		if len(matched) == 0 {
			return zeroLoadStats(), notFound("Not found: URI " + uri), nil
		}
		sort.Strings(matched)
		for _, n := range matched {
			refs = append(refs, ref{bucket, n})
		}
	}

	sources := make([]loadSource, 0, len(refs))
	for _, rf := range refs {
		data, ferr := p.sourceReader.FetchObjectBytes(ctx, rf.bucket, rf.object)
		if ferr != nil {
			// A missing object is a job-level notFound (real BigQuery accepts the
			// job then fails it); any other read failure is a real server error.
			if errors.Is(ferr, gcs.ErrNoSuchObject) {
				return zeroLoadStats(), notFound("Not found: URI " + gsURI(rf.bucket, rf.object)), nil
			}
			return nil, nil, model.NewProviderError("Internal", "failed to read source "+gsURI(rf.bucket, rf.object)+": "+ferr.Error(), 500)
		}
		sources = append(sources, loadSource{uri: gsURI(rf.bucket, rf.object), data: data})
	}
	return p.executeLoad(ctx, project, load, sources, strconv.Itoa(len(sources)), now)
}

// zeroLoadStats is the statistics.load map for a job that failed before (or
// without) reading any records.
func zeroLoadStats() map[string]any {
	return map[string]any{"inputFiles": "0", "inputFileBytes": "0", "outputRows": "0", "badRecords": "0"}
}

// runLoadData evaluates a configuration.load job against an in-memory source —
// the file bytes supplied as an uploaded media body. inputFiles is the
// statistics.load.inputFiles value the caller reports ("0" for a media upload,
// which names no sourceUris).
func (p *Provider) runLoadData(ctx context.Context, project string, load map[string]any, data []byte, inputFiles string, now time.Time) (stats map[string]any, jobErr map[string]any, err error) {
	return p.executeLoad(ctx, project, load, []loadSource{{data: data}}, inputFiles, now)
}

// executeLoad resolves the destination schema, decodes/coerces every source,
// and writes the result per writeDisposition. inputFiles is the
// statistics.load.inputFiles value: the number of resolved source objects for a
// gs:// load, or "0" for a media upload (which names no sourceUris). Nothing is
// written until every validation has passed, so a failed load never creates the
// destination table (real BigQuery's create/truncate/append is one atomic action
// on job completion).
func (p *Provider) executeLoad(ctx context.Context, project string, load map[string]any, sources []loadSource, inputFiles string, now time.Time) (stats map[string]any, jobErr map[string]any, err error) {
	if err := validateLoadOptions(load); err != nil {
		return nil, nil, err
	}

	dest := mapValue(load, "destinationTable")
	datasetID := strValue(dest, "datasetId")
	tableID := strValue(dest, "tableId")
	if datasetID == "" || tableID == "" {
		return nil, nil, invalidArgument("configuration.load.destinationTable.datasetId and tableId are required")
	}

	format := strings.ToUpper(strValue(load, "sourceFormat"))
	if format == "" {
		format = loadFormatCSV // Discovery default
	}
	switch format {
	case loadFormatNDJSON, loadFormatCSV, loadFormatParquet, loadFormatAvro:
	default:
		return nil, nil, model.NewProviderError("Unimplemented",
			"configuration.load.sourceFormat "+format+" is not supported", 501)
	}

	fields := schemaFieldsFromMap(mapValue(load, "schema"))

	t, terr := p.store.GetTable(ctx, project, datasetID, tableID)
	exists := true
	var tableFields []schemaField
	switch {
	case terr == nil:
		tableFields = parseSchemaFields(t.Schema)
		if len(fields) == 0 {
			fields = tableFields
		}
	case errors.Is(terr, bqstore.ErrNoSuchTable):
		exists = false
		if strings.EqualFold(strValue(load, "createDisposition"), "CREATE_NEVER") {
			// Real BigQuery: the table must already exist, else a 'notFound'
			// error is returned in the job result.
			return zeroLoadStats(), notFound(fmt.Sprintf("Not found: Table %s:%s.%s", project, datasetID, tableID)), nil
		}
	default:
		return nil, nil, mapErr(terr)
	}

	// Resolve a schema when neither the request nor the destination table
	// supplied one: the self-describing formats take it from the file, the
	// plain-text formats need autodetect.
	if len(fields) == 0 {
		switch {
		case isSelfDescribingFormat(format):
			derived, derr := describeSource(format, load, sources)
			if derr != nil {
				return nil, nil, derr
			}
			fields = derived
		case boolValue(load, "autodetect"):
			derived, derr := autodetectFields(format, load, sources)
			if derr != nil {
				return nil, nil, derr
			}
			fields = derived
		default:
			return nil, nil, invalidArgument("configuration.load.schema is required to create the destination table (enable autodetect for CSV/JSON)")
		}
		if len(fields) == 0 {
			return nil, nil, invalidArgument("could not determine a schema for the load source")
		}
	}

	disp := strings.ToUpper(strValue(load, "writeDisposition"))
	if disp == "" {
		disp = "WRITE_APPEND"
	}
	switch disp {
	case "WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY":
	default:
		return nil, nil, invalidArgument("configuration.load.writeDisposition " + disp + " is not supported")
	}

	maxBad, merr := intOption(load, "maxBadRecords")
	if merr != nil {
		return nil, nil, merr
	}

	var rows []map[string]any
	var bad []string
	totalBytes := int64(0)
	for _, src := range sources {
		totalBytes += int64(len(src.data))
		recs, derr := decodeLoadRecords(format, load, fields, src.data)
		if derr != nil {
			return nil, nil, derr
		}
		rows = append(rows, recs.rows...)
		for _, b := range recs.bad {
			bad = append(bad, qualifyBad(src.uri, b))
		}
	}

	stats = map[string]any{
		"inputFiles":     inputFiles,
		"inputFileBytes": strconv.FormatInt(totalBytes, 10),
		"outputRows":     strconv.FormatInt(int64(len(rows)), 10),
		"badRecords":     strconv.FormatInt(int64(len(bad)), 10),
	}
	if len(bad) > maxBad {
		return stats, map[string]any{
			"reason":  "invalid",
			"message": fmt.Sprintf("processing encountered too many bad records (%d > %d): %s", len(bad), maxBad, bad[0]),
		}, nil
	}
	if disp == "WRITE_EMPTY" && exists && t.NumRows > 0 {
		return stats, map[string]any{
			"reason":  "duplicate",
			"message": "Cannot use WRITE_EMPTY disposition with a non-empty table.",
		}, nil
	}

	// Validation passed: create the destination table (if needed) and write.
	schemaPatched := false
	var schemaRestore []byte
	if !exists {
		if _, derr := p.store.GetDataset(ctx, project, datasetID); derr != nil {
			return nil, nil, mapErr(derr)
		}
		nt := bqstore.Table{DatasetID: datasetID, TableID: tableID, CreateTime: now, UpdateTime: now}
		if raw, merr := json.Marshal(fieldsToSchema(fields)); merr == nil {
			nt.Schema = raw
		}
		if cerr := p.store.CreateTable(ctx, project, datasetID, nt); cerr != nil {
			return nil, nil, mapErr(cerr)
		}
		t = nt
	} else if len(tableFields) == 0 && len(fields) > 0 {
		// The destination existed without a schema (e.g. a Parquet/Avro load
		// whose schema came from the file); persist it so clients can decode the
		// rows that are about to be written. UpdateTableAtomic only rewrites the
		// schema, leaving a concurrent row-count change intact; it is restored
		// below if the write fails.
		if raw, merr := json.Marshal(fieldsToSchema(fields)); merr == nil {
			schemaRestore = t.Schema
			if _, uerr := p.store.UpdateTableAtomic(ctx, project, datasetID, tableID, func(tbl bqstore.Table) (bqstore.Table, error) {
				tbl.Schema = raw
				return tbl, nil
			}); uerr != nil {
				return nil, nil, mapErr(uerr)
			}
			schemaPatched = true
		}
	}

	out := make([]bqstore.Row, 0, len(rows))
	for _, obj := range rows {
		raw, merr := json.Marshal(obj)
		if merr != nil {
			return nil, nil, model.NewProviderError("Internal", "failed to encode loaded row", 500)
		}
		out = append(out, bqstore.Row{Data: raw})
	}
	var werr error
	if disp == "WRITE_TRUNCATE" {
		werr = p.store.ReplaceRows(ctx, project, datasetID, tableID, out)
	} else if len(out) > 0 {
		_, werr = p.store.InsertRows(ctx, project, datasetID, tableID, out)
	}
	if werr != nil {
		// Roll back a table this call created so a failed write leaves no trace.
		if !exists {
			_ = p.store.DeleteTable(ctx, project, datasetID, tableID)
		} else if schemaPatched {
			_, _ = p.store.UpdateTableAtomic(ctx, project, datasetID, tableID, func(tbl bqstore.Table) (bqstore.Table, error) {
				tbl.Schema = schemaRestore
				return tbl, nil
			})
		}
		return nil, nil, mapErr(werr)
	}
	return stats, nil, nil
}

// fieldsToSchema wraps fields in the {"fields": [...]} TableSchema object that
// is persisted on a table's schema column.
func fieldsToSchema(fields []schemaField) map[string]any {
	return map[string]any{"fields": fields}
}

// qualifyBad prefixes a per-record decode failure with its source URI so a
// bad-record report from a wildcard/multi-file load names the offending object.
func qualifyBad(uri, reason string) string {
	if uri == "" {
		return reason
	}
	return uri + ": " + reason
}

// notFound builds a status.errorResult for a job-level not-found failure.
func notFound(message string) map[string]any {
	return map[string]any{"reason": "notFound", "message": message}
}

// gsURI renders a bucket/object pair as a gs:// URI.
func gsURI(bucket, object string) string { return "gs://" + bucket + "/" + object }

// validateLoadOptions rejects load options the emulator does not model, so a
// client never gets a silently-simplified load. The options the emulator acts
// on are: sourceFormat, sourceUris (objects, a single-`*` wildcard, or a list),
// destinationTable, schema, autodetect, compression (CSV/JSON only),
// writeDisposition, createDisposition, skipLeadingRows, fieldDelimiter,
// nullMarker, maxBadRecords and ignoreUnknownValues. The quote character is only
// supported at its default `"` (the Go standard library cannot express another);
// everything listed here fails loud.
func validateLoadOptions(load map[string]any) error {
	unsupported := func(msg string) error { return model.NewProviderError("Unimplemented", msg, 501) }
	if boolValue(load, "allowJaggedRows") {
		return unsupported("configuration.load.allowJaggedRows is not supported")
	}
	if enc := strings.ToUpper(strValue(load, "encoding")); enc != "" && enc != "UTF-8" {
		return unsupported("configuration.load.encoding " + enc + " is not supported (UTF-8 only)")
	}
	if q, ok := load["quote"]; ok {
		if s, _ := q.(string); s != `"` {
			return unsupported(`configuration.load.quote is only supported as the default '"'`)
		}
	}
	if len(stringSlice(load, "schemaUpdateOptions")) > 0 {
		return unsupported("configuration.load.schemaUpdateOptions is not supported")
	}
	if mapValue(load, "timePartitioning") != nil || mapValue(load, "rangePartitioning") != nil ||
		mapValue(load, "clustering") != nil || mapValue(load, "hivePartitioningOptions") != nil {
		return unsupported("configuration.load partitioning/clustering is not supported")
	}
	for _, k := range []string{
		"dateFormat", "timeFormat", "datetimeFormat", "timestampFormat",
		"sourceColumnMatch", "referenceFileSchemaUri", "schemaInline",
	} {
		if strValue(load, k) != "" {
			return unsupported("configuration.load." + k + " is not supported")
		}
	}
	if len(stringSlice(load, "decimalTargetTypes")) > 0 || len(stringSlice(load, "nullMarkers")) > 0 {
		return unsupported("configuration.load decimalTargetTypes/nullMarkers is not supported")
	}
	if boolValue(load, "preserveAsciiControlCharacters") {
		return unsupported("configuration.load.preserveAsciiControlCharacters is not supported")
	}
	if ext := strings.ToUpper(strValue(load, "jsonExtension")); ext != "" && ext != "NONE" {
		return unsupported("configuration.load.jsonExtension " + ext + " is not supported")
	}
	return nil
}

// decodeLoadRecords decodes the source bytes into schema-coerced rows plus the
// list of records that failed. A non-nil error is a request-level failure (an
// unusable delimiter, an unreadable stream), never a per-record failure.
func decodeLoadRecords(format string, load map[string]any, fields []schemaField, data []byte) (*loadRecords, error) {
	switch format {
	case loadFormatNDJSON:
		dec, err := decompressCSVOrJSON(data)
		if err != nil {
			return nil, err
		}
		return decodeNDJSON(load, fields, dec)
	case loadFormatCSV:
		dec, err := decompressCSVOrJSON(data)
		if err != nil {
			return nil, err
		}
		return decodeCSV(load, fields, dec)
	case loadFormatParquet:
		return decodeParquet(load, fields, data)
	case loadFormatAvro:
		return decodeAvro(load, fields, data)
	}
	return nil, model.NewProviderError("Unimplemented", "unsupported sourceFormat "+format, 501)
}

// decompressCSVOrJSON transparently decompresses a gzip-wrapped CSV/JSON
// source. Real BigQuery has no `compression` field on JobConfigurationLoad — it
// auto-detects gzip — so the emulator detects the gzip magic bytes rather than
// requiring an option.
func decompressCSVOrJSON(data []byte) ([]byte, error) {
	if len(data) < 2 || data[0] != 0x1f || data[1] != 0x8b {
		return data, nil
	}
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, invalidArgument("failed to read gzip source: " + err.Error())
	}
	defer zr.Close()
	out, err := io.ReadAll(zr)
	if err != nil {
		return nil, invalidArgument("failed to decompress gzip source: " + err.Error())
	}
	return out, nil
}

// decodeNDJSON reads one JSON object per line. A malformed line, a missing
// REQUIRED field or an unknown field (unless ignoreUnknownValues) is a bad
// record. Numbers decode as json.Number so INTEGERs beyond 2^53 survive.
func decodeNDJSON(load map[string]any, fields []schemaField, data []byte) (*loadRecords, error) {
	ignoreUnknown := boolValue(load, "ignoreUnknownValues")
	res := &loadRecords{}
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 0, 64*1024), maxLoadLineBytes)
	line := 0
	for sc.Scan() {
		line++
		text := strings.TrimSpace(sc.Text())
		if text == "" {
			continue
		}
		dec := json.NewDecoder(strings.NewReader(text))
		dec.UseNumber()
		var obj map[string]any
		if err := dec.Decode(&obj); err != nil {
			res.bad = append(res.bad, fmt.Sprintf("line %d: %v", line, err))
			continue
		}
		row, err := coerceRow(fields, obj, ignoreUnknown)
		if err != nil {
			res.bad = append(res.bad, fmt.Sprintf("line %d: %v", line, err))
			continue
		}
		res.rows = append(res.rows, row)
	}
	if err := sc.Err(); err != nil {
		return nil, invalidArgument("failed to read source: " + err.Error())
	}
	return res, nil
}

// newRawCSVReader builds the csv.Reader for a CSV load with only the
// fieldDelimiter applied (skipLeadingRows is handled by the caller).
func newRawCSVReader(load map[string]any, data []byte) (*csv.Reader, error) {
	r := csv.NewReader(bytes.NewReader(data))
	r.FieldsPerRecord = -1
	if d := strValue(load, "fieldDelimiter"); d != "" {
		runes := []rune(d)
		if len(runes) != 1 {
			return nil, invalidArgument("configuration.load.fieldDelimiter must be a single character")
		}
		r.Comma = runes[0]
	}
	return r, nil
}

// newCSVReader builds the csv.Reader for a CSV load: it applies fieldDelimiter
// and skips skipLeadingRows header/leading records.
func newCSVReader(load map[string]any, data []byte) (*csv.Reader, int, error) {
	r, err := newRawCSVReader(load, data)
	if err != nil {
		return nil, 0, err
	}
	skip, err := intOption(load, "skipLeadingRows")
	if err != nil {
		return nil, 0, err
	}
	for i := 0; i < skip; i++ {
		if _, err := r.Read(); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, 0, invalidArgument("failed to read leading rows: " + err.Error())
		}
	}
	return r, skip, nil
}

// decodeCSV reads comma-separated (or fieldDelimiter-separated) records mapped
// positionally onto the schema fields. Nested/repeated fields, jagged rows and
// a non-default quote are rejected — the standard library cannot express them.
func decodeCSV(load map[string]any, fields []schemaField, data []byte) (*loadRecords, error) {
	r, skip, err := newCSVReader(load, data)
	if err != nil {
		return nil, err
	}
	// The default null marker is the empty string; a custom one is honored by
	// csvRow (where an empty field then means "error or empty string", not NULL).
	marker, _ := load["nullMarker"].(string)

	res := &loadRecords{}
	line := skip
	for {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		line++
		if err != nil {
			res.bad = append(res.bad, fmt.Sprintf("row %d: %v", line, err))
			continue
		}
		if len(rec) != len(fields) {
			res.bad = append(res.bad, fmt.Sprintf("row %d: expected %d columns, got %d", line, len(fields), len(rec)))
			continue
		}
		obj, err := csvRow(fields, rec, marker)
		if err != nil {
			res.bad = append(res.bad, fmt.Sprintf("row %d: %v", line, err))
			continue
		}
		res.rows = append(res.rows, obj)
	}
	return res, nil
}

// csvRow maps one CSV record positionally onto fields. The default null marker
// is the empty string (empty field => NULL). With a custom marker an empty
// field is an error for every type except STRING/BYTES (which take the empty
// value), matching the Discovery nullMarker description.
func csvRow(fields []schemaField, rec []string, nullMarker string) (map[string]any, error) {
	obj := make(map[string]any, len(fields))
	for i, f := range fields {
		if strings.EqualFold(f.Mode, "REPEATED") || isRecordType(f.Type) {
			return nil, fmt.Errorf("nested or repeated field %q cannot be loaded from CSV", f.Name)
		}
		raw := rec[i]
		if raw == nullMarker {
			if strings.EqualFold(f.Mode, "REQUIRED") {
				return nil, fmt.Errorf("missing required field %q", f.Name)
			}
			obj[f.Name] = nil
			continue
		}
		if raw == "" {
			if isStringColumn(f.Type) {
				obj[f.Name] = ""
				continue
			}
			return nil, fmt.Errorf("field %q: empty value is not allowed when a custom nullMarker is set", f.Name)
		}
		v, err := coerceScalar(f, raw)
		if err != nil {
			return nil, err
		}
		obj[f.Name] = v
	}
	return obj, nil
}

// isStringColumn reports whether a type takes an empty string as an empty value
// (rather than an error) under a custom nullMarker.
func isStringColumn(t string) bool {
	switch strings.ToUpper(t) {
	case "STRING", "BYTES":
		return true
	}
	return false
}

// coerceRow projects a decoded JSON object onto the schema (column names are
// case-insensitive, as in BigQuery): known fields are coerced to their declared
// type, absent optional fields are omitted, and unknown fields are rejected
// unless ignoreUnknown.
func coerceRow(fields []schemaField, raw map[string]any, ignoreUnknown bool) (map[string]any, error) {
	byLower := make(map[string]any, len(raw))
	for k, v := range raw {
		byLower[strings.ToLower(k)] = v
	}
	out := make(map[string]any, len(fields))
	for _, f := range fields {
		v, ok := byLower[strings.ToLower(f.Name)]
		if !ok || v == nil {
			if strings.EqualFold(f.Mode, "REQUIRED") {
				return nil, fmt.Errorf("missing required field %q", f.Name)
			}
			continue
		}
		cv, err := coerceValue(f, v)
		if err != nil {
			return nil, err
		}
		out[f.Name] = cv
	}
	if !ignoreUnknown {
		known := make(map[string]bool, len(fields))
		for _, f := range fields {
			known[strings.ToLower(f.Name)] = true
		}
		for k := range raw {
			if !known[strings.ToLower(k)] {
				return nil, fmt.Errorf("no such field: %s", k)
			}
		}
	}
	return out, nil
}

// coerceValue coerces a decoded JSON value to the field's type, recursing into
// REPEATED elements and RECORD fields.
func coerceValue(f schemaField, v any) (any, error) {
	if strings.EqualFold(f.Mode, "REPEATED") {
		arr, ok := v.([]any)
		if !ok {
			return nil, fmt.Errorf("field %q is REPEATED but the value is not an array", f.Name)
		}
		elem := f
		elem.Mode = "NULLABLE"
		out := make([]any, 0, len(arr))
		for _, e := range arr {
			cv, err := coerceValue(elem, e)
			if err != nil {
				return nil, err
			}
			out = append(out, cv)
		}
		return out, nil
	}
	if isRecordType(f.Type) {
		obj, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("field %q is a RECORD but the value is not an object", f.Name)
		}
		if len(f.Fields) == 0 {
			return obj, nil
		}
		return coerceRow(f.Fields, obj, false)
	}
	return coerceScalar(f, v)
}

// coerceScalar coerces a single scalar to the field's declared type. Strings
// (CSV fields, and lenient JSON) are parsed for numeric/boolean fields; JSON
// numbers and the native Go numeric types produced by the Parquet/Avro decoders
// are accepted for INTEGER/FLOAT; time.Time values are rendered for the temporal
// types; []byte is base64-encoded for BYTES; every other scalar renders to its
// canonical string for the string-like types. Non-finite floats are rejected —
// the JSON store cannot represent them.
func coerceScalar(f schemaField, v any) (any, error) {
	typ := strings.ToUpper(f.Type)
	if s, ok := v.(string); ok {
		return coerceScalarString(f, typ, s)
	}
	switch typ {
	case "INTEGER", "INT64":
		if n, ok := int64Value(v); ok {
			return n, nil
		}
		return nil, fmt.Errorf("field %q: value is not an INTEGER", f.Name)
	case "FLOAT", "FLOAT64":
		if fl, ok := float64Value(v); ok && !math.IsNaN(fl) && !math.IsInf(fl, 0) {
			return fl, nil
		}
		return nil, fmt.Errorf("field %q: value is not a finite FLOAT", f.Name)
	case "BOOLEAN", "BOOL":
		if b, ok := v.(bool); ok {
			return b, nil
		}
		return nil, fmt.Errorf("field %q: value is not a BOOLEAN", f.Name)
	case "BYTES":
		if b, ok := v.([]byte); ok {
			return base64.StdEncoding.EncodeToString(b), nil
		}
		return scalarString(v), nil
	case "DATE", "TIME", "TIMESTAMP", "DATETIME":
		if ts, ok := v.(time.Time); ok {
			return formatTemporal(typ, ts), nil
		}
		if d, ok := v.(time.Duration); ok {
			return formatDuration(d), nil
		}
		return scalarString(v), nil
	default:
		return scalarString(v), nil
	}
}

// coerceScalarString coerces a string value (CSV field or lenient JSON string).
func coerceScalarString(f schemaField, typ, s string) (any, error) {
	trimmed := strings.TrimSpace(s)
	switch typ {
	case "INTEGER", "INT64":
		n, err := strconv.ParseInt(trimmed, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("field %q: %q is not an INTEGER", f.Name, s)
		}
		return n, nil
	case "FLOAT", "FLOAT64":
		fl, err := strconv.ParseFloat(trimmed, 64)
		if err != nil || math.IsNaN(fl) || math.IsInf(fl, 0) {
			return nil, fmt.Errorf("field %q: %q is not a finite FLOAT", f.Name, s)
		}
		return fl, nil
	case "BOOLEAN", "BOOL":
		switch strings.ToLower(trimmed) {
		case "true":
			return true, nil
		case "false":
			return false, nil
		}
		return nil, fmt.Errorf("field %q: %q is not a BOOLEAN", f.Name, s)
	default:
		return s, nil
	}
}

// int64Value extracts an integer from the Go numeric types the decoders produce
// (json.Number, float64, int64/int32/int, and the unsigned types) and from
// integral floats.
func int64Value(v any) (int64, bool) {
	switch x := v.(type) {
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i, true
		}
		if fl, err := x.Float64(); err == nil && fl == math.Trunc(fl) && !math.IsInf(fl, 0) {
			return int64(fl), true
		}
	case int64:
		return x, true
	case int32:
		return int64(x), true
	case int:
		return int64(x), true
	case int16:
		return int64(x), true
	case int8:
		return int64(x), true
	case uint64:
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint:
		return int64(x), true
	case float64:
		if x == math.Trunc(x) && !math.IsNaN(x) && !math.IsInf(x, 0) {
			return int64(x), true
		}
	case float32:
		if float64(x) == math.Trunc(float64(x)) && !math.IsNaN(float64(x)) && !math.IsInf(float64(x), 0) {
			return int64(x), true
		}
	}
	return 0, false
}

// float64Value extracts a float from the Go numeric types the decoders produce.
func float64Value(v any) (float64, bool) {
	switch x := v.(type) {
	case json.Number:
		if fl, err := x.Float64(); err == nil {
			return fl, true
		}
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int64:
		return float64(x), true
	case int32:
		return float64(x), true
	case int:
		return float64(x), true
	case uint64:
		return float64(x), true
	case uint32:
		return float64(x), true
	}
	return 0, false
}

// formatTemporal renders a time.Time for a BigQuery temporal field's canonical
// string form.
func formatTemporal(typ string, t time.Time) string {
	switch typ {
	case "DATE":
		return t.UTC().Format("2006-01-02")
	case "TIME":
		return formatDuration(time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute + time.Duration(t.Second())*time.Second + time.Duration(t.Nanosecond()))
	case "DATETIME":
		return trimFraction(t.UTC().Format("2006-01-02T15:04:05.000000000"))
	case "TIMESTAMP":
		return trimFraction(t.UTC().Format("2006-01-02 15:04:05.000000000")) + " UTC"
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// formatDuration renders a duration as a BigQuery TIME string (HH:MM:SS[.frac]).
func formatDuration(d time.Duration) string {
	d = d % (24 * time.Hour)
	if d < 0 {
		d += 24 * time.Hour
	}
	base := time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC).Add(d)
	return trimFraction(base.Format("15:04:05.000000000"))
}

// trimFraction removes trailing zeros (and a bare trailing dot) from a
// fractional-seconds formatted string.
func trimFraction(s string) string {
	if i := strings.IndexByte(s, '.'); i >= 0 {
		s = strings.TrimRight(s, "0")
		s = strings.TrimSuffix(s, ".")
	}
	return s
}

// scalarString renders a scalar as the string a string-like BigQuery field
// stores (numbers without an exponent where possible).
func scalarString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 32)
	case int64:
		return strconv.FormatInt(x, 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int:
		return strconv.Itoa(x)
	case json.Number:
		return x.String()
	case []byte:
		return base64.StdEncoding.EncodeToString(x)
	default:
		if b, err := json.Marshal(v); err == nil {
			return string(b)
		}
		return fmt.Sprintf("%v", v)
	}
}

// parseGSUri splits a gs:// source URI into bucket and object. The object may
// contain a single `*` wildcard (globbing); `?`/`[` and a bucket-only URI are
// rejected. isGlob reports whether the object is a wildcard pattern.
func parseGSUri(uri string) (bucket, object string, isGlob bool, err error) {
	const scheme = "gs://"
	if !strings.HasPrefix(uri, scheme) {
		return "", "", false, fmt.Errorf("source URI %q must use the gs:// scheme", uri)
	}
	rest := uri[len(scheme):]
	i := strings.IndexByte(rest, '/')
	if i <= 0 || i == len(rest)-1 {
		return "", "", false, fmt.Errorf("source URI %q must be gs://bucket/object", uri)
	}
	bucket, object = rest[:i], rest[i+1:]
	if strings.ContainsAny(object, "?[") {
		return "", "", false, fmt.Errorf("source URI %q uses an unsupported wildcard (only '*' is allowed)", uri)
	}
	if strings.Contains(bucket, "*") {
		return "", "", false, fmt.Errorf("source URI %q places the wildcard in the bucket name (it must follow the bucket)", uri)
	}
	if strings.Count(object, "*") > 1 {
		return "", "", false, fmt.Errorf("source URI %q may contain at most one '*' wildcard", uri)
	}
	return bucket, object, strings.Contains(object, "*"), nil
}

// globPrefix returns the literal prefix of a wildcard object pattern, used to
// narrow the object listing.
func globPrefix(pattern string) string {
	if i := strings.IndexByte(pattern, '*'); i >= 0 {
		return pattern[:i]
	}
	return pattern
}

// globMatch reports whether name matches a pattern containing at most one `*`,
// which matches any sequence of characters (including `/`), as in BigQuery's
// gs:// URI globbing.
func globMatch(pattern, name string) bool {
	i := strings.IndexByte(pattern, '*')
	if i < 0 {
		return pattern == name
	}
	prefix, suffix := pattern[:i], pattern[i+1:]
	return len(name) >= len(prefix)+len(suffix) &&
		strings.HasPrefix(name, prefix) && strings.HasSuffix(name, suffix)
}

// schemaFieldsFromMap extracts the top-level fields from a TableSchema object
// (configuration.load.schema). A nil/unparseable schema yields nil.
func schemaFieldsFromMap(m map[string]any) []schemaField {
	if m == nil {
		return nil
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return nil
	}
	return parseSchemaFields(raw)
}

// stringSlice reads a []any of strings (sourceUris, schemaUpdateOptions),
// ignoring non-string entries.
func stringSlice(m map[string]any, key string) []string {
	raw, _ := m[key].([]any)
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// intOption reads an integer option. The Discovery document types
// skipLeadingRows/maxBadRecords as int32 (a JSON number); a numeric string is
// also accepted leniently. Absent/empty is 0; negative or non-integer is
// InvalidArgument.
func intOption(m map[string]any, key string) (int, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return 0, nil
	}
	switch x := v.(type) {
	case string:
		if strings.TrimSpace(x) == "" {
			return 0, nil
		}
		n, err := strconv.Atoi(strings.TrimSpace(x))
		if err != nil || n < 0 {
			return 0, invalidArgument(key + " must be a non-negative integer")
		}
		return n, nil
	case float64:
		if x < 0 || x != math.Trunc(x) {
			return 0, invalidArgument(key + " must be a non-negative integer")
		}
		return int(x), nil
	case json.Number:
		n, err := x.Int64()
		if err != nil || n < 0 {
			return 0, invalidArgument(key + " must be a non-negative integer")
		}
		return int(n), nil
	}
	return 0, invalidArgument(key + " must be a non-negative integer")
}

// ─── Parquet ─────────────────────────────────────────────────────────────────

// decodeParquet decodes a Parquet file into schema-coerced rows. The Parquet
// schema is mapped to BigQuery types (unsupported shapes fail loud before any
// row is read); each row is reconstructed generically and its logical-typed
// values are normalized to their canonical string form.
func decodeParquet(load map[string]any, fields []schemaField, data []byte) (*loadRecords, error) {
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, invalidArgument("failed to read Parquet source: " + err.Error())
	}
	nodes, err := parquetNodes(f.Schema().Fields())
	if err != nil {
		return nil, err
	}
	ignore := boolValue(load, "ignoreUnknownValues")
	rdr := parquet.NewReader(f)
	defer rdr.Close()
	res := &loadRecords{}
	row := 0
	for {
		raw := map[string]any{}
		rerr := rdr.Read(&raw)
		if errors.Is(rerr, io.EOF) {
			break
		}
		row++
		if rerr != nil {
			return nil, invalidArgument(fmt.Sprintf("failed to read Parquet row %d: %v", row, rerr))
		}
		obj := make(map[string]any, len(nodes))
		var rerr2 error
		for _, n := range nodes {
			v, ok := raw[n.bq.Name]
			if !ok {
				continue
			}
			nv, nerr := n.normalize(v)
			if nerr != nil {
				rerr2 = nerr
				break
			}
			obj[n.bq.Name] = nv
		}
		if rerr2 != nil {
			res.bad = append(res.bad, fmt.Sprintf("row %d: %v", row, rerr2))
			continue
		}
		cv, cerr := coerceRow(fields, obj, ignore)
		if cerr != nil {
			res.bad = append(res.bad, fmt.Sprintf("row %d: %v", row, cerr))
			continue
		}
		res.rows = append(res.rows, cv)
	}
	return res, nil
}

// describeSource derives a BigQuery schema from the first source file of a
// self-describing format (Parquet/Avro).
func describeSource(format string, load map[string]any, sources []loadSource) ([]schemaField, error) {
	if len(sources) == 0 {
		return nil, invalidArgument("no load source to read a schema from")
	}
	switch format {
	case loadFormatParquet:
		f, err := parquet.OpenFile(bytes.NewReader(sources[0].data), int64(len(sources[0].data)))
		if err != nil {
			return nil, invalidArgument("failed to read Parquet source: " + err.Error())
		}
		nodes, err := parquetNodes(f.Schema().Fields())
		if err != nil {
			return nil, err
		}
		return nodesSchema(nodes), nil
	case loadFormatAvro:
		schema, err := avroSourceSchema(sources[0].data)
		if err != nil {
			return nil, err
		}
		return avroSchemaFields(schema, boolValue(load, "useAvroLogicalTypes"))
	}
	return nil, model.NewProviderError("Unimplemented", "sourceFormat "+format+" is not self-describing", 501)
}

// pqNode is a normalized view of one Parquet schema node: its BigQuery field
// shape plus the conversion needed to turn the decoded Go value into a
// BigQuery-compatible value.
type pqNode struct {
	bq       schemaField
	scalar   func(any) (any, error)
	fields   map[string]*pqNode
	repeated bool
	elem     *pqNode
}

// normalize converts a decoded Parquet value (recursing for RECORD/REPEATED).
func (n *pqNode) normalize(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	if n.repeated {
		arr, ok := v.([]any)
		if !ok {
			return nil, fmt.Errorf("field %q: expected an array, got %T", n.bq.Name, v)
		}
		out := make([]any, 0, len(arr))
		for _, e := range arr {
			nv, err := n.elem.normalizeValue(e)
			if err != nil {
				return nil, err
			}
			out = append(out, nv)
		}
		return out, nil
	}
	return n.normalizeValue(v)
}

func (n *pqNode) normalizeValue(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	if len(n.fields) > 0 {
		m, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("field %q: expected an object, got %T", n.bq.Name, v)
		}
		out := make(map[string]any, len(m))
		for k, cv := range m {
			child, ok := n.fields[strings.ToLower(k)]
			if !ok {
				out[k] = cv
				continue
			}
			nv, err := child.normalize(cv)
			if err != nil {
				return nil, err
			}
			out[child.bq.Name] = nv
		}
		return out, nil
	}
	if n.scalar != nil {
		return n.scalar(v)
	}
	return v, nil
}

func nodesSchema(nodes []*pqNode) []schemaField {
	out := make([]schemaField, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.bq)
	}
	return out
}

func nodeChildMap(nodes []*pqNode) map[string]*pqNode {
	m := make(map[string]*pqNode, len(nodes))
	for _, n := range nodes {
		m[strings.ToLower(n.bq.Name)] = n
	}
	return m
}

// parquetNodes maps a Parquet field list to normalized nodes, failing loud on
// shapes the subset cannot represent (MAP, VARIANT, GEOMETRY, DECIMAL, ...).
func parquetNodes(fields []parquet.Field) ([]*pqNode, error) {
	out := make([]*pqNode, 0, len(fields))
	for _, f := range fields {
		n, err := parquetNode(f)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}

func parquetNode(f parquet.Field) (*pqNode, error) {
	name := f.Name()
	if !f.Leaf() {
		lt := f.Type().LogicalType()
		if lt != nil {
			switch {
			case lt.List != nil:
				kids := f.Fields()
				if len(kids) != 1 {
					return nil, unsupportedParquetShape("LIST", name)
				}
				elems := kids[0].Fields()
				if len(elems) != 1 {
					return nil, unsupportedParquetShape("LIST", name)
				}
				elem, err := parquetNode(elems[0])
				if err != nil {
					return nil, err
				}
				n := &pqNode{bq: elem.bq, repeated: true, elem: elem}
				n.bq.Mode = "REPEATED"
				n.bq.Name = name
				return n, nil
			case lt.Map != nil:
				return nil, unsupportedParquetShape("MAP", name)
			case lt.Variant != nil:
				return nil, unsupportedParquetShape("VARIANT", name)
			case lt.Geometry != nil:
				return nil, unsupportedParquetShape("GEOMETRY", name)
			case lt.Geography != nil:
				return nil, unsupportedParquetShape("GEOGRAPHY", name)
			}
		}
		kids := f.Fields()
		if len(kids) == 0 {
			return nil, unsupportedParquetShape("empty group", name)
		}
		children, err := parquetNodes(kids)
		if err != nil {
			return nil, err
		}
		sf := schemaField{Name: name, Type: "RECORD", Mode: parquetMode(f)}
		if f.Repeated() {
			// A legacy repeated group without the LIST logical type: treat it as a
			// REPEATED RECORD rather than silently dropping the repetition.
			sf.Mode = "REPEATED"
			for _, c := range children {
				sf.Fields = append(sf.Fields, c.bq)
			}
			elem := &pqNode{bq: sf, fields: nodeChildMap(children)}
			elem.bq.Mode = "NULLABLE"
			return &pqNode{bq: sf, repeated: true, elem: elem}, nil
		}
		for _, c := range children {
			sf.Fields = append(sf.Fields, c.bq)
		}
		return &pqNode{bq: sf, fields: nodeChildMap(children)}, nil
	}
	leaf, err := parquetLeaf(f)
	if err != nil {
		return nil, err
	}
	if f.Repeated() {
		n := &pqNode{bq: leaf.bq, repeated: true, elem: leaf}
		n.bq.Mode = "REPEATED"
		return n, nil
	}
	return leaf, nil
}

func parquetLeaf(f parquet.Field) (*pqNode, error) {
	bqType, conv, err := parquetType(f.Type())
	if err != nil {
		return nil, fmt.Errorf("Parquet field %q: %w", f.Name(), err)
	}
	sf := schemaField{Name: f.Name(), Type: bqType, Mode: parquetMode(f)}
	return &pqNode{bq: sf, scalar: conv}, nil
}

func parquetMode(f parquet.Field) string {
	if f.Required() {
		return "REQUIRED"
	}
	return "NULLABLE"
}

func unsupportedParquetShape(shape, name string) error {
	return model.NewProviderError("Unimplemented",
		"Parquet "+shape+" field "+strconv.Quote(name)+" is not supported", 501)
}

// parquetType maps a Parquet leaf type (with its logical type) to a BigQuery
// type and the conversion of a decoded value to a BigQuery-compatible value.
func parquetType(t parquet.Type) (string, func(any) (any, error), error) {
	if lt := t.LogicalType(); lt != nil {
		switch {
		case lt.Date != nil:
			return "DATE", parquetDate, nil
		case lt.Time != nil:
			factor, err := parquetTimeFactor(lt.Time.Unit)
			if err != nil {
				return "", nil, err
			}
			return "TIME", func(v any) (any, error) { return parquetTime(v, factor) }, nil
		case lt.Timestamp != nil:
			factor, err := parquetTimeFactor(lt.Timestamp.Unit)
			if err != nil {
				return "", nil, err
			}
			if !lt.Timestamp.IsAdjustedToUTC {
				// A local (non-UTC-adjusted) timestamp is a BigQuery DATETIME.
				return "DATETIME", func(v any) (any, error) { return parquetDatetime(v, factor) }, nil
			}
			return "TIMESTAMP", func(v any) (any, error) { return parquetTimestamp(v, factor) }, nil
		case lt.Decimal != nil:
			return "", nil, fmt.Errorf("DECIMAL is not supported")
		case lt.UTF8 != nil, lt.Enum != nil, lt.Json != nil, lt.UUID != nil:
			return "STRING", nil, nil
		case lt.Bson != nil:
			return "BYTES", nil, nil
		case lt.Integer != nil:
			return "INTEGER", nil, nil
		case lt.Float16 != nil:
			return "FLOAT", nil, nil
		case lt.Unknown != nil:
			return "", nil, fmt.Errorf("NULL type is not supported")
		}
	}
	switch t.Kind() {
	case parquet.Boolean:
		return "BOOLEAN", nil, nil
	case parquet.Int32, parquet.Int64:
		return "INTEGER", nil, nil
	case parquet.Float, parquet.Double:
		return "FLOAT", nil, nil
	case parquet.ByteArray, parquet.FixedLenByteArray:
		return "BYTES", nil, nil
	}
	return "", nil, fmt.Errorf("type %s is not supported", t)
}

// parquetTimeFactor returns the nanoseconds-per-unit factor for a Parquet time
// or timestamp unit.
func parquetTimeFactor(u format.TimeUnit) (int64, error) {
	switch {
	case u.Millis != nil:
		return int64(time.Millisecond), nil
	case u.Micros != nil:
		return int64(time.Microsecond), nil
	case u.Nanos != nil:
		return int64(time.Nanosecond), nil
	}
	return 0, fmt.Errorf("unknown time unit")
}

// parquetDate converts a Parquet DATE (days since the Unix epoch) to a BigQuery
// DATE string.
func parquetDate(v any) (any, error) {
	n, ok := int64Value(v)
	if !ok {
		return nil, fmt.Errorf("DATE value is not an integer")
	}
	return time.Unix(n*86400, 0).UTC().Format("2006-01-02"), nil
}

// parquetTime converts a Parquet TIME (a count of units since midnight) to a
// BigQuery TIME string.
func parquetTime(v any, factor int64) (any, error) {
	n, ok := int64Value(v)
	if !ok {
		return nil, fmt.Errorf("TIME value is not an integer")
	}
	return formatDuration(time.Duration(n) * time.Duration(factor)), nil
}

// parquetTimestamp converts a Parquet TIMESTAMP (a count of units since the
// Unix epoch) to a BigQuery TIMESTAMP string.
func parquetTimestamp(v any, factor int64) (any, error) {
	n, ok := int64Value(v)
	if !ok {
		return nil, fmt.Errorf("TIMESTAMP value is not an integer")
	}
	ts := time.Unix(0, n*factor).UTC()
	return trimFraction(ts.Format("2006-01-02 15:04:05.000000000")) + " UTC", nil
}

// parquetDatetime converts a Parquet local (non-UTC-adjusted) TIMESTAMP to a
// BigQuery DATETIME string.
func parquetDatetime(v any, factor int64) (any, error) {
	n, ok := int64Value(v)
	if !ok {
		return nil, fmt.Errorf("TIMESTAMP value is not an integer")
	}
	ts := time.Unix(0, n*factor).UTC()
	return trimFraction(ts.Format("2006-01-02T15:04:05.000000000")), nil
}

// ─── Avro ────────────────────────────────────────────────────────────────────

// decodeAvro decodes an Avro Object Container File into schema-coerced rows.
// useAvroLogicalTypes controls whether logical types (date/time/timestamp) are
// interpreted as BigQuery DATE/TIME/TIMESTAMP/DATETIME (true) or left as their
// raw integer base types (false) — matching the Discovery default of false.
func decodeAvro(load map[string]any, fields []schemaField, data []byte) (*loadRecords, error) {
	useLogical := boolValue(load, "useAvroLogicalTypes")
	// Validate the embedded schema's shapes first so an unsupported field (map,
	// decimal, multi-type union) fails loud even when an explicit schema was
	// supplied.
	schema, serr := avroSourceSchema(data)
	if serr != nil {
		return nil, serr
	}
	if _, aerr := avroSchemaFields(schema, useLogical); aerr != nil {
		return nil, aerr
	}
	dec, err := ocf.NewDecoder(bytes.NewReader(data))
	if err != nil {
		return nil, invalidArgument("failed to read Avro source: " + err.Error())
	}
	ignore := boolValue(load, "ignoreUnknownValues")
	res := &loadRecords{}
	row := 0
	for dec.HasNext() {
		row++
		var rec map[string]any
		if err := dec.Decode(&rec); err != nil {
			res.bad = append(res.bad, fmt.Sprintf("row %d: %v", row, err))
			continue
		}
		if !useLogical {
			raw, rerr := avroRawify(schema, rec)
			if rerr != nil {
				res.bad = append(res.bad, fmt.Sprintf("row %d: %v", row, rerr))
				continue
			}
			if m, ok := raw.(map[string]any); ok {
				rec = m
			}
		}
		obj, cerr := coerceRow(fields, rec, ignore)
		if cerr != nil {
			res.bad = append(res.bad, fmt.Sprintf("row %d: %v", row, cerr))
			continue
		}
		res.rows = append(res.rows, obj)
	}
	if err := dec.Error(); err != nil {
		return nil, invalidArgument("failed to read Avro source: " + err.Error())
	}
	return res, nil
}

// avroSourceSchema parses the writer schema embedded in an Avro container file.
func avroSourceSchema(data []byte) (avro.Schema, error) {
	dec, err := ocf.NewDecoder(bytes.NewReader(data))
	if err != nil {
		return nil, invalidArgument("failed to read Avro source: " + err.Error())
	}
	raw, ok := dec.Metadata()["avro.schema"]
	if !ok {
		return nil, invalidArgument("Avro source has no embedded schema")
	}
	schema, err := avro.Parse(string(raw))
	if err != nil {
		return nil, invalidArgument("failed to parse Avro schema: " + err.Error())
	}
	return schema, nil
}

// avroSchemaFields maps an Avro record schema to BigQuery fields, failing loud
// on shapes the subset cannot represent (map, decimal, duration, multi-type
// unions). useLogical selects whether logical types become DATE/TIME/TIMESTAMP/
// DATETIME or their raw integer base types (the Discovery default is false).
func avroSchemaFields(schema avro.Schema, useLogical bool) ([]schemaField, error) {
	rs, ok := schema.(*avro.RecordSchema)
	if !ok {
		return nil, invalidArgument("Avro source must contain records")
	}
	out := make([]schemaField, 0, len(rs.Fields()))
	for _, f := range rs.Fields() {
		sf, err := avroField(f.Name(), f.Type(), useLogical)
		if err != nil {
			return nil, err
		}
		out = append(out, sf)
	}
	return out, nil
}

func avroField(name string, s avro.Schema, useLogical bool) (schemaField, error) {
	if u, ok := s.(*avro.UnionSchema); ok {
		var nonNull []avro.Schema
		for _, t := range u.Types() {
			if t.Type() == avro.Null {
				continue
			}
			nonNull = append(nonNull, t)
		}
		if len(nonNull) != 1 {
			return schemaField{}, model.NewProviderError("Unimplemented",
				"Avro field "+strconv.Quote(name)+" has a union with more than one non-null type, which is not supported", 501)
		}
		sf, err := avroField(name, nonNull[0], useLogical)
		if err != nil {
			return schemaField{}, err
		}
		if !strings.EqualFold(sf.Mode, "REPEATED") {
			sf.Mode = "NULLABLE"
		}
		return sf, nil
	}
	if ls, ok := s.(avro.LogicalTypeSchema); ok {
		if lt := ls.Logical(); lt != nil {
			switch lt.Type() {
			case avro.Date, avro.TimeMillis, avro.TimeMicros,
				avro.TimestampMillis, avro.TimestampMicros,
				avro.LocalTimestampMillis, avro.LocalTimestampMicros:
				if !useLogical {
					return avroBaseField(name, s)
				}
				switch lt.Type() {
				case avro.Date:
					return schemaField{Name: name, Type: "DATE", Mode: "NULLABLE"}, nil
				case avro.TimeMillis, avro.TimeMicros:
					return schemaField{Name: name, Type: "TIME", Mode: "NULLABLE"}, nil
				case avro.TimestampMillis, avro.TimestampMicros:
					return schemaField{Name: name, Type: "TIMESTAMP", Mode: "NULLABLE"}, nil
				default:
					return schemaField{Name: name, Type: "DATETIME", Mode: "NULLABLE"}, nil
				}
			case avro.UUID:
				return schemaField{Name: name, Type: "STRING", Mode: "NULLABLE"}, nil
			case avro.Decimal:
				return schemaField{}, avroUnsupported(name, "DECIMAL")
			case avro.Duration:
				return schemaField{}, avroUnsupported(name, "DURATION")
			}
		}
	}
	switch t := s.(type) {
	case *avro.RecordSchema:
		sub, err := avroSchemaFields(s, useLogical)
		if err != nil {
			return schemaField{}, err
		}
		return schemaField{Name: name, Type: "RECORD", Mode: "NULLABLE", Fields: sub}, nil
	case *avro.EnumSchema:
		return schemaField{Name: name, Type: "STRING", Mode: "NULLABLE"}, nil
	case *avro.ArraySchema:
		elem, err := avroField(name, t.Items(), useLogical)
		if err != nil {
			return schemaField{}, err
		}
		elem.Mode = "REPEATED"
		return elem, nil
	case *avro.MapSchema:
		return schemaField{}, avroUnsupported(name, "MAP")
	case *avro.FixedSchema:
		return schemaField{Name: name, Type: "BYTES", Mode: "NULLABLE"}, nil
	case *avro.PrimitiveSchema:
		switch t.Type() {
		case avro.Boolean:
			return schemaField{Name: name, Type: "BOOLEAN", Mode: "NULLABLE"}, nil
		case avro.Int, avro.Long:
			return schemaField{Name: name, Type: "INTEGER", Mode: "NULLABLE"}, nil
		case avro.Float, avro.Double:
			return schemaField{Name: name, Type: "FLOAT", Mode: "NULLABLE"}, nil
		case avro.String, avro.Bytes:
			if t.Type() == avro.String {
				return schemaField{Name: name, Type: "STRING", Mode: "NULLABLE"}, nil
			}
			return schemaField{Name: name, Type: "BYTES", Mode: "NULLABLE"}, nil
		}
	}
	return schemaField{}, model.NewProviderError("Unimplemented",
		"Avro field "+strconv.Quote(name)+" has an unsupported type ("+string(s.Type())+")", 501)
}

// avroBaseField maps a logical-typed Avro field to the BigQuery type of its raw
// underlying primitive (used when useAvroLogicalTypes is false).
func avroBaseField(name string, s avro.Schema) (schemaField, error) {
	ps, ok := s.(*avro.PrimitiveSchema)
	if !ok {
		return schemaField{}, avroUnsupported(name, "logical type on a non-primitive")
	}
	switch ps.Type() {
	case avro.Int, avro.Long:
		return schemaField{Name: name, Type: "INTEGER", Mode: "NULLABLE"}, nil
	case avro.Bytes:
		return schemaField{Name: name, Type: "BYTES", Mode: "NULLABLE"}, nil
	case avro.String:
		return schemaField{Name: name, Type: "STRING", Mode: "NULLABLE"}, nil
	}
	return schemaField{}, avroUnsupported(name, "logical type "+string(ps.Type()))
}

// avroRawify converts hamba's logical-typed Go values (time.Time, time.Duration)
// back to the raw Avro base values when useAvroLogicalTypes is false, walking
// the writer schema.
func avroRawify(schema avro.Schema, v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	if u, ok := schema.(*avro.UnionSchema); ok {
		var nonNull avro.Schema
		for _, t := range u.Types() {
			if t.Type() != avro.Null {
				nonNull = t
				break
			}
		}
		if nonNull == nil {
			return v, nil
		}
		return avroRawify(nonNull, v)
	}
	if ls, ok := schema.(avro.LogicalTypeSchema); ok {
		if lt := ls.Logical(); lt != nil {
			switch lt.Type() {
			case avro.Date:
				if t, ok := v.(time.Time); ok {
					return int32(t.Unix() / 86400), nil
				}
			case avro.TimeMillis:
				if d, ok := v.(time.Duration); ok {
					return int32(d.Milliseconds()), nil
				}
			case avro.TimeMicros:
				if d, ok := v.(time.Duration); ok {
					return d.Microseconds(), nil
				}
			case avro.TimestampMillis, avro.LocalTimestampMillis:
				if t, ok := v.(time.Time); ok {
					return t.UnixMilli(), nil
				}
			case avro.TimestampMicros, avro.LocalTimestampMicros:
				if t, ok := v.(time.Time); ok {
					return t.UnixMicro(), nil
				}
			}
			return v, nil
		}
	}
	switch t := schema.(type) {
	case *avro.RecordSchema:
		m, ok := v.(map[string]any)
		if !ok {
			return v, nil
		}
		out := make(map[string]any, len(m))
		for k, fv := range m {
			out[k] = fv
		}
		for _, f := range t.Fields() {
			fv, ok := m[f.Name()]
			if !ok {
				continue
			}
			nv, err := avroRawify(f.Type(), fv)
			if err != nil {
				return nil, err
			}
			out[f.Name()] = nv
		}
		return out, nil
	case *avro.ArraySchema:
		arr, ok := v.([]any)
		if !ok {
			return v, nil
		}
		out := make([]any, len(arr))
		for i, e := range arr {
			nv, err := avroRawify(t.Items(), e)
			if err != nil {
				return nil, err
			}
			out[i] = nv
		}
		return out, nil
	}
	return v, nil
}

func avroUnsupported(name, shape string) error {
	return model.NewProviderError("Unimplemented",
		"Avro "+shape+" field "+strconv.Quote(name)+" is not supported", 501)
}

// ─── Autodetect ──────────────────────────────────────────────────────────────

// autodetectFields infers a schema for the plain-text formats (CSV/NDJSON) by
// sampling the sources, matching real BigQuery's autodetect scope.
func autodetectFields(format string, load map[string]any, sources []loadSource) ([]schemaField, error) {
	switch format {
	case loadFormatNDJSON:
		return autodetectNDJSON(load, sources)
	case loadFormatCSV:
		return autodetectCSV(load, sources)
	}
	return nil, model.NewProviderError("Unimplemented", "autodetect is not supported for "+format, 501)
}

// columnInfer accumulates the inferred BigQuery type of one column.
type columnInfer struct {
	name    string
	typ     string
	sawNull bool
	sawAny  bool
}

func (c *columnInfer) observe(v any) {
	if v == nil {
		c.sawNull = true
		return
	}
	c.sawAny = true
	t := inferType(v)
	c.typ = mergeInferred(c.typ, t)
}

func inferType(v any) string {
	switch x := v.(type) {
	case bool:
		return "BOOLEAN"
	case json.Number:
		if _, err := x.Int64(); err == nil {
			return "INTEGER"
		}
		return "FLOAT"
	case float64, float32, int64, int32, int:
		if _, ok := int64Value(v); ok {
			return "INTEGER"
		}
		return "FLOAT"
	case string:
		return inferStringType(x)
	}
	return "STRING"
}

func inferStringType(s string) string {
	t := strings.TrimSpace(s)
	if t == "" {
		return ""
	}
	if _, err := strconv.ParseInt(t, 10, 64); err == nil {
		return "INTEGER"
	}
	if _, err := strconv.ParseFloat(t, 64); err == nil {
		return "FLOAT"
	}
	switch strings.ToLower(t) {
	case "true", "false":
		return "BOOLEAN"
	}
	if _, err := time.Parse("2006-01-02", t); err == nil {
		return "DATE"
	}
	if _, err := time.Parse("15:04:05", t); err == nil {
		return "TIME"
	}
	if _, err := time.Parse(time.RFC3339, t); err == nil {
		return "TIMESTAMP"
	}
	if _, err := time.Parse("2006-01-02 15:04:05", t); err == nil {
		return "TIMESTAMP"
	}
	return "STRING"
}

// mergeInferred combines two inferred column types. An unknown (from an empty
// value) defers to the other; INTEGER+FLOAT widen to FLOAT; anything else mixed
// degrades to STRING.
func mergeInferred(a, b string) string {
	switch {
	case a == "":
		return b
	case b == "":
		return a
	case a == b:
		return a
	case (a == "INTEGER" && b == "FLOAT") || (a == "FLOAT" && b == "INTEGER"):
		return "FLOAT"
	default:
		return "STRING"
	}
}

func (c *columnInfer) field() schemaField {
	typ := c.typ
	if typ == "" {
		typ = "STRING"
	}
	return schemaField{Name: c.name, Type: typ, Mode: "NULLABLE"}
}

// autodetectNDJSON unions the keys of the sampled JSON objects (in the order
// they first appear in the first sampled object) and infers each key's type.
func autodetectNDJSON(load map[string]any, sources []loadSource) ([]schemaField, error) {
	var cols []*columnInfer
	byName := map[string]*columnInfer{}
	rows := 0
	for _, src := range sources {
		data, err := decompressCSVOrJSON(src.data)
		if err != nil {
			return nil, err
		}
		sc := bufio.NewScanner(bytes.NewReader(data))
		sc.Buffer(make([]byte, 0, 64*1024), maxLoadLineBytes)
		for sc.Scan() && rows < autodetectSampleRows {
			text := strings.TrimSpace(sc.Text())
			if text == "" {
				continue
			}
			dec := json.NewDecoder(strings.NewReader(text))
			dec.UseNumber()
			var obj map[string]any
			if err := dec.Decode(&obj); err != nil {
				continue
			}
			rows++
			if len(cols) == 0 {
				// Preserve the first object's key order for stable column order
				// (Go maps lose it).
				keys, kerr := jsonObjectKeyOrder(text)
				if kerr != nil {
					for k := range obj {
						keys = append(keys, k)
					}
					sort.Strings(keys)
				}
				for _, k := range keys {
					col := &columnInfer{name: k}
					byName[k] = col
					cols = append(cols, col)
					col.observe(obj[k])
				}
				continue
			}
			// Union keys that first appear after the first sampled row, so a
			// later row's extra column becomes part of the inferred schema (in
			// encounter order), as in real BigQuery.
			for k, v := range obj {
				col, ok := byName[k]
				if !ok {
					col = &columnInfer{name: k}
					byName[k] = col
					cols = append(cols, col)
				}
				col.observe(v)
			}
		}
		if err := sc.Err(); err != nil {
			return nil, invalidArgument("failed to read source: " + err.Error())
		}
		if rows >= autodetectSampleRows {
			break
		}
	}
	if len(cols) == 0 {
		return nil, invalidArgument("autodetect found no rows to infer a schema from")
	}
	out := make([]schemaField, 0, len(cols))
	for _, c := range cols {
		out = append(out, c.field())
	}
	return out, nil
}

// jsonObjectKeyOrder returns the top-level keys of a JSON object in document
// order. Nested objects/arrays are skipped.
func jsonObjectKeyOrder(text string) ([]string, error) {
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, errors.New("not a JSON object")
	}
	var keys []string
	depth := 0
	expectKey := true
	for {
		tok, err := dec.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return keys, nil
			}
			return nil, err
		}
		if d, ok := tok.(json.Delim); ok {
			switch d {
			case '{', '[':
				depth++
			case '}', ']':
				if depth == 0 {
					return keys, nil // end of the root object
				}
				depth--
			}
			if depth == 0 {
				expectKey = true
			}
			continue
		}
		if depth == 0 {
			if expectKey {
				if s, ok := tok.(string); ok {
					keys = append(keys, s)
				}
				expectKey = false
			} else {
				expectKey = true
			}
		}
	}
}

// autodetectCSV infers a schema from the sampled CSV records. With
// skipLeadingRows > 0 the first skipped record supplies the column names;
// otherwise columns are named string_field_<i>, matching real BigQuery.
func autodetectCSV(load map[string]any, sources []loadSource) ([]schemaField, error) {
	skip, err := intOption(load, "skipLeadingRows")
	if err != nil {
		return nil, err
	}
	var cols []*columnInfer
	rows := 0
	for _, src := range sources {
		data, err := decompressCSVOrJSON(src.data)
		if err != nil {
			return nil, err
		}
		r, err := newRawCSVReader(load, data)
		if err != nil {
			return nil, err
		}
		// Consume the leading rows; when a header is declared (skip > 0) the
		// first one names the columns.
		for i := 0; i < skip; i++ {
			rec, rerr := r.Read()
			if errors.Is(rerr, io.EOF) {
				break
			}
			if rerr != nil {
				return nil, invalidArgument("failed to read CSV source: " + rerr.Error())
			}
			if i == 0 && len(cols) == 0 {
				cols = make([]*columnInfer, len(rec))
				for j := range rec {
					cols[j] = &columnInfer{name: rec[j]}
				}
			}
		}
		for rows < autodetectSampleRows {
			rec, rerr := r.Read()
			if errors.Is(rerr, io.EOF) {
				break
			}
			if rerr != nil {
				return nil, invalidArgument("failed to read CSV source: " + rerr.Error())
			}
			if len(cols) == 0 {
				// No declared header: name the columns by position.
				cols = make([]*columnInfer, len(rec))
				for j := range rec {
					cols[j] = &columnInfer{name: fmt.Sprintf("string_field_%d", j)}
				}
			}
			for j, v := range rec {
				if j < len(cols) {
					cols[j].observe(v)
				}
			}
			rows++
		}
		if rows >= autodetectSampleRows {
			break
		}
	}
	if len(cols) == 0 {
		return nil, invalidArgument("autodetect found no records to infer a schema from")
	}
	out := make([]schemaField, 0, len(cols))
	for _, c := range cols {
		out = append(out, c.field())
	}
	return out, nil
}
