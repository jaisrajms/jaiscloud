package bigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/queryengine"
	bqstore "jaiscloud/internal/gcp/store/bigquery"
	"jaiscloud/internal/model"
)

// invalidQuery is a BigQuery semantic/syntax error: HTTP 400 with
// ErrorProto.reason "invalidQuery" (the codec maps the "InvalidQuery" code to
// that reason). Used for constructs outside the emulator's SQL subset so the
// client never receives a successful-looking wrong result.
func invalidQuery(msg string) error {
	return model.NewProviderError("InvalidQuery", msg, 400)
}

// storeCatalog adapts the BigQuery ResourceStore to the query engine's Catalog:
// a table's extracted schema plus its streamed rows. Rows are decoded with
// json.Number so INT64 values keep full precision.
type storeCatalog struct {
	store bqstore.Store
}

func (c storeCatalog) Table(ctx context.Context, project, dataset, table string) (queryengine.Table, error) {
	t, err := c.store.GetTable(ctx, project, dataset, table)
	if err != nil {
		if errors.Is(err, bqstore.ErrNoSuchTable) {
			return queryengine.Table{}, queryengine.ErrTableNotFound
		}
		return queryengine.Table{}, err
	}
	fields := toQueryFields(parseSchemaFields(t.Schema))
	rows, err := c.store.ListRows(ctx, project, dataset, table)
	if err != nil {
		return queryengine.Table{}, err
	}
	out := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		m := map[string]any{}
		if len(r.Data) > 0 {
			dec := json.NewDecoder(bytes.NewReader(r.Data))
			dec.UseNumber()
			if err := dec.Decode(&m); err != nil {
				return queryengine.Table{}, err
			}
		}
		out = append(out, m)
	}
	return queryengine.Table{Project: project, Dataset: dataset, Table: table, Fields: fields, Rows: out}, nil
}

// --- Mutator (DDL/DML writes) ---

// ListTables lists a dataset's table IDs for the query engine's DROP SCHEMA
// RESTRICT check.
func (c storeCatalog) ListTables(ctx context.Context, project, dataset string) ([]string, error) {
	tables, err := c.store.ListTables(ctx, project, dataset)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(tables))
	for i, t := range tables {
		names[i] = t.TableID
	}
	return names, nil
}

// CreateTable creates a store table with the engine's schema and rows. The
// rows (if any) are streamed through InsertRows so the store's NumRows and
// ordering stay consistent with tabledata.insertAll.
func (c storeCatalog) CreateTable(ctx context.Context, t queryengine.Table) error {
	if _, err := c.store.GetDataset(ctx, t.Project, t.Dataset); err != nil {
		if errors.Is(err, bqstore.ErrNoSuchDataset) {
			return queryengine.ErrDatasetNotFound
		}
		return err
	}
	now := clock.Now().UTC()
	schema, err := schemaJSON(t.Fields)
	if err != nil {
		return err
	}
	config, _ := json.Marshal(map[string]any{
		"kind":           kindPrefix + "table",
		"tableReference": map[string]any{"projectId": t.Project, "datasetId": t.Dataset, "tableId": t.Table},
		"type":           "TABLE",
	})
	st := bqstore.Table{
		DatasetID:  t.Dataset,
		TableID:    t.Table,
		Config:     config,
		Schema:     schema,
		CreateTime: now,
		UpdateTime: now,
	}
	if err := c.store.CreateTable(ctx, t.Project, t.Dataset, st); err != nil {
		if errors.Is(err, bqstore.ErrAlreadyExists) {
			return queryengine.ErrTableExists
		}
		return err
	}
	if len(t.Rows) > 0 {
		rows := make([]bqstore.Row, len(t.Rows))
		for i, r := range t.Rows {
			data, err := json.Marshal(r)
			if err != nil {
				return err
			}
			rows[i] = bqstore.Row{Data: data}
		}
		if _, err := c.store.InsertRows(ctx, t.Project, t.Dataset, t.Table, rows); err != nil {
			return err
		}
	}
	return nil
}

func (c storeCatalog) DropTable(ctx context.Context, project, dataset, table string) error {
	if err := c.store.DeleteTable(ctx, project, dataset, table); err != nil {
		if errors.Is(err, bqstore.ErrNoSuchTable) {
			return queryengine.ErrTableNotFound
		}
		return err
	}
	return nil
}

func (c storeCatalog) CreateDataset(ctx context.Context, project, dataset string) error {
	now := clock.Now().UTC()
	config, _ := json.Marshal(map[string]any{
		"kind":             kindPrefix + "dataset",
		"datasetReference": map[string]any{"projectId": project, "datasetId": dataset},
		"location":         "US",
	})
	d := bqstore.Dataset{DatasetID: dataset, Config: config, CreateTime: now, UpdateTime: now}
	if err := c.store.CreateDataset(ctx, project, d); err != nil {
		if errors.Is(err, bqstore.ErrAlreadyExists) {
			return queryengine.ErrDatasetExists
		}
		return err
	}
	return nil
}

func (c storeCatalog) DropDataset(ctx context.Context, project, dataset string) error {
	if err := c.store.DeleteDataset(ctx, project, dataset); err != nil {
		if errors.Is(err, bqstore.ErrNoSuchDataset) {
			return queryengine.ErrDatasetNotFound
		}
		return err
	}
	return nil
}

func (c storeCatalog) ReplaceRows(ctx context.Context, project, dataset, table string, rows []map[string]any) error {
	st := make([]bqstore.Row, len(rows))
	for i, r := range rows {
		data, err := json.Marshal(r)
		if err != nil {
			return err
		}
		st[i] = bqstore.Row{Data: data}
	}
	if err := c.store.ReplaceRows(ctx, project, dataset, table, st); err != nil {
		if errors.Is(err, bqstore.ErrNoSuchTable) {
			return queryengine.ErrTableNotFound
		}
		return err
	}
	return nil
}

// schemaJSON renders engine fields as the store's table.schema JSON (the wire
// TableFieldSchema shape used by tables.insert).
func schemaJSON(fields []queryengine.Field) (json.RawMessage, error) {
	if len(fields) == 0 {
		return nil, nil
	}
	out := make([]map[string]any, len(fields))
	for i, f := range fields {
		out[i] = map[string]any{
			"name": f.Name,
			"type": wireFieldType(f.Type),
			"mode": modeOr(f.Mode),
		}
	}
	return json.Marshal(map[string]any{"fields": out})
}

func toQueryFields(fields []schemaField) []queryengine.Field {
	out := make([]queryengine.Field, len(fields))
	for i, f := range fields {
		out[i] = queryengine.Field{Name: f.Name, Type: f.Type, Mode: f.Mode}
	}
	return out
}

// runQuery executes a jobs.query request body against the store. The caller has
// already validated that body.query is present and useLegacySql is false.
func (p *Provider) runQuery(ctx context.Context, project string, body map[string]any, dryRun bool) (queryengine.Result, error) {
	defProject, defDataset := "", ""
	if dd := mapValue(body, "defaultDataset"); dd != nil {
		defProject = strValue(dd, "projectId")
		defDataset = strValue(dd, "datasetId")
	}
	res, err := queryengine.ExecuteStatement(ctx, storeCatalog{store: p.store}, queryengine.Request{
		Project:        project,
		Query:          strValue(body, "query"),
		DefaultProject: defProject,
		DefaultDataset: defDataset,
		DryRun:         dryRun,
	})
	if err != nil {
		return queryengine.Result{}, mapQueryEngineErr(err)
	}
	return res, nil
}

func mapQueryEngineErr(err error) error {
	var nf *queryengine.TableNotFoundError
	if errors.As(err, &nf) {
		return model.NewProviderError("NotFound", fmt.Sprintf("Table %s.%s.%s not found", nf.Project, nf.Dataset, nf.Table), 404)
	}
	var dn *queryengine.DatasetNotFoundError
	if errors.As(err, &dn) {
		return model.NewProviderError("NotFound", fmt.Sprintf("Dataset %s.%s not found", dn.Project, dn.Dataset), 404)
	}
	switch {
	case errors.Is(err, queryengine.ErrTableExists), errors.Is(err, queryengine.ErrDatasetExists):
		return model.NewProviderError("AlreadyExists", err.Error(), 409)
	case errors.Is(err, queryengine.ErrTableNotFound), errors.Is(err, queryengine.ErrDatasetNotFound):
		return model.NewProviderError("NotFound", err.Error(), 404)
	}
	var ue *queryengine.UnsupportedError
	if errors.As(err, &ue) {
		return invalidQuery(ue.Reason)
	}
	return model.NewProviderError("Internal", err.Error(), 500)
}

// jobQueryBody extracts the query configuration stored on a job (from either
// jobs.query or jobs.insert). It returns nil when the job carries no query.
func jobQueryBody(j bqstore.Job) map[string]any {
	if len(j.Config) == 0 {
		return nil
	}
	var cfg map[string]any
	if json.Unmarshal(j.Config, &cfg) != nil {
		return nil
	}
	conf, _ := cfg["configuration"].(map[string]any)
	q, _ := conf["query"].(map[string]any)
	return q
}

// jobLocation returns the jobReference.location stored on a job, if any.
func jobLocation(j bqstore.Job) string {
	if len(j.Config) == 0 {
		return ""
	}
	var cfg map[string]any
	if json.Unmarshal(j.Config, &cfg) != nil {
		return ""
	}
	ref, _ := cfg["jobReference"].(map[string]any)
	return strValue(ref, "location")
}

// encodeQueryResult renders an executed result set as the Discovery
// jobs.query (bigquery#queryResponse) or getQueryResults
// (bigquery#getQueryResultsResponse) body.
func encodeQueryResult(kindSuffix, project, jobID, location string, res queryengine.Result, dryRun bool) map[string]any {
	sfields := make([]schemaField, len(res.Fields))
	fields := make([]any, len(res.Fields))
	for i, f := range res.Fields {
		sf := schemaField{Name: f.Name, Type: wireFieldType(f.Type), Mode: modeOr(f.Mode)}
		sfields[i] = sf
		fields[i] = map[string]any{"name": sf.Name, "type": sf.Type, "mode": sf.Mode}
	}
	rows := make([]any, 0, len(res.Rows))
	for _, r := range res.Rows {
		cells := make([]any, len(sfields))
		for i := range sfields {
			cells[i] = tableCell(sfields[i], r[i])
		}
		rows = append(rows, map[string]any{"f": cells})
	}
	ref := map[string]any{"projectId": project, "jobId": jobID}
	if location != "" {
		ref["location"] = location
	}
	out := map[string]any{
		"kind":         kindPrefix + kindSuffix,
		"jobComplete":  true,
		"jobReference": ref,
	}
	// statementType is declared on QueryResponse (jobs.query); the
	// getQueryResults response does not model it (JobStatistics2 does).
	if res.StatementType != "" && kindSuffix == "queryResponse" {
		out["statementType"] = res.StatementType
	}
	if isDMLStatement(res.StatementType) {
		// DML returns no schema/rows; only the affected-row count. A dry run
		// affects nothing, so it omits the count too.
		if !dryRun {
			out["numDmlAffectedRows"] = strconv.FormatInt(res.NumDMLAffectedRows, 10)
		}
	} else if res.StatementType != "" && res.StatementType != queryengine.StatementSelect {
		// DDL: no result set (the schema/rows are empty by contract).
	} else {
		out["schema"] = map[string]any{"fields": fields}
		out["rows"] = rows
		out["totalRows"] = strconv.Itoa(len(res.Rows))
	}
	return out
}

// isDMLStatement reports whether a statementType carries numDmlAffectedRows
// (INSERT/UPDATE/DELETE per the Discovery QueryResponse docs).
func isDMLStatement(typ string) bool {
	switch typ {
	case queryengine.StatementInsert, queryengine.StatementUpdate, queryengine.StatementDelete:
		return true
	}
	return false
}

// wireFieldType maps an engine/BigQuery type to the name the Discovery
// TableFieldSchema.type uses on the wire.
func wireFieldType(t string) string {
	switch strings.ToUpper(t) {
	case "INT64", "INTEGER":
		return "INTEGER"
	case "FLOAT64", "FLOAT":
		return "FLOAT"
	case "BOOL", "BOOLEAN":
		return "BOOLEAN"
	case "STRUCT", "RECORD":
		return "RECORD"
	default:
		return strings.ToUpper(t)
	}
}

func modeOr(m string) string {
	if m == "" {
		return "NULLABLE"
	}
	return strings.ToUpper(m)
}
