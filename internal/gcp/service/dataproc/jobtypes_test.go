package dataproc

import (
	"testing"

	"jaiscloud/internal/sparkhelpers"
)

func TestJobToEntryPoint_SparkJob(t *testing.T) {
	ep, _, err := jobToEntryPoint("sparkJob", map[string]any{
		"mainJarFileUri": "gs://b/a.jar",
		"mainClass":      "Main",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	jar, ok := ep.(sparkhelpers.JarEntryPoint)
	if !ok || jar.JarURI != "gs://b/a.jar" || jar.MainClass != "Main" {
		t.Fatalf("unexpected entrypoint: %+v", ep)
	}
}

func TestJobToEntryPoint_PySparkJob(t *testing.T) {
	ep, _, err := jobToEntryPoint("pysparkJob", map[string]any{
		"mainPythonFileUri": "gs://b/main.py",
		"pythonFileUris":    []any{"gs://b/dep.py"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	py, ok := ep.(sparkhelpers.PythonEntryPoint)
	if !ok || py.MainPythonFile != "gs://b/main.py" || len(py.PyFiles) != 1 {
		t.Fatalf("unexpected entrypoint: %+v", ep)
	}
}

func TestJobToEntryPoint_SparkRJob(t *testing.T) {
	ep, _, err := jobToEntryPoint("sparkRJob", map[string]any{"mainRFileUri": "gs://b/main.R"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r, ok := ep.(sparkhelpers.REntryPoint)
	if !ok || r.MainRFile != "gs://b/main.R" {
		t.Fatalf("unexpected entrypoint: %+v", ep)
	}
}

func TestJobToEntryPoint_SparkSqlJob(t *testing.T) {
	ep, _, err := jobToEntryPoint("sparkSqlJob", map[string]any{
		"queryList":       map[string]any{"queries": []any{"SELECT 1", "SELECT 2"}},
		"jarFileUris":     []any{"gs://b/dep.jar"},
		"scriptVariables": map[string]any{"k": "v"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sql, ok := ep.(sparkhelpers.SqlEntryPoint)
	if !ok {
		t.Fatalf("unexpected entrypoint type: %T", ep)
	}
	if len(sql.Queries) != 2 || sql.Queries[0] != "SELECT 1" {
		t.Fatalf("queries = %v", sql.Queries)
	}
	if sql.FileURI != "" {
		t.Fatalf("FileURI = %q, want empty", sql.FileURI)
	}
	if len(sql.JarFileURIs) != 1 || sql.JarFileURIs[0] != "gs://b/dep.jar" {
		t.Fatalf("jarFileUris = %v", sql.JarFileURIs)
	}
	if sql.HiveVars["k"] != "v" {
		t.Fatalf("scriptVariables = %v", sql.HiveVars)
	}
}

func TestJobToEntryPoint_SparkSqlJob_FileURI(t *testing.T) {
	ep, _, err := jobToEntryPoint("sparkSqlJob", map[string]any{"queryFileUri": "gs://b/q.sql"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sql, ok := ep.(sparkhelpers.SqlEntryPoint)
	if !ok || sql.FileURI != "gs://b/q.sql" || len(sql.Queries) != 0 {
		t.Fatalf("unexpected entrypoint: %+v", ep)
	}
}

func TestJobToEntryPoint_SparkSqlJob_RequiresQuery(t *testing.T) {
	if _, _, err := jobToEntryPoint("sparkSqlJob", map[string]any{}); err == nil {
		t.Fatal("expected error for sparkSqlJob without queryFileUri or queryList.queries")
	}
}

func TestJobToEntryPoint_SparkSqlJob_RejectsBoth(t *testing.T) {
	if _, _, err := jobToEntryPoint("sparkSqlJob", map[string]any{
		"queryFileUri": "gs://b/q.sql",
		"queryList":    map[string]any{"queries": []any{"SELECT 1"}},
	}); err == nil {
		t.Fatal("expected error when both queryFileUri and queryList are set (proto oneof)")
	}
}

func TestJobToEntryPoint_SparkSqlJob_RejectsEmptyQueries(t *testing.T) {
	if _, _, err := jobToEntryPoint("sparkSqlJob", map[string]any{
		"queryList": map[string]any{"queries": []any{}},
	}); err == nil {
		t.Fatal("expected error for an empty queryList.queries")
	}
}

func TestJobToEntryPoint_Unsupported(t *testing.T) {
	for _, jobType := range []string{"hadoopJob", "hiveJob", "pigJob", "prestoJob", "trinoJob", "flinkJob"} {
		if _, _, err := jobToEntryPoint(jobType, map[string]any{}); err == nil {
			t.Fatalf("expected error for %s", jobType)
		}
	}
}

func TestExtractJobType(t *testing.T) {
	jobType, typeJob := extractJobType(map[string]any{
		"labels": map[string]any{"k": "v"},
		"pysparkJob": map[string]any{
			"mainPythonFileUri": "gs://b/main.py",
		},
	})
	if jobType != "pysparkJob" || typeJob == nil {
		t.Fatalf("extractJobType: %q %v", jobType, typeJob)
	}
}
