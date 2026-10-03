package rescatalog

import "testing"

func TestLookupGlobal(t *testing.T) {
	d, ok := Lookup("global")
	if !ok {
		t.Fatal("global descriptor missing from the catalog")
	}
	if d.DisplayName() != "Global" {
		t.Fatalf("global display name = %q, want Global", d.DisplayName())
	}
	labels := d.Labels()
	if len(labels) != 1 || labels[0].Key != "project_id" || labels[0].ValueType != "STRING" {
		t.Fatalf("global labels = %+v, want a single project_id STRING label", labels)
	}
}
