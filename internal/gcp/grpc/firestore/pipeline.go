package firestore

import (
	"context"
	"fmt"
	"sort"
	"strings"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"jaiscloud/internal/clock"
	firestoreprovider "jaiscloud/internal/gcp/provider/firestore"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/model"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// ExecutePipeline implements the server-streaming Firestore.ExecutePipeline RPC
// over the existing transport-agnostic query engine.
//
// The pinned proto models the pipeline as an open-ended ordered list of stages
// whose arguments are arbitrary Value expression trees (the pipeline DSL). The
// emulator implements the read-only relational subset needed to express a
// collection / collection-group / database / document-set source followed by
// projection, filtering, ordering and cardinality stages:
//
//	source stages: collection, collection_group, database, documents, literals
//	transform:     where, sort, select, distinct, limit, offset
//
// Each stage is evaluated in order over the document stream. where and sort
// reuse the structured-query filter/order engine (firestoreprovider.FilterDocuments
// / SortDocuments); select and distinct reuse the projection engine. Every
// stage outside that vocabulary — and every expression inside a supported stage
// that the relational engine cannot represent (computed projections, non-field
// sort keys, boolean functions other than the comparison/composite subset) —
// fails loudly with codes.Unimplemented rather than fabricating a result,
// matching the emulator's convention of refusing to silently diverge. The whole
// pipeline is evaluated eagerly (one response carrying all results plus
// execution_time), which is a valid batching of the stream.
func (s *Service) ExecutePipeline(req *firestorepb.ExecutePipelineRequest, stream firestorepb.Firestore_ExecutePipelineServer) error {
	ctx := stream.Context()
	project, database, _, ok := splitParent(req.GetDatabase())
	if !ok {
		return mapError(model.NewProviderError("InvalidArgument", "invalid database resource name", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	sp := req.GetStructuredPipeline()
	if sp == nil || sp.GetPipeline() == nil || len(sp.GetPipeline().GetStages()) == 0 {
		return mapError(model.NewProviderError("InvalidArgument", "structured_pipeline.pipeline.stages is required", 400))
	}

	txn := req.GetTransaction()
	resp := &firestorepb.ExecutePipelineResponse{ExecutionTime: timestamppb.New(clock.Now())}
	if req.GetNewTransaction() != nil {
		newTxn, err := s.svc.BeginTransaction(ctx)
		if err != nil {
			return mapError(err)
		}
		resp.Transaction = newTxn
		txn = newTxn
	}

	docs, err := s.runPipeline(ctx, project, database, sp.GetPipeline().GetStages(), txn)
	if err != nil {
		return mapError(err)
	}
	resp.Results = encodeDocuments(docs)
	return stream.Send(resp)
}

// runPipeline evaluates the ordered stage list. The first stage must be a
// source; every later stage transforms the document stream.
func (s *Service) runPipeline(ctx context.Context, project, database string, stages []*firestorepb.Pipeline_Stage, txn []byte) ([]firestorestore.Document, error) {
	var docs []firestorestore.Document
	sourced := false
	for _, stage := range stages {
		name := stage.GetName()
		switch name {
		case "collection", "collection_group", "database", "documents", "literals":
			if sourced {
				return nil, pipelineInvalid("pipeline stage %q may only be used as the first stage", name)
			}
			out, err := s.runPipelineSource(ctx, project, database, stage, txn)
			if err != nil {
				return nil, err
			}
			docs = out
			sourced = true
		case "where":
			if !sourced {
				return nil, pipelineInvalid("pipeline stage %q requires a source stage first", name)
			}
			f, err := pipelineWhereFilter(stage)
			if err != nil {
				return nil, err
			}
			docs, err = firestoreprovider.FilterDocuments(docs, f)
			if err != nil {
				return nil, err
			}
		case "sort":
			if !sourced {
				return nil, pipelineInvalid("pipeline stage %q requires a source stage first", name)
			}
			orders, err := pipelineOrders(stage)
			if err != nil {
				return nil, err
			}
			docs = firestoreprovider.SortDocuments(docs, orders)
		case "select":
			if !sourced {
				return nil, pipelineInvalid("pipeline stage %q requires a source stage first", name)
			}
			proj, err := pipelineProjection(stage, name)
			if err != nil {
				return nil, err
			}
			docs = firestoreprovider.ProjectDocuments(docs, proj)
		case "distinct":
			if !sourced {
				return nil, pipelineInvalid("pipeline stage %q requires a source stage first", name)
			}
			proj, err := pipelineProjection(stage, name)
			if err != nil {
				return nil, err
			}
			docs = firestoreprovider.DistinctDocuments(docs, proj)
		case "limit":
			if !sourced {
				return nil, pipelineInvalid("pipeline stage %q requires a source stage first", name)
			}
			docs = pipelineLimit(docs, stage)
		case "offset":
			if !sourced {
				return nil, pipelineInvalid("pipeline stage %q requires a source stage first", name)
			}
			docs = pipelineOffset(docs, stage)
		default:
			return nil, pipelineUnimplemented("ExecutePipeline stage %q is not supported by this emulator", name)
		}
	}
	if !sourced {
		return nil, pipelineInvalid("pipeline must begin with a source stage")
	}
	return docs, nil
}

// pipelineInvalid returns an INVALID_ARGUMENT provider error (mapped to
// codes.InvalidArgument by ExecutePipeline).
func pipelineInvalid(format string, args ...any) error {
	return model.NewProviderError("InvalidArgument", fmt.Sprintf(format, args...), 400)
}

// pipelineUnimplemented returns an UNIMPLEMENTED provider error (mapped to
// codes.Unimplemented by ExecutePipeline) for a construct the relational subset
// deliberately refuses to evaluate.
func pipelineUnimplemented(format string, args ...any) error {
	return model.NewProviderError("Unimplemented", fmt.Sprintf(format, args...), 501)
}

// runPipelineSource resolves one source stage into its document set.
func (s *Service) runPipelineSource(ctx context.Context, project, database string, stage *firestorepb.Pipeline_Stage, txn []byte) ([]firestorestore.Document, error) {
	switch stage.GetName() {
	case "collection":
		ref := referenceArg(stage, 0)
		if ref == "" {
			return nil, pipelineInvalid("collection stage requires a collection reference argument")
		}
		parentRel, collectionID, ok := pipelineCollectionPath(ref)
		if !ok {
			return nil, pipelineInvalid("collection stage has an empty collection path")
		}
		return s.runPipelineQuery(ctx, project, database, parentRel, &firestoreprovider.StructuredQuery{
			From: []firestoreprovider.CollectionSelector{{CollectionID: collectionID}},
		}, txn)
	case "collection_group":
		ancestor := referenceArg(stage, 0)
		collectionID := stringArg(stage, 1)
		if collectionID == "" {
			return nil, pipelineInvalid("collection_group stage requires a collection id argument")
		}
		parentRel := strings.TrimPrefix(ancestor, "/")
		return s.runPipelineQuery(ctx, project, database, parentRel, &firestoreprovider.StructuredQuery{
			From: []firestoreprovider.CollectionSelector{{CollectionID: collectionID, AllDescendants: true}},
		}, txn)
	case "database":
		return s.runPipelineQuery(ctx, project, database, "", &firestoreprovider.StructuredQuery{}, txn)
	case "documents":
		out := make([]firestorestore.Document, 0, len(stage.GetArgs()))
		for i := range stage.GetArgs() {
			ref := referenceArg(stage, i)
			if ref == "" {
				return nil, pipelineInvalid("documents stage arguments must be document references")
			}
			name := pipelineDocumentName(project, database, ref)
			doc, err := s.svc.GetDocument(ctx, name, txn, nil)
			if err != nil {
				return nil, err
			}
			out = append(out, doc)
		}
		return out, nil
	case "literals":
		out := make([]firestorestore.Document, 0, len(stage.GetArgs()))
		for _, arg := range stage.GetArgs() {
			fields, err := decodeFields(&firestorepb.Document{Fields: arg.GetMapValue().GetFields()})
			if err != nil {
				return nil, pipelineInvalid("%s", err.Error())
			}
			out = append(out, firestorestore.Document{Fields: fields})
		}
		return out, nil
	}
	return nil, pipelineUnimplemented("ExecutePipeline source stage %q is not supported by this emulator", stage.GetName())
}

// runPipelineQuery runs a structured query through the shared provider engine.
func (s *Service) runPipelineQuery(ctx context.Context, project, database, rel string, q *firestoreprovider.StructuredQuery, txn []byte) ([]firestorestore.Document, error) {
	docs, err := s.svc.RunQuery(ctx, project, database, rel, q, txn)
	if err != nil {
		return nil, err
	}
	return docs, nil
}

// ─── where ────────────────────────────────────────────────────────────────────

// pipelineComparisonOps maps the pipeline DSL's comparison function names onto
// the wire FieldFilter operator names with a constant right-hand side.
var pipelineComparisonOps = map[string]string{
	"equal":                 "EQUAL",
	"not_equal":             "NOT_EQUAL",
	"less_than":             "LESS_THAN",
	"less_than_or_equal":    "LESS_THAN_OR_EQUAL",
	"greater_than":          "GREATER_THAN",
	"greater_than_or_equal": "GREATER_THAN_OR_EQUAL",
}

// pipelineArrayFieldOps maps the pipeline DSL's array/any functions onto the
// wire FieldFilter operator names with an array right-hand side.
var pipelineArrayFieldOps = map[string]string{
	"array_contains":     "ARRAY_CONTAINS",
	"array_contains_any": "ARRAY_CONTAINS_ANY",
	"equal_any":          "IN",
	"not_equal_any":      "NOT_IN",
}

// pipelineWhereFilter transcodes a where stage's boolean-expression argument
// into the engine's Filter tree (FieldFilter leaves plus AND/OR composites).
func pipelineWhereFilter(stage *firestorepb.Pipeline_Stage) (*firestoreprovider.Filter, error) {
	args := stage.GetArgs()
	if len(args) == 0 {
		return nil, pipelineInvalid("where stage requires a boolean expression argument")
	}
	fn := args[0].GetFunctionValue()
	if fn == nil {
		return nil, pipelineInvalid("where stage argument must be a boolean expression")
	}
	return pipelineBooleanExpr(fn)
}

// pipelineBooleanExpr decodes one pipeline boolean function into a Filter. The
// supported subset is the comparison FieldFilters plus the and/or composite
// operators; everything else fails loud with Unimplemented.
func pipelineBooleanExpr(fn *firestorepb.Function) (*firestoreprovider.Filter, error) {
	name := fn.GetName()
	switch name {
	case "and", "or":
		if len(fn.GetArgs()) == 0 {
			return nil, pipelineInvalid("%s requires at least one argument", name)
		}
		op := "AND"
		if name == "or" {
			op = "OR"
		}
		cf := &firestoreprovider.CompositeFilter{Op: op}
		for i, a := range fn.GetArgs() {
			sub := a.GetFunctionValue()
			if sub == nil {
				return nil, pipelineInvalid("%s argument %d must be a boolean expression", name, i+1)
			}
			f, err := pipelineBooleanExpr(sub)
			if err != nil {
				return nil, err
			}
			cf.Filters = append(cf.Filters, f)
		}
		return &firestoreprovider.Filter{CompositeFilter: cf}, nil
	}

	if op, ok := pipelineComparisonOps[name]; ok {
		fieldPath, err := pipelineFieldArg(fn, 0)
		if err != nil {
			return nil, err
		}
		if len(fn.GetArgs()) < 2 {
			return nil, pipelineInvalid("%s requires a value argument", name)
		}
		val, err := decodeValue(fn.GetArgs()[1])
		if err != nil {
			return nil, pipelineUnimplemented("where function %q requires a constant value argument", name)
		}
		return &firestoreprovider.Filter{FieldFilter: &firestoreprovider.FieldFilter{
			Field: firestoreprovider.FieldReference{FieldPath: fieldPath},
			Op:    op,
			Value: val,
		}}, nil
	}

	if op, ok := pipelineArrayFieldOps[name]; ok {
		fieldPath, err := pipelineFieldArg(fn, 0)
		if err != nil {
			return nil, err
		}
		if len(fn.GetArgs()) < 2 {
			return nil, pipelineInvalid("%s requires an array argument", name)
		}
		var operand *firestorestore.Value
		if name == "array_contains" {
			v, err := decodeValue(fn.GetArgs()[1])
			if err != nil {
				return nil, pipelineUnimplemented("where function %q requires a constant value argument", name)
			}
			operand = v
		} else {
			av, err := pipelineArrayOperand(fn.GetArgs()[1])
			if err != nil {
				return nil, err
			}
			operand = firestorestore.ArrayVal(av.Values...)
		}
		return &firestoreprovider.Filter{FieldFilter: &firestoreprovider.FieldFilter{
			Field: firestoreprovider.FieldReference{FieldPath: fieldPath},
			Op:    op,
			Value: operand,
		}}, nil
	}

	return nil, pipelineUnimplemented("where function %q is not supported by this emulator", name)
}

// pipelineFieldArg returns a field-reference argument of a pipeline function.
func pipelineFieldArg(fn *firestorepb.Function, i int) (string, error) {
	args := fn.GetArgs()
	if i >= len(args) {
		return "", pipelineInvalid("%s requires at least %d arguments", fn.GetName(), i+1)
	}
	fp := args[i].GetFieldReferenceValue()
	if fp == "" {
		return "", pipelineInvalid("%s argument %d must be a field reference", fn.GetName(), i+1)
	}
	return fp, nil
}

// pipelineArrayOperand decodes an array operand, accepting either an array
// value directly or the pipeline DSL's array(...) function.
func pipelineArrayOperand(v *firestorepb.Value) (*firestorestore.ArrayValue, error) {
	if av := v.GetArrayValue(); av != nil {
		return decodeArrayValue(av)
	}
	if fn := v.GetFunctionValue(); fn != nil && fn.GetName() == "array" {
		out := &firestorestore.ArrayValue{}
		for _, a := range fn.GetArgs() {
			dv, err := decodeValue(a)
			if err != nil {
				return nil, pipelineInvalid("array element must be a constant value")
			}
			out.Values = append(out.Values, dv)
		}
		return out, nil
	}
	return nil, pipelineInvalid("expected an array value")
}

// ─── sort ─────────────────────────────────────────────────────────────────────

// pipelineOrders transcodes a sort stage's map arguments
// ({direction, expression}) into the engine's orderBy clauses. Only
// field-reference keys are supported; computed order keys fail loud.
func pipelineOrders(stage *firestorepb.Pipeline_Stage) ([]firestoreprovider.Order, error) {
	args := stage.GetArgs()
	if len(args) == 0 {
		return nil, pipelineInvalid("sort stage requires at least one ordering")
	}
	orders := make([]firestoreprovider.Order, 0, len(args))
	for i, a := range args {
		m := a.GetMapValue()
		if m == nil {
			return nil, pipelineInvalid("sort ordering %d must be a map", i+1)
		}
		fields := m.GetFields()
		expr := fields["expression"]
		fieldPath := expr.GetFieldReferenceValue()
		if fieldPath == "" {
			return nil, pipelineUnimplemented("sort supports only field-reference orderings")
		}
		direction := strings.ToUpper(fields["direction"].GetStringValue())
		switch direction {
		case "", "ASCENDING", "ASC":
			direction = "ASCENDING"
		case "DESCENDING", "DESC":
			direction = "DESCENDING"
		default:
			return nil, pipelineInvalid("invalid sort direction %q", direction)
		}
		orders = append(orders, firestoreprovider.Order{
			Field:     firestoreprovider.FieldReference{FieldPath: fieldPath},
			Direction: direction,
		})
	}
	return orders, nil
}

// ─── select / distinct ────────────────────────────────────────────────────────

// pipelineProjection decodes a select/distinct stage's map argument into a
// projection. Only field-reference selectables are supported; a computed or
// otherwise aliased expression fails loud. An absent or empty map yields nil
// (select: no projection / all fields, matching StructuredQuery.Projection's
// documented "empty means all"; distinct: whole-document distinctness).
func pipelineProjection(stage *firestorepb.Pipeline_Stage, kind string) (*firestoreprovider.Projection, error) {
	args := stage.GetArgs()
	if len(args) == 0 {
		return nil, nil
	}
	m := args[0].GetMapValue()
	if m == nil {
		return nil, pipelineInvalid("%s stage argument must be a map", kind)
	}
	fields := m.GetFields()
	if len(fields) == 0 {
		return nil, nil
	}
	// Map iteration order is random; sort by alias so the transcode (and any
	// projection output) is deterministic.
	aliases := make([]string, 0, len(fields))
	for alias := range fields {
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)
	proj := &firestoreprovider.Projection{}
	for _, alias := range aliases {
		fieldPath := fields[alias].GetFieldReferenceValue()
		if fieldPath == "" {
			return nil, pipelineUnimplemented("%s supports only field-reference selectables", kind)
		}
		proj.Fields = append(proj.Fields, firestoreprovider.FieldReference{FieldPath: fieldPath, OutputPath: alias})
	}
	return proj, nil
}

// ─── limit / offset ───────────────────────────────────────────────────────────

// pipelineLimit truncates the stream to the stage's integer argument (a
// non-positive limit means "no limit", matching StructuredQuery.Limit).
func pipelineLimit(docs []firestorestore.Document, stage *firestorepb.Pipeline_Stage) []firestorestore.Document {
	n := intArg(stage, 0)
	if n <= 0 || n >= len(docs) {
		return docs
	}
	return docs[:n]
}

// pipelineOffset drops the first stage-argument documents.
func pipelineOffset(docs []firestorestore.Document, stage *firestorepb.Pipeline_Stage) []firestorestore.Document {
	n := intArg(stage, 0)
	if n <= 0 {
		return docs
	}
	if n >= len(docs) {
		return nil
	}
	return docs[n:]
}

// ─── argument helpers ─────────────────────────────────────────────────────────

// referenceArg returns a reference-typed stage argument, tolerating a
// string-typed argument for callers that build stages by hand.
func referenceArg(stage *firestorepb.Pipeline_Stage, i int) string {
	args := stage.GetArgs()
	if i >= len(args) {
		return ""
	}
	if ref := args[i].GetReferenceValue(); ref != "" {
		return ref
	}
	return args[i].GetStringValue()
}

// stringArg returns a string-typed stage argument.
func stringArg(stage *firestorepb.Pipeline_Stage, i int) string {
	args := stage.GetArgs()
	if i >= len(args) {
		return ""
	}
	return args[i].GetStringValue()
}

// intArg returns an integer-typed stage argument.
func intArg(stage *firestorepb.Pipeline_Stage, i int) int {
	args := stage.GetArgs()
	if i >= len(args) {
		return 0
	}
	return int(args[i].GetIntegerValue())
}

// pipelineCollectionPath splits a collection reference ("/a/b/coll") into the
// parent document path relative to the documents root ("a/b") and the
// collection id ("coll"). A full resource name is accepted too.
func pipelineCollectionPath(ref string) (parentRel, collectionID string, ok bool) {
	if idx := strings.Index(ref, "/documents/"); idx >= 0 {
		ref = ref[idx+len("/documents"):]
	}
	ref = strings.TrimPrefix(ref, "/")
	if ref == "" {
		return "", "", false
	}
	parts := strings.Split(ref, "/")
	collectionID = parts[len(parts)-1]
	parentRel = strings.Join(parts[:len(parts)-1], "/")
	return parentRel, collectionID, true
}

// pipelineDocumentName expands a documents-stage reference ("/a/b/doc", or a
// full resource name) into a full document name.
func pipelineDocumentName(project, database, ref string) string {
	if strings.HasPrefix(ref, "projects/") {
		return ref
	}
	base := "projects/" + project + "/databases/" + database + "/documents"
	ref = strings.TrimPrefix(ref, "/")
	if ref == "" {
		return base
	}
	return base + "/" + ref
}
