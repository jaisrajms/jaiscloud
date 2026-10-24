// Package testdepth is the shared behavioural-depth assertion kit for the GCP
// official-client suites (plan DC2,
// plan_docs/gcp-depth-closure-wave-plan.md).
//
// The depth record introduced by the test-depth wave (docs/gcp/testability.md
// §10) has three classes that are client-observable contract, and this kit
// asserts exactly those three, once, instead of re-deriving them per service:
//
//   - p4 listing/paging — Walk follows pageToken to the end and asserts every
//     resource appears exactly once, in the documented order, across page
//     boundaries, with an empty final token and no page larger than requested
//     (AssertPaging, CheckPages).
//   - p3 errors/negatives — StatusOf extracts the typed error an official
//     client returns (*googleapi.Error over REST, a gRPC status over gRPC) and
//     CheckError/AssertError match it against the documented envelope
//     (ErrorSpec, the Spec* catalogue, RunNegatives).
//   - p2 concurrency/contention — Race runs N writers that start together and
//     AssertOneWinner asserts exactly one winner with the documented conflict
//     for the losers, refusing to run in a binary without the race detector.
//
// The kit is client-agnostic and import-only for its consumers: it depends on
// no service client package, so a suite attributes its coverage to the service
// it exercises and to nothing else. A consumer adapts its own list/error calls
// with a closure, e.g. an apiary REST client:
//
//	items := testdepth.AssertPaging(t, func() testdepth.Lister[*iam.ServiceAccount] {
//		svc := svc // the client handle, created once
//		return func(req testdepth.ListRequest) (testdepth.Page[*iam.ServiceAccount], error) {
//			call := svc.Projects.ServiceAccounts.List(parent).PageSize(req.PageSize)
//			if req.PageToken != "" {
//				call = call.PageToken(req.PageToken)
//			}
//			resp, err := call.Do()
//			if err != nil {
//				return testdepth.Page[*iam.ServiceAccount]{}, err
//			}
//			return testdepth.Page[*iam.ServiceAccount]{Items: resp.Accounts, NextPageToken: resp.NextPageToken}, nil
//		}
//	}, testdepth.Paging[*iam.ServiceAccount]{
//		Name:     func(acct *iam.ServiceAccount) string { return acct.Name },
//		PageSize: 2,
//		Expect:   want,
//		Order:    testdepth.OrderAscendingName,
//	})
//
// and a gRPC iterator client (note the factory: an iterator is stateful, and the
// contract walk runs twice to prove cross-page stability):
//
//	items := testdepth.AssertPaging(t, func() testdepth.Lister[*managedkafkapb.Topic] {
//		it := client.ListTopics(ctx, &managedkafkapb.ListTopicsRequest{Parent: parent})
//		return func(req testdepth.ListRequest) (testdepth.Page[*managedkafkapb.Topic], error) {
//			page, next, err := it.InternalFetch(int(req.PageSize), req.PageToken)
//			return testdepth.Page[*managedkafkapb.Topic]{Items: page, NextPageToken: next}, err
//		}
//	}, cfg)
//
// Errors and contention are asserted the same way:
//
//	testdepth.AssertError(t, err, testdepth.SpecAlreadyExists())
//	testdepth.AssertOneWinner(t, testdepth.Race(6, create), func(err error) bool {
//		return len(testdepth.CheckError(err, testdepth.SpecAlreadyExists())) == 0
//	})
package testdepth
