package ui

import (
	"context"

	bigqueryui "jaiscloud/internal/gcp/ui/bigquery"
	computeui "jaiscloud/internal/gcp/ui/compute"
	firestoreui "jaiscloud/internal/gcp/ui/firestore"
	iamui "jaiscloud/internal/gcp/ui/iam"
	kmsui "jaiscloud/internal/gcp/ui/kms"
	loggingui "jaiscloud/internal/gcp/ui/logging"
	monitoringui "jaiscloud/internal/gcp/ui/monitoring"
	pubsubui "jaiscloud/internal/gcp/ui/pubsub"
	secretmanagerui "jaiscloud/internal/gcp/ui/secretmanager"
	storageui "jaiscloud/internal/gcp/ui/storage"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// One forwarder per console method that is a registry action. routed_guard_test.go
// fails if a ProviderInterface method of these services is neither forwarded here
// nor listed there as a deliberate exception, or if a forwarder names a key no
// provider registers, so this file cannot silently fall behind.
//
// The console is a client like any other: when a plugin overrides a service, the
// console must show and change what the plugin serves, so every console call that
// is a registry action goes through Registry.Dispatch with origin "ui". Console
// calls that are not registry actions stay on the built-in.

// uiCaller dispatches console calls. A nil registry (unit tests) calls directly.
type uiCaller struct{ reg *provider.Registry }

func (c uiCaller) call(ctx context.Context, key string, direct func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error), nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if c.reg == nil || !c.reg.Has(key) {
		return direct(ctx, nr)
	}
	nr.Origin = model.OriginUI
	return c.reg.Dispatch(ctx, key, nr)
}

// routedPubSub serves the pubsub console through the registry (origin "ui"). Methods not listed here stay on the embedded built-in.
type routedPubSub struct {
	pubsubui.ProviderInterface
	c uiCaller
}

func routePubSub(reg *provider.Registry, p pubsubui.ProviderInterface) pubsubui.ProviderInterface {
	return routedPubSub{ProviderInterface: p, c: uiCaller{reg: reg}}
}

func (p routedPubSub) TopicList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "PubSub.TopicList", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.TopicList(ctx, nr)
	}, nr)
}

func (p routedPubSub) TopicCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "PubSub.TopicCreate", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.TopicCreate(ctx, nr)
	}, nr)
}

func (p routedPubSub) TopicGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "PubSub.TopicGet", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.TopicGet(ctx, nr)
	}, nr)
}

func (p routedPubSub) TopicDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "PubSub.TopicDelete", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.TopicDelete(ctx, nr)
	}, nr)
}

func (p routedPubSub) TopicPublish(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "PubSub.TopicPublish", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.TopicPublish(ctx, nr)
	}, nr)
}

func (p routedPubSub) SubscriptionList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "PubSub.SubscriptionList", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.SubscriptionList(ctx, nr)
	}, nr)
}

func (p routedPubSub) SubscriptionCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "PubSub.SubscriptionCreate", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.SubscriptionCreate(ctx, nr)
	}, nr)
}

func (p routedPubSub) SubscriptionGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "PubSub.SubscriptionGet", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.SubscriptionGet(ctx, nr)
	}, nr)
}

func (p routedPubSub) SubscriptionUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "PubSub.SubscriptionUpdate", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.SubscriptionUpdate(ctx, nr)
	}, nr)
}

func (p routedPubSub) SubscriptionDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "PubSub.SubscriptionDelete", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.SubscriptionDelete(ctx, nr)
	}, nr)
}

func (p routedPubSub) TopicGetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "PubSub.TopicGetIamPolicy", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.TopicGetIamPolicy(ctx, nr)
	}, nr)
}

func (p routedPubSub) TopicSetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "PubSub.TopicSetIamPolicy", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.TopicSetIamPolicy(ctx, nr)
	}, nr)
}

func (p routedPubSub) SubscriptionGetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "PubSub.SubscriptionGetIamPolicy", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.SubscriptionGetIamPolicy(ctx, nr)
	}, nr)
}

func (p routedPubSub) SubscriptionSetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "PubSub.SubscriptionSetIamPolicy", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.SubscriptionSetIamPolicy(ctx, nr)
	}, nr)
}

// routedStorage serves the storage console through the registry (origin "ui"). Methods not listed here stay on the embedded built-in.
type routedStorage struct {
	storageui.ProviderInterface
	c uiCaller
}

func routeStorage(reg *provider.Registry, p storageui.ProviderInterface) storageui.ProviderInterface {
	return routedStorage{ProviderInterface: p, c: uiCaller{reg: reg}}
}

func (p routedStorage) BucketsList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Storage.BucketsList", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.BucketsList(ctx, nr)
	}, nr)
}

func (p routedStorage) BucketsInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Storage.BucketsInsert", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.BucketsInsert(ctx, nr)
	}, nr)
}

func (p routedStorage) BucketsGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Storage.BucketsGet", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.BucketsGet(ctx, nr)
	}, nr)
}

func (p routedStorage) BucketsUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Storage.BucketsUpdate", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.BucketsUpdate(ctx, nr)
	}, nr)
}

func (p routedStorage) BucketsLockRetentionPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Storage.BucketsLockRetentionPolicy", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.BucketsLockRetentionPolicy(ctx, nr)
	}, nr)
}

func (p routedStorage) BucketsDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Storage.BucketsDelete", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.BucketsDelete(ctx, nr)
	}, nr)
}

func (p routedStorage) ObjectsList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Storage.ObjectsList", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ObjectsList(ctx, nr)
	}, nr)
}

func (p routedStorage) ObjectsInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Storage.ObjectsInsert", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ObjectsInsert(ctx, nr)
	}, nr)
}

func (p routedStorage) ObjectsGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Storage.ObjectsGet", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ObjectsGet(ctx, nr)
	}, nr)
}

func (p routedStorage) ObjectsGetMedia(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Storage.ObjectsGetMedia", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ObjectsGetMedia(ctx, nr)
	}, nr)
}

func (p routedStorage) ObjectsPatch(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Storage.ObjectsPatch", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ObjectsPatch(ctx, nr)
	}, nr)
}

func (p routedStorage) ObjectsDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Storage.ObjectsDelete", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ObjectsDelete(ctx, nr)
	}, nr)
}

func (p routedStorage) ObjectsRestore(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Storage.ObjectsRestore", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ObjectsRestore(ctx, nr)
	}, nr)
}

func (p routedStorage) BucketsGetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Storage.BucketsGetIamPolicy", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.BucketsGetIamPolicy(ctx, nr)
	}, nr)
}

func (p routedStorage) BucketsSetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Storage.BucketsSetIamPolicy", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.BucketsSetIamPolicy(ctx, nr)
	}, nr)
}

func (p routedStorage) BucketACLList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Storage.BucketACLList", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.BucketACLList(ctx, nr)
	}, nr)
}

func (p routedStorage) BucketACLInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Storage.BucketACLInsert", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.BucketACLInsert(ctx, nr)
	}, nr)
}

func (p routedStorage) ObjectsGetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Storage.ObjectsGetIamPolicy", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ObjectsGetIamPolicy(ctx, nr)
	}, nr)
}

func (p routedStorage) ObjectsSetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Storage.ObjectsSetIamPolicy", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ObjectsSetIamPolicy(ctx, nr)
	}, nr)
}

func (p routedStorage) ObjectACLList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Storage.ObjectACLList", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ObjectACLList(ctx, nr)
	}, nr)
}

func (p routedStorage) ObjectACLInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Storage.ObjectACLInsert", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ObjectACLInsert(ctx, nr)
	}, nr)
}

// routedFirestore serves the firestore console through the registry (origin "ui"). Methods not listed here stay on the embedded built-in.
type routedFirestore struct {
	firestoreui.ProviderInterface
	c uiCaller
}

func routeFirestore(reg *provider.Registry, p firestoreui.ProviderInterface) firestoreui.ProviderInterface {
	return routedFirestore{ProviderInterface: p, c: uiCaller{reg: reg}}
}

func (p routedFirestore) ListCollectionIds(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Firestore.ListCollectionIds", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ListCollectionIds(ctx, nr)
	}, nr)
}

func (p routedFirestore) ListDocuments(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Firestore.ListDocuments", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ListDocuments(ctx, nr)
	}, nr)
}

func (p routedFirestore) DocumentsGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Firestore.GetDocument", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.DocumentsGet(ctx, nr)
	}, nr)
}

func (p routedFirestore) CreateDocument(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Firestore.CreateDocument", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.CreateDocument(ctx, nr)
	}, nr)
}

func (p routedFirestore) DocumentsPatch(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Firestore.PatchDocument", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.DocumentsPatch(ctx, nr)
	}, nr)
}

func (p routedFirestore) DocumentsDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Firestore.DeleteDocument", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.DocumentsDelete(ctx, nr)
	}, nr)
}

func (p routedFirestore) RunQuery(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Firestore.RunQuery", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.RunQuery(ctx, nr)
	}, nr)
}

func (p routedFirestore) CreateIndex(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Firestore.CreateIndex", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.CreateIndex(ctx, nr)
	}, nr)
}

func (p routedFirestore) ListIndexes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Firestore.ListIndexes", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ListIndexes(ctx, nr)
	}, nr)
}

func (p routedFirestore) GetIndex(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Firestore.GetIndex", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.GetIndex(ctx, nr)
	}, nr)
}

func (p routedFirestore) DeleteIndex(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Firestore.DeleteIndex", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.DeleteIndex(ctx, nr)
	}, nr)
}

// routedBigQuery serves the bigquery console through the registry (origin "ui"). Methods not listed here stay on the embedded built-in.
type routedBigQuery struct {
	bigqueryui.ProviderInterface
	c uiCaller
}

func routeBigQuery(reg *provider.Registry, p bigqueryui.ProviderInterface) bigqueryui.ProviderInterface {
	return routedBigQuery{ProviderInterface: p, c: uiCaller{reg: reg}}
}

func (p routedBigQuery) ListDatasets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "BigQuery.ListDatasets", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ListDatasets(ctx, nr)
	}, nr)
}

func (p routedBigQuery) GetDataset(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "BigQuery.GetDataset", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.GetDataset(ctx, nr)
	}, nr)
}

func (p routedBigQuery) CreateDataset(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "BigQuery.CreateDataset", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.CreateDataset(ctx, nr)
	}, nr)
}

func (p routedBigQuery) DeleteDataset(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "BigQuery.DeleteDataset", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.DeleteDataset(ctx, nr)
	}, nr)
}

func (p routedBigQuery) ListTables(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "BigQuery.ListTables", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ListTables(ctx, nr)
	}, nr)
}

func (p routedBigQuery) GetTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "BigQuery.GetTable", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.GetTable(ctx, nr)
	}, nr)
}

func (p routedBigQuery) CreateTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "BigQuery.CreateTable", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.CreateTable(ctx, nr)
	}, nr)
}

func (p routedBigQuery) DeleteTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "BigQuery.DeleteTable", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.DeleteTable(ctx, nr)
	}, nr)
}

func (p routedBigQuery) ListRows(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "BigQuery.ListRows", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ListRows(ctx, nr)
	}, nr)
}

func (p routedBigQuery) InsertAll(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "BigQuery.InsertAll", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.InsertAll(ctx, nr)
	}, nr)
}

func (p routedBigQuery) Query(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "BigQuery.Query", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.Query(ctx, nr)
	}, nr)
}

func (p routedBigQuery) ListJobs(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "BigQuery.ListJobs", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ListJobs(ctx, nr)
	}, nr)
}

func (p routedBigQuery) GetJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "BigQuery.GetJob", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.GetJob(ctx, nr)
	}, nr)
}

func (p routedBigQuery) DeleteJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "BigQuery.DeleteJob", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.DeleteJob(ctx, nr)
	}, nr)
}

func (p routedBigQuery) CancelJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "BigQuery.CancelJob", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.CancelJob(ctx, nr)
	}, nr)
}

// routedCompute serves the compute console through the registry (origin "ui"). Methods not listed here stay on the embedded built-in.
type routedCompute struct {
	computeui.ProviderInterface
	c uiCaller
}

func routeCompute(reg *provider.Registry, p computeui.ProviderInterface) computeui.ProviderInterface {
	return routedCompute{ProviderInterface: p, c: uiCaller{reg: reg}}
}

func (p routedCompute) InstancesAggregatedList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Compute.InstancesAggregatedList", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.InstancesAggregatedList(ctx, nr)
	}, nr)
}

func (p routedCompute) InstancesGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Compute.InstancesGet", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.InstancesGet(ctx, nr)
	}, nr)
}

func (p routedCompute) InstancesStart(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Compute.InstancesStart", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.InstancesStart(ctx, nr)
	}, nr)
}

func (p routedCompute) InstancesStop(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Compute.InstancesStop", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.InstancesStop(ctx, nr)
	}, nr)
}

func (p routedCompute) InstancesDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Compute.InstancesDelete", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.InstancesDelete(ctx, nr)
	}, nr)
}

// routedIAM serves the iam console through the registry (origin "ui"). Methods not listed here stay on the embedded built-in.
type routedIAM struct {
	iamui.ProviderInterface
	c uiCaller
}

func routeIAM(reg *provider.Registry, p iamui.ProviderInterface) iamui.ProviderInterface {
	return routedIAM{ProviderInterface: p, c: uiCaller{reg: reg}}
}

func (p routedIAM) Create(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.ServiceAccountCreate", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.Create(ctx, nr)
	}, nr)
}

func (p routedIAM) List(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.ServiceAccountList", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.List(ctx, nr)
	}, nr)
}

func (p routedIAM) Get(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.ServiceAccountGet", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.Get(ctx, nr)
	}, nr)
}

func (p routedIAM) Delete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.ServiceAccountDelete", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.Delete(ctx, nr)
	}, nr)
}

func (p routedIAM) Disable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.ServiceAccountDisable", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.Disable(ctx, nr)
	}, nr)
}

func (p routedIAM) Enable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.ServiceAccountEnable", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.Enable(ctx, nr)
	}, nr)
}

func (p routedIAM) GetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.ServiceAccountGetIamPolicy", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.GetIamPolicy(ctx, nr)
	}, nr)
}

func (p routedIAM) SetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.ServiceAccountSetIamPolicy", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.SetIamPolicy(ctx, nr)
	}, nr)
}

func (p routedIAM) TestIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.ServiceAccountTestIamPermissions", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.TestIamPermissions(ctx, nr)
	}, nr)
}

func (p routedIAM) ServiceAccountKeyCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.ServiceAccountKeyCreate", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ServiceAccountKeyCreate(ctx, nr)
	}, nr)
}

func (p routedIAM) ServiceAccountKeyList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.ServiceAccountKeyList", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ServiceAccountKeyList(ctx, nr)
	}, nr)
}

func (p routedIAM) ServiceAccountKeyGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.ServiceAccountKeyGet", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ServiceAccountKeyGet(ctx, nr)
	}, nr)
}

func (p routedIAM) ServiceAccountKeyDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.ServiceAccountKeyDelete", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ServiceAccountKeyDelete(ctx, nr)
	}, nr)
}

func (p routedIAM) ServiceAccountKeyDisable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.ServiceAccountKeyDisable", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ServiceAccountKeyDisable(ctx, nr)
	}, nr)
}

func (p routedIAM) ServiceAccountKeyEnable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.ServiceAccountKeyEnable", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ServiceAccountKeyEnable(ctx, nr)
	}, nr)
}

func (p routedIAM) ServiceAccountSignBlob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.ServiceAccountSignBlob", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ServiceAccountSignBlob(ctx, nr)
	}, nr)
}

func (p routedIAM) ServiceAccountSignJwt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.ServiceAccountSignJwt", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ServiceAccountSignJwt(ctx, nr)
	}, nr)
}

// routedKMS serves the kms console through the registry (origin "ui"). Methods not listed here stay on the embedded built-in.
type routedKMS struct {
	kmsui.ProviderInterface
	c uiCaller
}

func routeKMS(reg *provider.Registry, p kmsui.ProviderInterface) kmsui.ProviderInterface {
	return routedKMS{ProviderInterface: p, c: uiCaller{reg: reg}}
}

func (p routedKMS) KeyRingCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "KMS.KeyRingCreate", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.KeyRingCreate(ctx, nr)
	}, nr)
}

func (p routedKMS) KeyRingList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "KMS.KeyRingList", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.KeyRingList(ctx, nr)
	}, nr)
}

func (p routedKMS) KeyRingGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "KMS.KeyRingGet", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.KeyRingGet(ctx, nr)
	}, nr)
}

func (p routedKMS) CryptoKeyCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "KMS.CryptoKeyCreate", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.CryptoKeyCreate(ctx, nr)
	}, nr)
}

func (p routedKMS) CryptoKeyList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "KMS.CryptoKeyList", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.CryptoKeyList(ctx, nr)
	}, nr)
}

func (p routedKMS) CryptoKeyGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "KMS.CryptoKeyGet", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.CryptoKeyGet(ctx, nr)
	}, nr)
}

func (p routedKMS) CryptoKeyVersionCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "KMS.CryptoKeyVersionCreate", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.CryptoKeyVersionCreate(ctx, nr)
	}, nr)
}

func (p routedKMS) CryptoKeyVersionList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "KMS.CryptoKeyVersionList", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.CryptoKeyVersionList(ctx, nr)
	}, nr)
}

func (p routedKMS) CryptoKeyVersionGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "KMS.CryptoKeyVersionGet", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.CryptoKeyVersionGet(ctx, nr)
	}, nr)
}

func (p routedKMS) CryptoKeyVersionDestroy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "KMS.CryptoKeyVersionDestroy", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.CryptoKeyVersionDestroy(ctx, nr)
	}, nr)
}

func (p routedKMS) CryptoKeyVersionUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "KMS.CryptoKeyVersionUpdate", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.CryptoKeyVersionUpdate(ctx, nr)
	}, nr)
}

func (p routedKMS) CryptoKeyUpdatePrimaryVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "KMS.CryptoKeyUpdatePrimaryVersion", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.CryptoKeyUpdatePrimaryVersion(ctx, nr)
	}, nr)
}

func (p routedKMS) CryptoKeyEncrypt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "KMS.CryptoKeyEncrypt", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.CryptoKeyEncrypt(ctx, nr)
	}, nr)
}

func (p routedKMS) CryptoKeyDecrypt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "KMS.CryptoKeyDecrypt", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.CryptoKeyDecrypt(ctx, nr)
	}, nr)
}

func (p routedKMS) CryptoKeyVersionAsymmetricSign(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "KMS.CryptoKeyVersionAsymmetricSign", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.CryptoKeyVersionAsymmetricSign(ctx, nr)
	}, nr)
}

func (p routedKMS) CryptoKeyVersionAsymmetricDecrypt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "KMS.CryptoKeyVersionAsymmetricDecrypt", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.CryptoKeyVersionAsymmetricDecrypt(ctx, nr)
	}, nr)
}

func (p routedKMS) CryptoKeyVersionMacSign(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "KMS.CryptoKeyVersionMacSign", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.CryptoKeyVersionMacSign(ctx, nr)
	}, nr)
}

func (p routedKMS) CryptoKeyVersionMacVerify(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "KMS.CryptoKeyVersionMacVerify", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.CryptoKeyVersionMacVerify(ctx, nr)
	}, nr)
}

func (p routedKMS) CryptoKeyVersionGetPublicKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "KMS.CryptoKeyVersionGetPublicKey", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.CryptoKeyVersionGetPublicKey(ctx, nr)
	}, nr)
}

// routedSecret serves the secretmanager console through the registry (origin "ui"). Methods not listed here stay on the embedded built-in.
type routedSecret struct {
	secretmanagerui.ProviderInterface
	c uiCaller
}

func routeSecret(reg *provider.Registry, p secretmanagerui.ProviderInterface) secretmanagerui.ProviderInterface {
	return routedSecret{ProviderInterface: p, c: uiCaller{reg: reg}}
}

func (p routedSecret) Create(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Secret.Create", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.Create(ctx, nr)
	}, nr)
}

func (p routedSecret) List(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Secret.List", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.List(ctx, nr)
	}, nr)
}

func (p routedSecret) Get(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Secret.Get", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.Get(ctx, nr)
	}, nr)
}

func (p routedSecret) Update(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Secret.Update", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.Update(ctx, nr)
	}, nr)
}

func (p routedSecret) Delete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Secret.Delete", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.Delete(ctx, nr)
	}, nr)
}

func (p routedSecret) AddVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Secret.AddVersion", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.AddVersion(ctx, nr)
	}, nr)
}

func (p routedSecret) Access(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Secret.Access", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.Access(ctx, nr)
	}, nr)
}

func (p routedSecret) ListVersions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Secret.ListVersions", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ListVersions(ctx, nr)
	}, nr)
}

func (p routedSecret) GetVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Secret.GetVersion", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.GetVersion(ctx, nr)
	}, nr)
}

func (p routedSecret) DestroyVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Secret.DestroyVersion", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.DestroyVersion(ctx, nr)
	}, nr)
}

func (p routedSecret) DisableVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Secret.DisableVersion", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.DisableVersion(ctx, nr)
	}, nr)
}

func (p routedSecret) EnableVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Secret.EnableVersion", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.EnableVersion(ctx, nr)
	}, nr)
}

func (p routedSecret) GetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Secret.GetIamPolicy", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.GetIamPolicy(ctx, nr)
	}, nr)
}

func (p routedSecret) SetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Secret.SetIamPolicy", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.SetIamPolicy(ctx, nr)
	}, nr)
}

func (p routedSecret) TestIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Secret.TestIamPermissions", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.TestIamPermissions(ctx, nr)
	}, nr)
}

// routedLogging serves the logging console through the registry (origin "ui"). Methods not listed here stay on the embedded built-in.
type routedLogging struct {
	loggingui.ProviderInterface
	c uiCaller
}

func routeLogging(reg *provider.Registry, p loggingui.ProviderInterface) loggingui.ProviderInterface {
	return routedLogging{ProviderInterface: p, c: uiCaller{reg: reg}}
}

func (p routedLogging) EntryList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Logging.EntryList", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.EntryList(ctx, nr)
	}, nr)
}

func (p routedLogging) LogList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Logging.LogList", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.LogList(ctx, nr)
	}, nr)
}

func (p routedLogging) LogDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Logging.LogDelete", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.LogDelete(ctx, nr)
	}, nr)
}

func (p routedLogging) MetricList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Logging.MetricList", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.MetricList(ctx, nr)
	}, nr)
}

func (p routedLogging) MetricGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Logging.MetricGet", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.MetricGet(ctx, nr)
	}, nr)
}

func (p routedLogging) MetricCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Logging.MetricCreate", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.MetricCreate(ctx, nr)
	}, nr)
}

func (p routedLogging) MetricUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Logging.MetricUpdate", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.MetricUpdate(ctx, nr)
	}, nr)
}

func (p routedLogging) MetricDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Logging.MetricDelete", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.MetricDelete(ctx, nr)
	}, nr)
}

func (p routedLogging) SinkList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Logging.SinkList", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.SinkList(ctx, nr)
	}, nr)
}

func (p routedLogging) SinkGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Logging.SinkGet", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.SinkGet(ctx, nr)
	}, nr)
}

func (p routedLogging) SinkCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Logging.SinkCreate", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.SinkCreate(ctx, nr)
	}, nr)
}

func (p routedLogging) SinkPatch(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Logging.SinkPatch", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.SinkPatch(ctx, nr)
	}, nr)
}

func (p routedLogging) SinkDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Logging.SinkDelete", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.SinkDelete(ctx, nr)
	}, nr)
}

func (p routedLogging) ExclusionList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Logging.ExclusionList", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ExclusionList(ctx, nr)
	}, nr)
}

func (p routedLogging) ExclusionGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Logging.ExclusionGet", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ExclusionGet(ctx, nr)
	}, nr)
}

func (p routedLogging) ExclusionCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Logging.ExclusionCreate", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ExclusionCreate(ctx, nr)
	}, nr)
}

func (p routedLogging) ExclusionPatch(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Logging.ExclusionPatch", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ExclusionPatch(ctx, nr)
	}, nr)
}

func (p routedLogging) ExclusionDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Logging.ExclusionDelete", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ExclusionDelete(ctx, nr)
	}, nr)
}

// routedMonitoring serves the monitoring console through the registry (origin "ui"). Methods not listed here stay on the embedded built-in.
type routedMonitoring struct {
	monitoringui.ProviderInterface
	c uiCaller
}

func routeMonitoring(reg *provider.Registry, p monitoringui.ProviderInterface) monitoringui.ProviderInterface {
	return routedMonitoring{ProviderInterface: p, c: uiCaller{reg: reg}}
}

func (p routedMonitoring) ListMetricDescriptors(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Monitoring.ListMetricDescriptors", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ListMetricDescriptors(ctx, nr)
	}, nr)
}

func (p routedMonitoring) ListTimeSeries(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Monitoring.ListTimeSeries", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ListTimeSeries(ctx, nr)
	}, nr)
}

func (p routedMonitoring) ListAlertPolicies(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Monitoring.ListAlertPolicies", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ListAlertPolicies(ctx, nr)
	}, nr)
}

func (p routedMonitoring) GetAlertPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Monitoring.GetAlertPolicy", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.GetAlertPolicy(ctx, nr)
	}, nr)
}

func (p routedMonitoring) CreateAlertPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Monitoring.CreateAlertPolicy", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.CreateAlertPolicy(ctx, nr)
	}, nr)
}

func (p routedMonitoring) UpdateAlertPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Monitoring.UpdateAlertPolicy", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.UpdateAlertPolicy(ctx, nr)
	}, nr)
}

func (p routedMonitoring) DeleteAlertPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Monitoring.DeleteAlertPolicy", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.DeleteAlertPolicy(ctx, nr)
	}, nr)
}

func (p routedMonitoring) ListNotificationChannels(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Monitoring.ListNotificationChannels", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ListNotificationChannels(ctx, nr)
	}, nr)
}

func (p routedMonitoring) GetNotificationChannel(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Monitoring.GetNotificationChannel", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.GetNotificationChannel(ctx, nr)
	}, nr)
}

func (p routedMonitoring) CreateNotificationChannel(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Monitoring.CreateNotificationChannel", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.CreateNotificationChannel(ctx, nr)
	}, nr)
}

func (p routedMonitoring) UpdateNotificationChannel(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Monitoring.UpdateNotificationChannel", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.UpdateNotificationChannel(ctx, nr)
	}, nr)
}

func (p routedMonitoring) DeleteNotificationChannel(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Monitoring.DeleteNotificationChannel", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.DeleteNotificationChannel(ctx, nr)
	}, nr)
}

func (p routedMonitoring) ListNotificationChannelDescriptors(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Monitoring.ListNotificationChannelDescriptors", func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return p.ProviderInterface.ListNotificationChannelDescriptors(ctx, nr)
	}, nr)
}
