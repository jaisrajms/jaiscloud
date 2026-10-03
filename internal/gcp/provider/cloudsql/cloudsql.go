// Package cloudsql implements the Cloud SQL Admin v1beta4 control-plane
// provider (sqladmin.googleapis.com/sql/v1beta4): instances, databases, users,
// and the operations that mutate them, plus the synthesized flags/tiers
// discovery and connectSettings surfaces.
//
// This is metadata-only, mirroring the Cloud DNS and Memorystore providers and
// the AWS RDS provider: instances, databases, users, and operations are stored
// as records in the shared ResourceStore, keyed by the project (AccountID) at
// store.GlobalRegion. The emulator never stands up a database server, so an
// instance is born in state RUNNABLE with a synthesized connectionName,
// selfLink, serviceAccountEmailAddress, and a cosmetic PRIMARY IP that nothing
// listens on. There is no SQL engine, no data plane, and no AuthN/AuthZ.
//
// Create/update/patch/restart/delete of instances, databases, and users return
// Cloud SQL's own Operation envelope (kind "sql#operation") inline with
// status "DONE" — NOT a google.longrunning.Operation — and the same operation
// is persisted so operations.get/list can read it back.
//
// Deferred surfaces (sslCerts, backupRuns, clone, failover, promoteReplica,
// import/export, demote, resetSslConfig, operation cancel, and the instance
// read-only certificate custom methods) resolve to the Unimplemented action and
// fail loud with 501 rather than silently succeeding.
package cloudsql

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// Resource types in the shared ResourceStore.
const (
	rtInstance  = "gcp_cloudsql_instance"
	rtDatabase  = "gcp_cloudsql_database"
	rtUser      = "gcp_cloudsql_user"
	rtOperation = "gcp_cloudsql_operation"
)

// Kind constants for the GCP wire envelope.
const (
	kindInstance        = "sql#instance"
	kindInstancesList   = "sql#instancesList"
	kindDatabase        = "sql#database"
	kindDatabasesList   = "sql#databasesList"
	kindUser            = "sql#user"
	kindUsersList       = "sql#usersList"
	kindOperation       = "sql#operation"
	kindOperationsList  = "sql#operationsList"
	kindFlagsList       = "sql#flagsList"
	kindTiersList       = "sql#tiersList"
	kindConnectSettings = "sql#connectSettings"
)

// sqlBasePath is the public Cloud SQL Admin REST root. selfLink/targetLink are
// absolute URLs rooted here.
const sqlBasePath = "https://sqladmin.googleapis.com/sql/v1beta4/"

// defaultRegion is used when a create request omits DatabaseInstance.region.
const defaultRegion = "us-central1"

// defaultDatabaseVersion is used when a create request omits databaseVersion.
const defaultDatabaseVersion = "MYSQL_8_0"

// Provider handles Cloud SQL instances, databases, users, and operations.
type Provider struct {
	resources store.ResourceStore
}

// New returns a Provider backed by the shared ResourceStore.
func New(resources store.ResourceStore) *Provider {
	return &Provider{resources: resources}
}

// Routes maps "CloudSQL.<Action>" keys to their handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"CloudSQL.InstancesInsert":  p.InstancesInsert,
		"CloudSQL.InstancesGet":     p.InstancesGet,
		"CloudSQL.InstancesList":    p.InstancesList,
		"CloudSQL.InstancesUpdate":  p.InstancesUpdate,
		"CloudSQL.InstancesPatch":   p.InstancesPatch,
		"CloudSQL.InstancesDelete":  p.InstancesDelete,
		"CloudSQL.InstancesRestart": p.InstancesRestart,

		"CloudSQL.DatabasesInsert": p.DatabasesInsert,
		"CloudSQL.DatabasesGet":    p.DatabasesGet,
		"CloudSQL.DatabasesList":   p.DatabasesList,
		"CloudSQL.DatabasesUpdate": p.DatabasesUpdate,
		"CloudSQL.DatabasesPatch":  p.DatabasesPatch,
		"CloudSQL.DatabasesDelete": p.DatabasesDelete,

		"CloudSQL.UsersInsert": p.UsersInsert,
		"CloudSQL.UsersGet":    p.UsersGet,
		"CloudSQL.UsersList":   p.UsersList,
		"CloudSQL.UsersUpdate": p.UsersUpdate,
		"CloudSQL.UsersDelete": p.UsersDelete,

		"CloudSQL.OperationsGet":  p.OperationsGet,
		"CloudSQL.OperationsList": p.OperationsList,

		"CloudSQL.FlagsList":     p.ListFlags,
		"CloudSQL.TiersList":     p.ListTiers,
		"CloudSQL.ConnectGet":    p.GetConnectSettings,
		"CloudSQL.Unimplemented": p.Unimplemented,
	}
}

// ─── Instance operations ──────────────────────────────────────────────────────

func (p *Provider) InstancesInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyMap(nr)
	inst := cloneMap(body)
	name := stringField(inst, "name")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "instance name is required", 400)
	}

	project := accountID(nr)
	region := stringField(inst, "region")
	if region == "" {
		region = defaultRegion
	}
	inst["kind"] = kindInstance
	inst["project"] = project
	inst["region"] = region
	if stringField(inst, "databaseVersion") == "" {
		inst["databaseVersion"] = defaultDatabaseVersion
	}
	inst["state"] = "RUNNABLE"
	inst["instanceType"] = "CLOUD_SQL_INSTANCE"
	inst["backendType"] = "SECOND_GEN"
	if stringField(inst, "gceZone") == "" {
		inst["gceZone"] = region + "-a"
	}
	inst["connectionName"] = project + ":" + region + ":" + name
	inst["selfLink"] = sqlBasePath + resourcePath(nr, "cloudsql-instance", name)
	inst["serviceAccountEmailAddress"] = serviceAccountEmail(project)
	inst["createTime"] = formatTimestamp(clock.Now())
	inst["etag"] = contentEtag(name)
	inst["ipAddresses"] = []any{map[string]any{
		"type":      "PRIMARY",
		"ipAddress": synthIP(project + "/" + name),
	}}
	inst["settings"] = normalizeSettings(inst["settings"], region+"-a")

	data, err := json.Marshal(inst)
	if err != nil {
		return nil, err
	}
	err = p.resources.Create(ctx, project, store.GlobalRegion, store.ResourceEntry{Type: rtInstance, ID: name, Data: data})
	if errors.Is(err, store.ErrAlreadyExists) {
		return nil, model.NewProviderError("AlreadyExists", fmt.Sprintf("instance %q already exists", name), 409)
	}
	if err != nil {
		return nil, err
	}
	return provider.OK(p.recordOperation(ctx, nr, "CREATE", name, stringField(inst, "selfLink"))), nil
}

func (p *Provider) InstancesGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "instance")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing instance", 400)
	}
	inst, err := p.loadInstance(ctx, accountID(nr), name)
	if err != nil {
		return nil, err
	}
	return provider.OK(inst), nil
}

func (p *Provider) InstancesList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, accountID(nr), store.GlobalRegion, rtInstance, "")
	if err != nil {
		return nil, err
	}
	instances := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var m map[string]any
		if json.Unmarshal(e.Data, &m) == nil {
			instances = append(instances, m)
		}
	}
	page, next := paginate(instances, func(m map[string]any) string { return stringField(m, "name") }, nr.Params)
	items := make([]any, 0, len(page))
	for _, m := range page {
		items = append(items, m)
	}
	resp := map[string]any{"kind": kindInstancesList, "items": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) InstancesUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.applyInstanceUpdate(ctx, nr)
}

func (p *Provider) InstancesPatch(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.applyInstanceUpdate(ctx, nr)
}

// applyInstanceUpdate merges the mutable instance fields from the request body.
// region and the server-owned output fields are immutable/ignored; settings are
// deep-merged so a patch touching one sub-field retains the rest.
func (p *Provider) applyInstanceUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "instance")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing instance", 400)
	}
	body := bodyMap(nr)
	updated, err := p.resources.UpsertAtomic(ctx, accountID(nr), store.GlobalRegion, rtInstance, name, func(current store.ResourceEntry, exists bool) (store.ResourceEntry, error) {
		if !exists {
			return store.ResourceEntry{}, store.ErrNotFound
		}
		var inst map[string]any
		if err := json.Unmarshal(current.Data, &inst); err != nil {
			return store.ResourceEntry{}, err
		}
		mergeInstance(inst, body)
		inst["state"] = "RUNNABLE"
		data, err := json.Marshal(inst)
		if err != nil {
			return store.ResourceEntry{}, err
		}
		current.Data = data
		return current, nil
	})
	if err != nil {
		return nil, storeError(err, "instance")
	}
	var inst map[string]any
	if err := json.Unmarshal(updated.Data, &inst); err != nil {
		return nil, err
	}
	return provider.OK(p.recordOperation(ctx, nr, "UPDATE", name, stringField(inst, "selfLink"))), nil
}

func (p *Provider) InstancesDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "instance")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing instance", 400)
	}
	inst, err := p.loadInstance(ctx, accountID(nr), name)
	if err != nil {
		return nil, err
	}
	if err := p.resources.Delete(ctx, accountID(nr), store.GlobalRegion, rtInstance, name); err != nil {
		return nil, storeError(err, "instance")
	}
	p.purgeInstanceChildren(ctx, accountID(nr), name)
	return provider.OK(p.recordOperation(ctx, nr, "DELETE", name, stringField(inst, "selfLink"))), nil
}

func (p *Provider) InstancesRestart(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "instance")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing instance", 400)
	}
	inst, err := p.loadInstance(ctx, accountID(nr), name)
	if err != nil {
		return nil, err
	}
	return provider.OK(p.recordOperation(ctx, nr, "RESTART", name, stringField(inst, "selfLink"))), nil
}

func (p *Provider) loadInstance(ctx context.Context, account, name string) (map[string]any, error) {
	e, err := p.resources.Get(ctx, account, store.GlobalRegion, rtInstance, name)
	if err != nil {
		return nil, storeError(err, "instance")
	}
	var m map[string]any
	if err := json.Unmarshal(e.Data, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// purgeInstanceChildren removes the databases and users owned by an instance.
func (p *Provider) purgeInstanceChildren(ctx context.Context, account, instance string) {
	for _, rt := range []string{rtDatabase, rtUser} {
		if entries, err := p.resources.List(ctx, account, store.GlobalRegion, rt, instance); err == nil {
			for _, e := range entries {
				_ = p.resources.Delete(ctx, account, store.GlobalRegion, rt, e.ID)
			}
		}
	}
}

// ─── Database operations ──────────────────────────────────────────────────────

func (p *Provider) DatabasesInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	instance := strParam(nr, "instance")
	if instance == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing instance", 400)
	}
	if _, err := p.loadInstance(ctx, accountID(nr), instance); err != nil {
		return nil, err
	}
	db := cloneMap(bodyMap(nr))
	name := stringField(db, "name")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "database name is required", 400)
	}
	project := accountID(nr)
	db["kind"] = kindDatabase
	db["instance"] = instance
	db["project"] = project
	db["name"] = name
	db["selfLink"] = sqlBasePath + resourcePath(nr, "cloudsql-database", instance+"/"+name)

	data, err := json.Marshal(db)
	if err != nil {
		return nil, err
	}
	err = p.resources.Create(ctx, project, store.GlobalRegion, store.ResourceEntry{Type: rtDatabase, ID: databaseKey(instance, name), Data: data})
	if errors.Is(err, store.ErrAlreadyExists) {
		return nil, model.NewProviderError("AlreadyExists", fmt.Sprintf("database %q already exists", name), 409)
	}
	if err != nil {
		return nil, err
	}
	return provider.OK(p.recordOperation(ctx, nr, "CREATE_DATABASE", instance, stringField(db, "selfLink"))), nil
}

func (p *Provider) DatabasesGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	instance, name := strParam(nr, "instance"), strParam(nr, "database")
	if instance == "" || name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing instance or database", 400)
	}
	db, err := p.loadDatabase(ctx, accountID(nr), instance, name)
	if err != nil {
		return nil, err
	}
	return provider.OK(db), nil
}

func (p *Provider) DatabasesList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	instance := strParam(nr, "instance")
	if instance == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing instance", 400)
	}
	if _, err := p.loadInstance(ctx, accountID(nr), instance); err != nil {
		return nil, err
	}
	entries, err := p.resources.List(ctx, accountID(nr), store.GlobalRegion, rtDatabase, instance)
	if err != nil {
		return nil, err
	}
	dbs := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var m map[string]any
		if json.Unmarshal(e.Data, &m) == nil && stringField(m, "instance") == instance {
			dbs = append(dbs, m)
		}
	}
	page, next := paginate(dbs, func(m map[string]any) string { return stringField(m, "name") }, nr.Params)
	items := make([]any, 0, len(page))
	for _, m := range page {
		items = append(items, m)
	}
	resp := map[string]any{"kind": kindDatabasesList, "items": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) DatabasesUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.applyDatabaseUpdate(ctx, nr)
}

func (p *Provider) DatabasesPatch(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.applyDatabaseUpdate(ctx, nr)
}

func (p *Provider) applyDatabaseUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	instance, name := strParam(nr, "instance"), strParam(nr, "database")
	if instance == "" || name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing instance or database", 400)
	}
	body := bodyMap(nr)
	updated, err := p.resources.UpsertAtomic(ctx, accountID(nr), store.GlobalRegion, rtDatabase, databaseKey(instance, name), func(current store.ResourceEntry, exists bool) (store.ResourceEntry, error) {
		if !exists {
			return store.ResourceEntry{}, store.ErrNotFound
		}
		var db map[string]any
		if err := json.Unmarshal(current.Data, &db); err != nil {
			return store.ResourceEntry{}, err
		}
		for _, k := range []string{"charset", "collation"} {
			if v, ok := body[k]; ok {
				db[k] = v
			}
		}
		if s, ok := body["sqlserverDatabaseDetails"]; ok {
			db["sqlserverDatabaseDetails"] = s
		}
		data, err := json.Marshal(db)
		if err != nil {
			return store.ResourceEntry{}, err
		}
		current.Data = data
		return current, nil
	})
	if err != nil {
		return nil, storeError(err, "database")
	}
	var db map[string]any
	if err := json.Unmarshal(updated.Data, &db); err != nil {
		return nil, err
	}
	return provider.OK(p.recordOperation(ctx, nr, "UPDATE_DATABASE", instance, stringField(db, "selfLink"))), nil
}

func (p *Provider) DatabasesDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	instance, name := strParam(nr, "instance"), strParam(nr, "database")
	if instance == "" || name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing instance or database", 400)
	}
	db, err := p.loadDatabase(ctx, accountID(nr), instance, name)
	if err != nil {
		return nil, err
	}
	if err := p.resources.Delete(ctx, accountID(nr), store.GlobalRegion, rtDatabase, databaseKey(instance, name)); err != nil {
		return nil, storeError(err, "database")
	}
	return provider.OK(p.recordOperation(ctx, nr, "DELETE_DATABASE", instance, stringField(db, "selfLink"))), nil
}

func (p *Provider) loadDatabase(ctx context.Context, account, instance, name string) (map[string]any, error) {
	e, err := p.resources.Get(ctx, account, store.GlobalRegion, rtDatabase, databaseKey(instance, name))
	if err != nil {
		return nil, storeError(err, "database")
	}
	var m map[string]any
	if err := json.Unmarshal(e.Data, &m); err != nil {
		return nil, err
	}
	if stringField(m, "instance") != instance {
		return nil, model.NewProviderError("NotFound", "database not found", 404)
	}
	return m, nil
}

func databaseKey(instance, name string) string { return instance + "/" + name }

// ─── User operations ──────────────────────────────────────────────────────────

func (p *Provider) UsersInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	instance := strParam(nr, "instance")
	if instance == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing instance", 400)
	}
	if _, err := p.loadInstance(ctx, accountID(nr), instance); err != nil {
		return nil, err
	}
	user := cloneMap(bodyMap(nr))
	name := stringField(user, "name")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "user name is required", 400)
	}
	host := stringField(user, "host")
	if stringField(user, "type") == "" {
		user["type"] = "BUILT_IN"
	}
	delete(user, "password")
	user["kind"] = kindUser
	user["instance"] = instance
	user["project"] = accountID(nr)
	user["name"] = name

	data, err := json.Marshal(user)
	if err != nil {
		return nil, err
	}
	key := userKey(instance, name, host)
	err = p.resources.Create(ctx, accountID(nr), store.GlobalRegion, store.ResourceEntry{Type: rtUser, ID: key, Data: data})
	if errors.Is(err, store.ErrAlreadyExists) {
		return nil, model.NewProviderError("AlreadyExists", fmt.Sprintf("user %q already exists", name), 409)
	}
	if err != nil {
		return nil, err
	}
	return provider.OK(p.recordOperation(ctx, nr, "CREATE_USER", instance, instanceLink(nr, instance))), nil
}

func (p *Provider) UsersGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	instance, name, host := userParams(nr)
	if instance == "" || name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing instance or user name", 400)
	}
	user, err := p.findUser(ctx, accountID(nr), instance, name, host)
	if err != nil {
		return nil, err
	}
	return provider.OK(user), nil
}

func (p *Provider) UsersList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	instance := strParam(nr, "instance")
	if instance == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing instance", 400)
	}
	if _, err := p.loadInstance(ctx, accountID(nr), instance); err != nil {
		return nil, err
	}
	users, err := p.listUsers(ctx, accountID(nr), instance)
	if err != nil {
		return nil, err
	}
	page, next := paginate(users, func(m map[string]any) string { return stringField(m, "name") + "/" + stringField(m, "host") }, nr.Params)
	items := make([]any, 0, len(page))
	for _, m := range page {
		items = append(items, m)
	}
	resp := map[string]any{"kind": kindUsersList, "items": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) UsersUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	instance := strParam(nr, "instance")
	if instance == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing instance", 400)
	}
	body := bodyMap(nr)
	name := stringField(body, "name")
	if name == "" {
		name = userParamsName(nr)
	}
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "user name is required", 400)
	}
	host := stringField(body, "host")

	key := userKey(instance, name, host)
	updated, err := p.resources.UpsertAtomic(ctx, accountID(nr), store.GlobalRegion, rtUser, key, func(current store.ResourceEntry, exists bool) (store.ResourceEntry, error) {
		if !exists {
			return store.ResourceEntry{}, store.ErrNotFound
		}
		var user map[string]any
		if err := json.Unmarshal(current.Data, &user); err != nil {
			return store.ResourceEntry{}, err
		}
		if v := stringField(body, "type"); v != "" {
			user["type"] = v
		}
		if v := stringField(body, "dualPasswordType"); v != "" {
			user["dualPasswordType"] = v
		}
		if v, ok := body["passwordPolicy"]; ok {
			user["passwordPolicy"] = v
		}
		if v, ok := body["sqlserverUserDetails"]; ok {
			user["sqlserverUserDetails"] = v
		}
		delete(user, "password")
		data, err := json.Marshal(user)
		if err != nil {
			return store.ResourceEntry{}, err
		}
		current.Data = data
		return current, nil
	})
	if err != nil {
		return nil, storeError(err, "user")
	}
	var user map[string]any
	if err := json.Unmarshal(updated.Data, &user); err != nil {
		return nil, err
	}
	return provider.OK(p.recordOperation(ctx, nr, "UPDATE_USER", instance, instanceLink(nr, instance))), nil
}

func (p *Provider) UsersDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	instance, name, host := userParams(nr)
	if instance == "" || name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing instance or user name", 400)
	}
	user, err := p.findUser(ctx, accountID(nr), instance, name, host)
	if err != nil {
		return nil, err
	}
	key := userKey(instance, name, stringField(user, "host"))
	if err := p.resources.Delete(ctx, accountID(nr), store.GlobalRegion, rtUser, key); err != nil {
		return nil, storeError(err, "user")
	}
	return provider.OK(p.recordOperation(ctx, nr, "DELETE_USER", instance, instanceLink(nr, instance))), nil
}

func (p *Provider) listUsers(ctx context.Context, account, instance string) ([]map[string]any, error) {
	entries, err := p.resources.List(ctx, account, store.GlobalRegion, rtUser, instance)
	if err != nil {
		return nil, err
	}
	users := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var m map[string]any
		if json.Unmarshal(e.Data, &m) == nil && stringField(m, "instance") == instance {
			users = append(users, m)
		}
	}
	return users, nil
}

func (p *Provider) findUser(ctx context.Context, account, instance, name, host string) (map[string]any, error) {
	users, err := p.listUsers(ctx, account, instance)
	if err != nil {
		return nil, err
	}
	for _, u := range users {
		if stringField(u, "name") == name && (host == "" || stringField(u, "host") == host) {
			return u, nil
		}
	}
	return nil, model.NewProviderError("NotFound", "user not found", 404)
}

// userParams returns (instance, name, host) for a user request. Users.Get uses
// the path-segment "user", while Users.Delete carries the name/host as query
// parameters; both are accepted.
func userParams(nr *model.NormalizedRequest) (instance, name, host string) {
	instance = strParam(nr, "instance")
	name = strParam(nr, "user")
	if name == "" {
		name = strParam(nr, "name")
	}
	host = strParam(nr, "host")
	return instance, name, host
}

func userParamsName(nr *model.NormalizedRequest) string {
	if n := strParam(nr, "user"); n != "" {
		return n
	}
	return strParam(nr, "name")
}

func userKey(instance, name, host string) string { return instance + "/" + name + "/" + host }

// ─── Operation operations ─────────────────────────────────────────────────────

func (p *Provider) OperationsGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	opID := strParam(nr, "operation")
	if opID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing operation", 400)
	}
	e, err := p.resources.Get(ctx, accountID(nr), store.GlobalRegion, rtOperation, opID)
	if err != nil {
		return nil, storeError(err, "operation")
	}
	var m map[string]any
	if err := json.Unmarshal(e.Data, &m); err != nil {
		return nil, err
	}
	return provider.OK(m), nil
}

func (p *Provider) OperationsList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, accountID(nr), store.GlobalRegion, rtOperation, "")
	if err != nil {
		return nil, err
	}
	ops := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var m map[string]any
		if json.Unmarshal(e.Data, &m) == nil {
			ops = append(ops, m)
		}
	}
	page, next := paginate(ops, func(m map[string]any) string { return stringField(m, "name") }, nr.Params)
	items := make([]any, 0, len(page))
	for _, m := range page {
		items = append(items, m)
	}
	resp := map[string]any{"kind": kindOperationsList, "items": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

// recordOperation builds Cloud SQL's own Operation envelope (sql#operation)
// with status DONE, persists it so operations.get/list can read it back, and
// returns it for the caller to embed in the mutation response.
func (p *Provider) recordOperation(ctx context.Context, nr *model.NormalizedRequest, opType, targetID, targetLink string) map[string]any {
	project := accountID(nr)
	opID := randomHex(16)
	now := formatTimestamp(clock.Now())
	op := map[string]any{
		"kind":          kindOperation,
		"name":          opID,
		"operationType": opType,
		"status":        "DONE",
		"targetId":      targetID,
		"targetLink":    targetLink,
		"targetProject": project,
		"selfLink":      sqlBasePath + resourcePath(nr, "cloudsql-operation", opID),
		"insertTime":    now,
		"startTime":     now,
		"endTime":       now,
		"user":          serviceAccountEmail(project),
	}
	data, err := json.Marshal(op)
	if err == nil {
		_ = p.resources.Upsert(ctx, project, store.GlobalRegion, store.ResourceEntry{Type: rtOperation, ID: opID, Data: data})
	}
	return op
}

// ─── Discovery surfaces ───────────────────────────────────────────────────────

// postgresVersions are the PostgreSQL major versions the emulator advertises.
var postgresVersions = []string{"POSTGRES_15", "POSTGRES_16", "POSTGRES_17", "POSTGRES_18"}

// sqlFlags is a small, stable catalogue of real Cloud SQL database flags. It is
// static (there is no backend), but the names/types/appliesTo mirror flags real
// GCP returns so clients that pre-validate settings succeed.
var sqlFlags = []map[string]any{
	{"name": "character_set_server", "type": "STRING", "appliesTo": []string{"MYSQL_8_0", "MYSQL_5_7"}, "requiresRestart": false},
	{"name": "collation_server", "type": "STRING", "appliesTo": []string{"MYSQL_8_0", "MYSQL_5_7"}, "requiresRestart": false},
	{"name": "default_time_zone", "type": "STRING", "appliesTo": []string{"MYSQL_8_0"}, "requiresRestart": false},
	{"name": "log_output", "type": "STRING", "appliesTo": []string{"MYSQL_8_0"}, "allowedStringValues": []string{"TABLE", "FILE", "NONE"}, "requiresRestart": false},
	{"name": "max_connections", "type": "INTEGER", "appliesTo": append([]string{"MYSQL_8_0"}, postgresVersions...), "minValue": "1", "maxValue": "100000", "requiresRestart": false},
	{"name": "slow_query_log", "type": "BOOLEAN", "appliesTo": []string{"MYSQL_8_0"}, "requiresRestart": false},
	{"name": "cloudsql.iam_authentication", "type": "BOOLEAN", "appliesTo": postgresVersions, "requiresRestart": true},
	{"name": "log_min_duration_statement", "type": "INTEGER", "appliesTo": postgresVersions, "minValue": "-1", "maxValue": "2147483647", "requiresRestart": false},
}

func (p *Provider) ListFlags(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// Build a fresh slice of copies: paginate sorts in place, and the catalogue
	// is a shared package-level value that concurrent requests must not mutate.
	flags := make([]map[string]any, 0, len(sqlFlags))
	for _, f := range sqlFlags {
		item := cloneMap(f)
		item["kind"] = "sql#flag"
		flags = append(flags, item)
	}
	page, nextPageToken := paginate(flags, func(f map[string]any) string { return stringField(f, "name") }, nr.Params)
	items := make([]any, 0, len(page))
	for _, f := range page {
		items = append(items, f)
	}
	resp := map[string]any{"kind": kindFlagsList, "items": items}
	if nextPageToken != "" {
		resp["nextPageToken"] = nextPageToken
	}
	return provider.OK(resp), nil
}

// sqlTier is the implicit machine tier applied when a create/update omits
// settings.tier; it is also the smallest entry in the static tier catalogue.
const sqlTier = "db-f1-micro"

// sqlTierSpec is one entry in the static machine-tier catalogue.
type sqlTierSpec struct {
	name  string
	ramMB string
}

// sqlTiers is the static machine-tier catalogue tiers.list returns. Real Cloud
// SQL's catalogue is region-specific and far larger; this covers the shared-core
// and standard/highmem tiers clients commonly pick plus the predefined custom
// machine types (db-custom-{vcpu}-{memoryMB}) the SDKs and console resolve.
var sqlTiers = []sqlTierSpec{
	{sqlTier, "614"},
	{"db-g1-small", "1740"},
	{"db-n1-standard-1", "3840"},
	{"db-n1-standard-2", "7680"},
	{"db-n1-standard-4", "15360"},
	{"db-n1-standard-8", "30720"},
	{"db-n1-standard-16", "61440"},
	{"db-n1-highmem-2", "13312"},
	{"db-n1-highmem-4", "26624"},
	{"db-n1-highmem-8", "53248"},
	{"db-custom-1-3840", "3840"},
	{"db-custom-2-7680", "7680"},
	{"db-custom-4-15360", "15360"},
	{"db-custom-8-30720", "30720"},
}

// sqlTierRegions are the regions the catalogue advertises each tier in.
var sqlTierRegions = []string{"us-central1", "us-east1", "europe-west1", "asia-east1"}

func (p *Provider) ListTiers(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	tiers := make([]map[string]any, 0, len(sqlTiers))
	for _, t := range sqlTiers {
		tiers = append(tiers, map[string]any{
			"kind":      "sql#tier",
			"tier":      t.name,
			"region":    sqlTierRegions,
			"RAM":       t.ramMB,
			"DiskQuota": "3072",
		})
	}
	page, nextPageToken := paginate(tiers, func(t map[string]any) string { return stringField(t, "tier") }, nr.Params)
	items := make([]any, 0, len(page))
	for _, t := range page {
		items = append(items, t)
	}
	resp := map[string]any{"kind": kindTiersList, "items": items}
	if nextPageToken != "" {
		resp["nextPageToken"] = nextPageToken
	}
	return provider.OK(resp), nil
}

func (p *Provider) GetConnectSettings(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	instance := strParam(nr, "instance")
	if instance == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing instance", 400)
	}
	inst, err := p.loadInstance(ctx, accountID(nr), instance)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{
		"kind":            kindConnectSettings,
		"backendType":     stringField(inst, "backendType"),
		"databaseVersion": stringField(inst, "databaseVersion"),
		"region":          stringField(inst, "region"),
	}
	if ips, ok := inst["ipAddresses"]; ok {
		resp["ipAddresses"] = ips
	}
	return provider.OK(resp), nil
}

// Unimplemented is the fail-loud handler for deferred Cloud SQL surfaces
// (sslCerts, backupRuns, clone, failover, promoteReplica, import/export,
// demote, resetSslConfig, and operation cancel).
func (p *Provider) Unimplemented(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return nil, model.NewProviderError("Unimplemented", "operation is not supported by the Cloud SQL emulator", 501)
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

func bodyMap(nr *model.NormalizedRequest) map[string]any {
	m, _ := nr.Params["body"].(map[string]any)
	return m
}

func stringField(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	s, _ := m[key].(string)
	return s
}

func accountID(nr *model.NormalizedRequest) string {
	if nr.AccountID != "" {
		return nr.AccountID
	}
	if p := strParam(nr, "project"); p != "" {
		return p
	}
	return "jaiscloud-project"
}

// resourcePath formats a relative resource name through the injected
// formatter, falling back to the raw name when none was injected.
func resourcePath(nr *model.NormalizedRequest, rt, name string) string {
	if nr.ResourceID != nil {
		return nr.ResourceID(rt, name)
	}
	return name
}

func instanceLink(nr *model.NormalizedRequest, instance string) string {
	return sqlBasePath + resourcePath(nr, "cloudsql-instance", instance)
}

// cloneMap returns a shallow copy of m so a handler never mutates the parsed
// request body in place.
func cloneMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// mergeInstance overlays the mutable fields of patch onto inst. Region and
// server-owned output fields are immutable and ignored; settings are
// deep-merged so a patch touching one sub-field retains the rest.
func mergeInstance(inst, patch map[string]any) {
	for k, v := range patch {
		switch k {
		case "name", "project", "region", "kind", "state", "selfLink",
			"connectionName", "createTime", "serviceAccountEmailAddress",
			"ipAddresses", "backendType", "instanceType", "currentDiskSize",
			"databaseInstalledVersion", "etag":
			continue
		case "settings":
			inst["settings"] = mergeSettings(inst["settings"], v)
		default:
			inst[k] = v
		}
	}
}

// mergeSettings overlays the patch settings onto the stored settings, field by
// field, so unspecified sub-fields are retained.
func mergeSettings(stored, patch any) map[string]any {
	out, _ := stored.(map[string]any)
	if out == nil {
		out = map[string]any{}
	}
	p, _ := patch.(map[string]any)
	for k, v := range p {
		out[k] = v
	}
	return out
}

// normalizeSettings fills the default settings sub-fields. The zone argument is
// the instance's gceZone; when omitted the stored locationPreference is kept.
func normalizeSettings(v any, zone string) map[string]any {
	s, _ := v.(map[string]any)
	if s == nil {
		s = map[string]any{}
	}
	normalizeSettingsInPlace(s, zone)
	return s
}

func normalizeSettingsInPlace(s map[string]any, zone string) {
	if stringField(s, "tier") == "" {
		s["tier"] = sqlTier
	}
	if _, ok := s["dataDiskSizeGb"]; !ok {
		s["dataDiskSizeGb"] = "10"
	} else {
		s["dataDiskSizeGb"] = stringifyInt(s["dataDiskSizeGb"])
	}
	if stringField(s, "availabilityType") == "" {
		s["availabilityType"] = "ZONAL"
	}
	if stringField(s, "activationPolicy") == "" {
		s["activationPolicy"] = "ALWAYS"
	}
	if stringField(s, "dataDiskType") == "" {
		s["dataDiskType"] = "PD_SSD"
	}
	if stringField(s, "pricingPlan") == "" {
		s["pricingPlan"] = "PER_USE"
	}
	if stringField(s, "replicationType") == "" {
		s["replicationType"] = "SYNCHRONOUS"
	}
	if _, ok := s["ipConfiguration"]; !ok {
		s["ipConfiguration"] = map[string]any{"ipv4Enabled": true}
	}
	if zone != "" {
		if _, ok := s["locationPreference"]; !ok {
			s["locationPreference"] = map[string]any{
				"kind": "sql#locationPreference",
				"zone": zone,
			}
		}
	}
}

// stringifyInt renders a JSON scalar as a decimal string, preserving the
// int64-as-string wire encoding Cloud SQL uses for fields such as
// settings.dataDiskSizeGb.
func stringifyInt(v any) string {
	switch n := v.(type) {
	case string:
		return n
	case float64:
		return strconv.FormatInt(int64(n), 10)
	case int:
		return strconv.Itoa(n)
	case int64:
		return strconv.FormatInt(n, 10)
	}
	return fmt.Sprint(v)
}

// storeError translates store sentinels into GCP ProviderErrors.
func storeError(err error, what string) error {
	switch {
	case errors.Is(err, store.ErrNotFound):
		return model.NewProviderError("NotFound", what+" not found", 404)
	case errors.Is(err, store.ErrAlreadyExists):
		return model.NewProviderError("AlreadyExists", what+" already exists", 409)
	}
	return err
}

func formatTimestamp(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// contentEtag derives a stable etag from a resource name.
func contentEtag(seed string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(seed))
	return strconv.FormatUint(h.Sum64(), 16)
}

// serviceAccountEmail synthesizes the instance service account email.
func serviceAccountEmail(project string) string {
	return "p" + numericID(project) + "@gcp-sa-cloud-sql.iam.gserviceaccount.com"
}

// synthIP derives a stable cosmetic IPv4 address from a seed. Nothing listens
// on it; a real Cloud SQL public IP is allocated from Google's ranges.
func synthIP(seed string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(seed))
	n := h.Sum32()
	return fmt.Sprintf("34.%d.%d.%d", (n>>16)&0xff, (n>>8)&0xff, n&0xff)
}

// numericID derives a stable decimal id from a string.
func numericID(s string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	n := h.Sum64()
	if n == 0 {
		n = 1
	}
	return strconv.FormatUint(n, 10)
}

func randomHex(n int) string {
	b := make([]byte, (n+1)/2)
	if _, err := rand.Read(b); err != nil {
		return strings.Repeat("0", n)
	}
	return hex.EncodeToString(b)[:n]
}

// paginate sorts items by key and applies Cloud SQL's maxResults/pageToken
// cursor pagination. The next page token is the base64url-encoded key of the
// last returned item.
func paginate[T any](items []T, key func(T) string, params map[string]any) (page []T, nextPageToken string) {
	sort.Slice(items, func(i, j int) bool { return key(items[i]) < key(items[j]) })
	size := maxResults(params)
	start := 0
	if tok, _ := params["pageToken"].(string); tok != "" {
		if raw, err := base64.RawURLEncoding.DecodeString(tok); err == nil {
			cursor := string(raw)
			for start < len(items) && key(items[start]) <= cursor {
				start++
			}
		}
	}
	end := start + size
	if end > len(items) {
		end = len(items)
	}
	page = items[start:end]
	if end < len(items) {
		nextPageToken = base64.RawURLEncoding.EncodeToString([]byte(key(items[end-1])))
	}
	return page, nextPageToken
}

// maxResults reads Cloud SQL's maxResults query parameter, falling back to
// pageSize and the default of 100.
func maxResults(params map[string]any) int {
	for _, k := range []string{"maxResults", "pageSize"} {
		switch v := params[k].(type) {
		case string:
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				return n
			}
		case int:
			if v > 0 {
				return v
			}
		case float64:
			if v > 0 {
				return int(v)
			}
		}
	}
	return 100
}
