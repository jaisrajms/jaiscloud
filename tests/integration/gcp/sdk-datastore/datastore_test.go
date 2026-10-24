package datastore_test

import (
	"context"
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
)

type Task struct {
	Description string
	Done        bool
	Priority    int
}

func TestDatastore(t *testing.T) {
	ctx := context.Background()
	client := DatastoreClient(ctx)
	defer client.Close()

	suffix := uniqueName("go-ds")

	t.Run("PutAndGet", func(t *testing.T) {
		key := datastore.NameKey("Task", "task-"+suffix, nil)
		task := &Task{Description: "Buy groceries", Done: false, Priority: 4}

		_, err := client.Put(ctx, key, task)
		require.NoError(t, err)

		t.Cleanup(func() { client.Delete(ctx, key) })

		var got Task
		err = client.Get(ctx, key, &got)
		require.NoError(t, err)
		assert.Equal(t, "Buy groceries", got.Description)
		assert.False(t, got.Done)
		assert.Equal(t, 4, got.Priority)
	})

	t.Run("Query", func(t *testing.T) {
		key1 := datastore.NameKey("Task", "task1-"+suffix, nil)
		key2 := datastore.NameKey("Task", "task2-"+suffix, nil)

		client.Put(ctx, key1, &Task{Description: "Task A", Done: false})
		client.Put(ctx, key2, &Task{Description: "Task B", Done: true})

		t.Cleanup(func() {
			client.Delete(ctx, key1)
			client.Delete(ctx, key2)
		})

		var results []Task
		q := datastore.NewQuery("Task").FilterField("Done", "=", false)
		_, err := client.GetAll(ctx, q, &results)
		require.NoError(t, err)

		descriptions := make([]string, len(results))
		for i, r := range results {
			descriptions[i] = r.Description
		}
		assert.Contains(t, descriptions, "Task A")
	})

	// CursorPaging exercises the official client's cursor paging path: a
	// limited query returns MORE_RESULTS_AFTER_LIMIT + an end cursor, and
	// resuming from that cursor must return the next entities (AUD6-6).
	t.Run("CursorPaging", func(t *testing.T) {
		kind := "PagingTask-" + suffix
		names := []string{"page-a-" + suffix, "page-b-" + suffix, "page-c-" + suffix}
		for i, name := range names {
			_, err := client.Put(ctx, datastore.NameKey(kind, name, nil), &Task{Description: name, Priority: i})
			require.NoError(t, err)
		}
		t.Cleanup(func() {
			for _, name := range names {
				client.Delete(ctx, datastore.NameKey(kind, name, nil))
			}
		})

		it := client.Run(ctx, datastore.NewQuery(kind).Limit(2))
		var first []string
		for len(first) < 2 {
			var task Task
			_, err := it.Next(&task)
			require.NoError(t, err)
			first = append(first, task.Description)
		}
		assert.Equal(t, names[:2], first)

		cursor, err := it.Cursor()
		require.NoError(t, err)

		var rest []string
		it2 := client.Run(ctx, datastore.NewQuery(kind).Start(cursor))
		for {
			var task Task
			_, err := it2.Next(&task)
			if err == iterator.Done {
				break
			}
			require.NoError(t, err)
			rest = append(rest, task.Description)
		}
		assert.Equal(t, names[2:], rest)
	})

	t.Run("Update", func(t *testing.T) {
		key := datastore.NameKey("Task", "update-"+suffix, nil)
		_, err := client.Put(ctx, key, &Task{Description: "Original", Done: false})
		require.NoError(t, err)

		t.Cleanup(func() { client.Delete(ctx, key) })

		var task Task
		require.NoError(t, client.Get(ctx, key, &task))
		task.Done = true
		_, err = client.Put(ctx, key, &task)
		require.NoError(t, err)

		var updated Task
		require.NoError(t, client.Get(ctx, key, &updated))
		assert.True(t, updated.Done)
	})

	t.Run("Delete", func(t *testing.T) {
		key := datastore.NameKey("Task", "delete-"+suffix, nil)
		_, err := client.Put(ctx, key, &Task{Description: "Delete Me"})
		require.NoError(t, err)

		err = client.Delete(ctx, key)
		require.NoError(t, err)

		var task Task
		err = client.Get(ctx, key, &task)
		assert.ErrorIs(t, err, datastore.ErrNoSuchEntity)
	})

	t.Run("AllocateIDs", func(t *testing.T) {
		incomplete := datastore.IncompleteKey("Task", nil)
		keys, err := client.AllocateIDs(ctx, []*datastore.Key{incomplete})
		require.NoError(t, err)
		require.Len(t, keys, 1)
		assert.Greater(t, keys[0].ID, int64(0))
	})

	t.Run("AncestorKeys", func(t *testing.T) {
		parent := datastore.NameKey("TaskList", "list-"+suffix, nil)
		child := datastore.NameKey("Task", "child-"+suffix, parent)

		_, err := client.Put(ctx, child, &Task{Description: "child", Priority: 1})
		require.NoError(t, err)
		t.Cleanup(func() { client.Delete(ctx, child) })

		var got Task
		require.NoError(t, client.Get(ctx, child, &got))
		assert.Equal(t, "child", got.Description)

		// An ancestor query returns the descendant.
		var results []Task
		q := datastore.NewQuery("Task").Ancestor(parent).FilterField("Priority", "=", 1)
		_, err = client.GetAll(ctx, q, &results)
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, "child", results[0].Description)

		// A different ancestor does not match.
		other := datastore.NameKey("TaskList", "other-"+suffix, nil)
		results = nil
		_, err = client.GetAll(ctx, datastore.NewQuery("Task").Ancestor(other), &results)
		require.NoError(t, err)
		assert.Empty(t, results)

		// The same kind+name under a different parent is a distinct entity.
		sibling := datastore.NameKey("Task", "child-"+suffix, other)
		var missing Task
		assert.ErrorIs(t, client.Get(ctx, sibling, &missing), datastore.ErrNoSuchEntity)
	})

	t.Run("Namespace", func(t *testing.T) {
		ns := "ns-" + suffix
		key := datastore.NameKey("Task", "ns-"+suffix, nil)
		key.Namespace = ns

		_, err := client.Put(ctx, key, &Task{Description: "namespaced"})
		require.NoError(t, err)
		t.Cleanup(func() { client.Delete(ctx, key) })

		// The same kind+name in the default namespace is a different entity.
		def := datastore.NameKey("Task", "ns-"+suffix, nil)
		var got Task
		assert.ErrorIs(t, client.Get(ctx, def, &got), datastore.ErrNoSuchEntity)

		var results []Task
		_, err = client.GetAll(ctx, datastore.NewQuery("Task").Namespace(ns), &results)
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, "namespaced", results[0].Description)
	})

	t.Run("NamedDatabase", func(t *testing.T) {
		dbID := "db-" + suffix
		dbClient, err := datastore.NewClientWithDatabase(ctx, ProjectID(), dbID)
		require.NoError(t, err)
		defer dbClient.Close()

		key := datastore.NameKey("Task", "db-"+suffix, nil)
		_, err = dbClient.Put(ctx, key, &Task{Description: "in-db"})
		require.NoError(t, err)
		t.Cleanup(func() { dbClient.Delete(ctx, key) })

		var got Task
		require.NoError(t, dbClient.Get(ctx, key, &got))
		assert.Equal(t, "in-db", got.Description)

		// The same key in the default database is a different entity.
		var def Task
		assert.ErrorIs(t, client.Get(ctx, key, &def), datastore.ErrNoSuchEntity)
	})

	// ArrayFilters exercises the array-valued property semantics end to end
	// through the official client (TD28): a filter on an array property is
	// satisfied when any element compares to the value, so `Tags = "beta"`
	// matches an entity whose Tags is ["alpha","beta"], a range filter compares
	// elementwise, and `!=` matches because only one element need differ.
	t.Run("ArrayFilters", func(t *testing.T) {
		kind := "TagTask-" + suffix
		key := datastore.NameKey(kind, "tagged-"+suffix, nil)
		_, err := client.Put(ctx, key, &TagTask{Tags: []string{"alpha", "beta"}})
		require.NoError(t, err)
		t.Cleanup(func() { client.Delete(ctx, key) })

		soloKey := datastore.NameKey(kind, "solo-"+suffix, nil)
		_, err = client.Put(ctx, soloKey, &TagTask{Tags: []string{"alpha"}})
		require.NoError(t, err)
		t.Cleanup(func() { client.Delete(ctx, soloKey) })

		var got []TagTask
		_, err = client.GetAll(ctx, datastore.NewQuery(kind).FilterField("Tags", "=", "beta"), &got)
		require.NoError(t, err)
		require.Len(t, got, 1)

		got = nil
		_, err = client.GetAll(ctx, datastore.NewQuery(kind).FilterField("Tags", "=", "gamma"), &got)
		require.NoError(t, err)
		assert.Empty(t, got)

		got = nil
		_, err = client.GetAll(ctx, datastore.NewQuery(kind).FilterField("Tags", ">", "alpha"), &got)
		require.NoError(t, err)
		require.Len(t, got, 1)

		// `!= "alpha"` matches the two-tag entity (only one element need
		// differ) but excludes the single-tag one whose only value is "alpha".
		got = nil
		_, err = client.GetAll(ctx, datastore.NewQuery(kind).FilterField("Tags", "!=", "alpha"), &got)
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, []string{"alpha", "beta"}, got[0].Tags)
	})

	// KeyFilters exercises the documented key filters through the official
	// client (TD29): an entity has no stored "__key__" property, so equality
	// and range filters on it must resolve to the entity's own key.
	t.Run("KeyFilters", func(t *testing.T) {
		kind := "KeyFilterTask-" + suffix
		names := []string{"ka-" + suffix, "kb-" + suffix, "kc-" + suffix}
		numDesc := "kid-" + suffix
		keys := make([]*datastore.Key, 0, len(names)+1)
		for i, name := range names {
			k := datastore.NameKey(kind, name, nil)
			keys = append(keys, k)
			_, err := client.Put(ctx, k, &Task{Description: name, Priority: i})
			require.NoError(t, err)
		}
		// A numeric-ID key of the same kind: numeric IDs order before key names.
		numKey := datastore.IDKey(kind, 5, nil)
		keys = append(keys, numKey)
		_, err := client.Put(ctx, numKey, &Task{Description: numDesc})
		require.NoError(t, err)
		t.Cleanup(func() {
			for _, k := range keys {
				client.Delete(ctx, k)
			}
		})

		descriptions := func(op string, v any) []string {
			t.Helper()
			var got []Task
			_, err := client.GetAll(ctx, datastore.NewQuery(kind).FilterField("__key__", op, v), &got)
			require.NoError(t, err)
			out := make([]string, len(got))
			for i, g := range got {
				out[i] = g.Description
			}
			return out
		}

		// The names sort by byte value, so keys[1] splits the name-keyed set;
		// the numeric-ID entity is the lowest key and so is below it too.
		assert.Equal(t, []string{names[1]}, descriptions("=", keys[1]))
		assert.Equal(t, []string{names[2]}, descriptions(">", keys[1]))
		assert.ElementsMatch(t, []string{names[0], numDesc}, descriptions("<", keys[1]))
		assert.ElementsMatch(t, []string{names[0], names[2], numDesc}, descriptions("!=", keys[1]))

		// Every numeric ID precedes every key name, so the id-5 entity is the
		// lowest key in the kind and every name key is greater.
		assert.Equal(t, []string{numDesc}, descriptions("=", numKey))
		assert.Empty(t, descriptions("<", numKey))
		assert.ElementsMatch(t, names, descriptions(">", numKey))
	})
}

// TagTask is a kind whose property is array-valued, used by ArrayFilters.
type TagTask struct {
	Tags []string
}
