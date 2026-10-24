package storage

import (
	"context"
	"errors"
	"io"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/store/gcs"
	"jaiscloud/internal/model"
)

// Documented GCS reasons for the objects.restore failures that are not plain
// precondition/store errors. The REST codec renders them in
// error.errors[].reason (see GCSCodec.EncodeError) and the gRPC mapper keeps the
// same status code.
const (
	reasonObjectNotSoftDeleted   = "objectNotSoftDeleted"
	reasonSoftDeletePolicyAbsent = "SoftDeletePolicyRequired"
	reasonRetentionPolicyNotMet  = "retentionPolicyNotMet"
)

// RestoreObjectCore restores a soft-deleted (tombstoned) object generation. It
// is the transport-neutral core behind the REST objects.restore handler and the
// gRPC RestoreObject RPC.
//
// Per the JSON API contract a restore does not revive the old generation in
// place: it creates a new object copy with a fresh generation, metageneration
// reset to 1 and createTime set to the restore time, inheriting the tombstone's
// metadata (including its encryption envelope — the emulator copies the stored
// ciphertext verbatim, so restoring a CSEK/CMEK object never needs the key).
//
// Failure modes mirror the documented ones:
//
//   - 400 SoftDeletePolicyRequired — the bucket has soft delete disabled.
//   - 404 notFound — the generation does not exist (or was never soft deleted
//     long enough to survive).
//   - 412 objectNotSoftDeleted — the generation is live or noncurrent.
//   - 412 precondition — the ifGeneration*/ifMetageneration* checks failed.
//   - 403 retentionPolicyNotMet — a locked bucket retention policy still
//     protects the object.
//
// precondition is validated against the current live generation atomically with
// the metadata write (PutObject*Checked).
func (p *Provider) RestoreObjectCore(ctx context.Context, bucket, object, generation string, precondition *gcs.Precondition) (gcs.ObjectMeta, error) {
	bmeta, err := p.objects.GetBucket(ctx, bucket)
	if err != nil {
		return gcs.ObjectMeta{}, err // gcs.ErrNoSuchBucket → 404 by the caller
	}
	if softDeleteDisabled(bmeta) {
		return gcs.ObjectMeta{}, model.NewProviderError("InvalidArgument",
			"The bucket does not have a soft delete policy, so objects cannot be restored.", 400).
			WithData(map[string]any{"reason": reasonSoftDeletePolicyAbsent})
	}
	target, err := p.objects.GetObjectGeneration(ctx, bucket, object, generation)
	if err != nil {
		return gcs.ObjectMeta{}, err // gcs.ErrNoSuchObject → 404 by the caller
	}
	if !target.SoftDeleted {
		// Live and noncurrent (archived) generations are both rejected; only a
		// generation removed under the soft delete policy is restorable.
		return gcs.ObjectMeta{}, model.NewProviderError("PreconditionFailed",
			"The specified object is not soft-deleted and cannot be restored.", 412).
			WithData(map[string]any{"reason": reasonObjectNotSoftDeleted})
	}
	now := clock.Now()
	if !target.HardDeleteTime.IsZero() && !now.Before(target.HardDeleteTime) {
		// The soft-delete window closed: the copy no longer exists. Reclaim the
		// row and bytes here (the emulator purges lazily; there is no sweeper).
		_, _ = p.objects.DeleteObjectGeneration(ctx, bucket, object, target.Generation, nil)
		_ = p.blobs.Delete(ctx, blobsNamespace, blobKey(bucket, object, target.Generation))
		return gcs.ObjectMeta{}, model.NewProviderError("NotFound", "object not found", 404)
	}
	if err := lockedRetentionBlocksRestore(bmeta, target, now); err != nil {
		return gcs.ObjectMeta{}, err
	}

	project := p.bucketProject(ctx, bucket)
	newGeneration := p.nextGen()
	// Copy the stored ciphertext so the restored object keeps the tombstone's
	// envelope encryption (server DEK, CMEK or CSEK) without re-encrypting.
	oldBlob := blobKey(bucket, object, target.Generation)
	rc, err := p.blobs.GetStream(ctx, blobsNamespace, oldBlob, 0, -1)
	if err != nil {
		return gcs.ObjectMeta{}, model.NewProviderError("NotFound", "object not found", 404)
	}
	ciphertext, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return gcs.ObjectMeta{}, err
	}
	newBlob := blobKey(bucket, object, newGeneration)
	if err := p.blobs.Put(ctx, blobsNamespace, newBlob, ciphertext); err != nil {
		return gcs.ObjectMeta{}, err
	}

	meta := target
	meta.Generation = newGeneration
	meta.Metageneration = "1"
	meta.TimeCreated = now
	meta.Updated = now
	meta.TimeDeleted = nil
	meta.SoftDeleted = false
	meta.HardDeleteTime = time.Time{}
	// Soft-delete restore writes the new copy to Standard storage.
	meta.StorageClass = "STANDARD"

	// The live generation, if any, is superseded: archived when the bucket is
	// versioned, soft-deleted otherwise (which is what the write core does for
	// an overwrite).
	var prev *gcs.ObjectMeta
	if existing, gerr := p.objects.GetObjectMeta(ctx, bucket, object); gerr == nil && existing.Generation != newGeneration {
		if perr := objectWriteBlockedError(existing); perr != nil {
			_ = p.blobs.Delete(ctx, blobsNamespace, newBlob)
			return gcs.ObjectMeta{}, perr
		}
		prev = &existing
	}
	versioned := p.bucketVersioned(ctx, bucket)
	// A restore that overwrites a live object soft-deletes that object (the same
	// rule as an overwrite), so the replaced copy stays recoverable.
	var softPriorWindow time.Duration
	if !versioned && prev != nil {
		softPriorWindow = softDeleteWindow(bmeta)
	}
	err = p.putLiveObject(ctx, meta, versioned, softPriorWindow, precondition)
	if err != nil {
		_ = p.blobs.Delete(ctx, blobsNamespace, newBlob)
		return gcs.ObjectMeta{}, err
	}

	if prev != nil {
		replacedEvent := "OBJECT_DELETE"
		if versioned {
			replacedEvent = "OBJECT_ARCHIVE"
		} else if softPriorWindow == 0 {
			_ = p.blobs.Delete(ctx, blobsNamespace, blobKey(bucket, object, prev.Generation))
		}
		p.publishObjectEvent(ctx, project, bucket, object, replacedEvent, *prev, prev.Updated,
			map[string]string{"overwrittenByGeneration": meta.Generation})
	}
	p.publishObjectEvent(ctx, project, bucket, object, "OBJECT_FINALIZE", meta, now,
		map[string]string{"restoredGeneration": target.Generation})

	// The soft-deleted source is NOT consumed: real GCS keeps it for the rest of
	// its retention window, so the bucket holds the new live copy alongside the
	// original soft-deleted one (and a second restore of the same generation
	// creates another copy).
	return meta, nil
}

// softDeleteWindow returns the bucket's soft delete retention window. An absent
// policy means the GCS default of 7 days.
func softDeleteWindow(bmeta map[string]any) time.Duration {
	const defaultWindow = 7 * 24 * time.Hour
	pol, _ := bmeta["softDeletePolicy"].(map[string]any)
	if pol == nil {
		return defaultWindow
	}
	secs, _ := pol["retentionDurationSeconds"].(string)
	if secs == "" {
		return defaultWindow
	}
	if d := parseRetentionPeriod(secs); d > 0 {
		return d
	}
	return defaultWindow
}

// softDeleteDisabled reports whether the bucket turned soft delete off
// (softDeletePolicy.retentionDurationSeconds "0"). An absent policy means the
// GCS default (7 days) applies, so soft delete is enabled.
func softDeleteDisabled(bmeta map[string]any) bool {
	pol, _ := bmeta["softDeletePolicy"].(map[string]any)
	if pol == nil {
		return false
	}
	secs, _ := pol["retentionDurationSeconds"].(string)
	return secs == "0"
}

// lockedRetentionBlocksRestore enforces the documented bucket-lock rule: while
// the object's age is below the bucket's retention period, restore fails 403
// retentionPolicyNotMet.
func lockedRetentionBlocksRestore(bmeta map[string]any, target gcs.ObjectMeta, now time.Time) error {
	rp, _ := bmeta["retentionPolicy"].(map[string]any)
	if rp == nil {
		return nil
	}
	if locked, _ := rp["isLocked"].(bool); !locked {
		return nil
	}
	period, _ := rp["retentionPeriod"].(string)
	d := parseRetentionPeriod(period)
	if d <= 0 || target.TimeCreated.IsZero() {
		return nil
	}
	if now.Sub(target.TimeCreated) < d {
		return model.NewProviderError("PermissionDenied",
			"The object's retention period has not expired, so it cannot be restored.", 403).
			WithData(map[string]any{"reason": reasonRetentionPolicyNotMet})
	}
	return nil
}

// MapRestoreStoreError translates the store sentinels the restore core can
// return into the caller-facing GCS errors. The REST handler and the gRPC RPC
// call it after RestoreObjectCore; the model.ProviderError cases (the documented
// 400/403/412 restore failures) pass through untouched.
func MapRestoreStoreError(err error) error {
	switch {
	case errors.Is(err, gcs.ErrNoSuchBucket):
		return model.NewProviderError("NotFound", "bucket not found", 404)
	case errors.Is(err, gcs.ErrNoSuchObject):
		return model.NewProviderError("NotFound", "object generation not found", 404)
	case errors.Is(err, gcs.ErrPreconditionFailed):
		return model.NewProviderError("PreconditionFailed", "At least one of the pre-conditions you specified did not hold", 412)
	}
	return err
}
