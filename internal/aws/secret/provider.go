package secret

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

const sfxAlpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

func randSuffix6() string {
	b := make([]byte, 6)
	rand.Read(b)
	out := make([]byte, 6)
	for i := range b {
		out[i] = sfxAlpha[int(b[i])%len(sfxAlpha)]
	}
	return string(out)
}

// arnName returns the name component used in the secret ARN: "name-suffix".
func (e SecretEntry) arnName() string {
	if e.RandomSuffix != "" {
		return e.Name + "-" + e.RandomSuffix
	}
	return e.Name
}

// SecretProvider handles SecretsManager API operations.
type SecretProvider struct {
	store   SecretStore
	kms     model.KeyEncryptor // nil → NoopKeyEncryptor behaviour
	invoker RotationInvoker    // nil → rotation Lambda not invoked
}

// New constructs a SecretProvider.
// kms may be nil, in which case secret values are stored unencrypted (dev/memory mode).
func New(store SecretStore, kms model.KeyEncryptor) *SecretProvider {
	return &SecretProvider{store: store, kms: kms}
}

// Routes returns all SecretsManager handler registrations.
func (p *SecretProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Secret.CreateSecret":             p.CreateSecret,
		"Secret.DescribeSecret":           p.DescribeSecret,
		"Secret.UpdateSecret":             p.UpdateSecret,
		"Secret.DeleteSecret":             p.DeleteSecret,
		"Secret.RestoreSecret":            p.RestoreSecret,
		"Secret.ListSecrets":              p.ListSecrets,
		"Secret.PutSecretValue":           p.PutSecretValue,
		"Secret.GetSecretValue":           p.GetSecretValue,
		"Secret.ListSecretVersionIds":     p.ListSecretVersionIds,
		"Secret.TagResource":              p.TagResource,
		"Secret.UntagResource":            p.UntagResource,
		"Secret.RotateSecret":             p.RotateSecret,
		"Secret.CancelRotateSecret":       p.CancelRotateSecret,
		"Secret.GetResourcePolicy":        p.GetResourcePolicy,
		"Secret.PutResourcePolicy":        p.PutResourcePolicy,
		"Secret.DeleteResourcePolicy":     p.DeleteResourcePolicy,
		"Secret.UpdateSecretVersionStage": p.UpdateSecretVersionStage,
		"Secret.GetRandomPassword":        p.GetRandomPassword,
		"Secret.BatchGetSecretValue":      p.BatchGetSecretValue,
		"Secret.ValidateResourcePolicy":   p.ValidateResourcePolicy,
	}
}

// ─── Secret lifecycle ─────────────────────────────────────────────────────────

func (p *SecretProvider) CreateSecret(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["Name"].(string)
	if name == "" {
		return nil, model.NewProviderError("ValidationException", "Name is required", 400)
	}
	secretID := newID()
	kmsKeyID, _ := nr.Params["KmsKeyId"].(string)
	desc, _ := nr.Params["Description"].(string)
	tags := extractTags(nr.Params)

	e := SecretEntry{
		AccountID:    nr.AccountID,
		Region:       nr.Region,
		SecretID:     secretID,
		Name:         name,
		Description:  desc,
		KMSKeyID:     kmsKeyID,
		Tags:         tags,
		RandomSuffix: randSuffix6(),
	}
	if err := p.store.CreateSecret(ctx, e); err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			return nil, model.NewProviderError("ResourceExistsException", "secret already exists: "+name, 400)
		}
		return nil, fmt.Errorf("sm: create secret: %w", err)
	}

	secretARN := nr.ResourceID(model.RTSecretsManagerSecret, e.arnName())

	// Store initial value if provided.
	if sv, _ := nr.Params["SecretString"].(string); sv != "" {
		if err := p.putValue(ctx, secretID, kmsKeyID, []byte(sv), name, false); err != nil {
			return nil, err
		}
	} else if svb, _ := nr.Params["SecretBinary"].(string); svb != "" {
		raw, err := base64.StdEncoding.DecodeString(svb)
		if err != nil {
			return nil, model.NewProviderError("ValidationException", "SecretBinary must be base64-encoded", 400)
		}
		if err := p.putValue(ctx, secretID, kmsKeyID, raw, name, true); err != nil {
			return nil, err
		}
	}

	return provider.OK(map[string]any{
		"ARN":  secretARN,
		"Name": name,
	}), nil
}

func (p *SecretProvider) DescribeSecret(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.resolveSecret(ctx, nr)
	if err != nil {
		return nil, err
	}
	versions, _ := p.store.ListVersions(ctx, e.SecretID)
	return provider.OK(p.secretDetail(e, versions, nr)), nil
}

func (p *SecretProvider) UpdateSecret(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.resolveSecret(ctx, nr)
	if err != nil {
		return nil, err
	}
	if e.DeletedAt != nil {
		return nil, model.NewProviderError("InvalidRequestException", "secret is scheduled for deletion", 400)
	}
	if d, ok := nr.Params["Description"].(string); ok {
		e.Description = d
	}
	if k, ok := nr.Params["KmsKeyId"].(string); ok {
		e.KMSKeyID = k
	}

	// 3.7.1 — store new secret value when SecretString or SecretBinary is present.
	var newVersionID string
	if sv, _ := nr.Params["SecretString"].(string); sv != "" {
		newVersionID = newID()
		if err := p.putValueWithStages(ctx, e.SecretID, e.KMSKeyID, []byte(sv), e.Name, newVersionID, false, []string{"AWSCURRENT"}); err != nil {
			return nil, err
		}
	} else if svb, _ := nr.Params["SecretBinary"].(string); svb != "" {
		raw, decErr := base64.StdEncoding.DecodeString(svb)
		if decErr != nil {
			return nil, model.NewProviderError("ValidationException", "SecretBinary must be base64-encoded", 400)
		}
		newVersionID = newID()
		if err := p.putValueWithStages(ctx, e.SecretID, e.KMSKeyID, raw, e.Name, newVersionID, true, []string{"AWSCURRENT"}); err != nil {
			return nil, err
		}
	}

	if err := p.store.UpdateSecret(ctx, e); err != nil {
		return nil, fmt.Errorf("sm: update secret: %w", err)
	}
	secretARN := nr.ResourceID(model.RTSecretsManagerSecret, e.arnName())
	resp := map[string]any{
		"ARN":  secretARN,
		"Name": e.Name,
	}
	if newVersionID != "" {
		resp["VersionId"] = newVersionID
	}
	return provider.OK(resp), nil
}

func (p *SecretProvider) DeleteSecret(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.resolveSecret(ctx, nr)
	if err != nil {
		return nil, err
	}

	if e.DeletedAt != nil {
		return nil, model.NewProviderError("InvalidRequestException",
			"You tried to perform the operation on a secret that's currently marked deleted.", 400)
	}

	forceDelete, _ := nr.Params["ForceDeleteWithoutRecovery"].(bool)
	_, hasRecoveryDays := nr.Params["RecoveryWindowInDays"]
	if forceDelete && hasRecoveryDays {
		return nil, model.NewProviderError("InvalidParameterException",
			"You can't use ForceDeleteWithoutRecovery in conjunction with RecoveryWindowInDays.", 400)
	}

	recoveryDays := int64(30)
	if v, ok := nr.Params["RecoveryWindowInDays"].(float64); ok {
		recoveryDays = int64(v)
		if recoveryDays < 7 || recoveryDays > 30 {
			return nil, model.NewProviderError("InvalidParameterException",
				"RecoveryWindowInDays value must be between 7 and 30 days (inclusive).", 400)
		}
	}

	secretARN := nr.ResourceID(model.RTSecretsManagerSecret, e.arnName())

	if forceDelete {
		if err := p.store.DeleteSecret(ctx, e.SecretID); err != nil {
			return nil, fmt.Errorf("sm: force delete: %w", err)
		}
		return provider.OK(map[string]any{
			"ARN":          secretARN,
			"Name":         e.Name,
			"DeletionDate": nr.Clock.Now().Unix(),
		}), nil
	}

	deletionDate := nr.Clock.Now().Add(time.Duration(recoveryDays) * 24 * time.Hour)
	e.DeletedAt = &deletionDate
	if err := p.store.UpdateSecret(ctx, e); err != nil {
		return nil, fmt.Errorf("sm: soft delete: %w", err)
	}
	return provider.OK(map[string]any{
		"ARN":          secretARN,
		"Name":         e.Name,
		"DeletionDate": deletionDate.Unix(),
	}), nil
}

func (p *SecretProvider) RestoreSecret(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.resolveSecret(ctx, nr)
	if err != nil {
		return nil, err
	}
	if e.DeletedAt == nil {
		return nil, model.NewProviderError("InvalidRequestException", "secret is not scheduled for deletion", 400)
	}
	e.DeletedAt = nil
	if err := p.store.UpdateSecret(ctx, e); err != nil {
		return nil, fmt.Errorf("sm: restore secret: %w", err)
	}
	secretARN := nr.ResourceID(model.RTSecretsManagerSecret, e.arnName())
	return provider.OK(map[string]any{
		"ARN":  secretARN,
		"Name": e.Name,
	}), nil
}

func (p *SecretProvider) ListSecrets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	secrets, err := p.store.ListSecrets(ctx, nr.AccountID)
	if err != nil {
		return nil, fmt.Errorf("sm: list secrets: %w", err)
	}
	items := make([]map[string]any, 0, len(secrets))
	for _, e := range secrets {
		items = append(items, p.secretSummary(e, nr))
	}
	return provider.OK(map[string]any{"SecretList": items}), nil
}

// ─── Secret values ────────────────────────────────────────────────────────────

func (p *SecretProvider) PutSecretValue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.resolveSecret(ctx, nr)
	if err != nil {
		return nil, err
	}
	if e.DeletedAt != nil {
		return nil, model.NewProviderError("InvalidRequestException", "secret is scheduled for deletion", 400)
	}

	var raw []byte
	var isBinary bool
	if sv, _ := nr.Params["SecretString"].(string); sv != "" {
		raw = []byte(sv)
	} else if svb, _ := nr.Params["SecretBinary"].(string); svb != "" {
		raw, err = base64.StdEncoding.DecodeString(svb)
		if err != nil {
			return nil, model.NewProviderError("ValidationException", "SecretBinary must be base64-encoded", 400)
		}
		isBinary = true
	} else {
		return nil, model.NewProviderError("ValidationException", "SecretString or SecretBinary is required", 400)
	}

	versionID, _ := nr.Params["ClientRequestToken"].(string)
	if versionID == "" {
		versionID = newID()
	}

	stages := []string{"AWSCURRENT"}
	if vs, ok := nr.Params["VersionStages"].([]any); ok && len(vs) > 0 {
		stages = make([]string, 0, len(vs))
		for _, s := range vs {
			if str, ok := s.(string); ok {
				stages = append(stages, str)
			}
		}
	}

	if err := p.putValueWithStages(ctx, e.SecretID, e.KMSKeyID, raw, e.Name, versionID, isBinary, stages); err != nil {
		return nil, err
	}

	secretARN := nr.ResourceID(model.RTSecretsManagerSecret, e.arnName())
	return provider.OK(map[string]any{
		"ARN":           secretARN,
		"Name":          e.Name,
		"VersionId":     versionID,
		"VersionStages": stages,
	}), nil
}

func (p *SecretProvider) GetSecretValue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.resolveSecret(ctx, nr)
	if err != nil {
		return nil, err
	}
	if e.DeletedAt != nil {
		return nil, model.NewProviderError("InvalidRequestException", "secret is scheduled for deletion", 400)
	}

	var v VersionEntry
	if vid, _ := nr.Params["VersionId"].(string); vid != "" {
		v, err = p.store.GetVersion(ctx, e.SecretID, vid)
	} else if stage, _ := nr.Params["VersionStage"].(string); stage != "" {
		v, err = p.store.GetVersionByStage(ctx, e.SecretID, stage)
	} else {
		v, err = p.store.GetVersionByStage(ctx, e.SecretID, "AWSCURRENT")
	}
	if err != nil {
		if errors.Is(err, ErrVersionNotFound) {
			return nil, model.NewProviderError("ResourceNotFoundException", "no secret value found", 400)
		}
		return nil, fmt.Errorf("sm: get version: %w", err)
	}

	// Decrypt the value.
	pt, err := p.decrypt(ctx, e.KMSKeyID, v.SecretBinary, e.Name)
	if err != nil {
		return nil, fmt.Errorf("sm: decrypt value: %w", err)
	}

	// 3.7.4 — update LastAccessedDate to today midnight UTC (date precision, skip if already today).
	todayMidnight := nr.Clock.Now().UTC().Truncate(24 * time.Hour)
	if e.LastAccessedDate == nil || !e.LastAccessedDate.Equal(todayMidnight) {
		e.LastAccessedDate = &todayMidnight
		_ = p.store.UpdateSecret(ctx, e)
	}

	secretARN := nr.ResourceID(model.RTSecretsManagerSecret, e.arnName())
	resp := map[string]any{
		"ARN":           secretARN,
		"Name":          e.Name,
		"VersionId":     v.VersionID,
		"VersionStages": v.Stages,
		"CreatedDate":   v.CreatedAt.Unix(),
	}
	if v.IsBinary {
		resp["SecretBinary"] = pt
	} else {
		resp["SecretString"] = string(pt)
	}
	return provider.OK(resp), nil
}

func (p *SecretProvider) ListSecretVersionIds(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.resolveSecret(ctx, nr)
	if err != nil {
		return nil, err
	}
	versions, err := p.store.ListVersions(ctx, e.SecretID)
	if err != nil {
		return nil, fmt.Errorf("sm: list versions: %w", err)
	}
	items := make([]map[string]any, 0, len(versions))
	for _, v := range versions {
		items = append(items, map[string]any{
			"VersionId":     v.VersionID,
			"VersionStages": v.Stages,
			"CreatedDate":   v.CreatedAt.Unix(),
		})
	}
	secretARN := nr.ResourceID(model.RTSecretsManagerSecret, e.arnName())
	return provider.OK(map[string]any{
		"ARN":      secretARN,
		"Name":     e.Name,
		"Versions": items,
	}), nil
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func (p *SecretProvider) TagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.resolveSecret(ctx, nr)
	if err != nil {
		return nil, err
	}
	newTags := extractTags(nr.Params)
	if e.Tags == nil {
		e.Tags = make(map[string]string)
	}
	for k, v := range newTags {
		e.Tags[k] = v
	}
	if err := p.store.UpdateSecret(ctx, e); err != nil {
		return nil, fmt.Errorf("sm: tag resource: %w", err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *SecretProvider) UntagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.resolveSecret(ctx, nr)
	if err != nil {
		return nil, err
	}
	if tagKeys, ok := nr.Params["TagKeys"].([]any); ok {
		for _, k := range tagKeys {
			delete(e.Tags, fmt.Sprint(k))
		}
	}
	if err := p.store.UpdateSecret(ctx, e); err != nil {
		return nil, fmt.Errorf("sm: untag resource: %w", err)
	}
	return provider.OK(map[string]any{}), nil
}

// ─── Rotation ─────────────────────────────────────────────────────────────────

func (p *SecretProvider) RotateSecret(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.resolveSecret(ctx, nr)
	if err != nil {
		return nil, err
	}

	// Persist RotationLambdaARN and RotationRules if provided.
	if lambdaARN, ok := nr.Params["RotationLambdaARN"].(string); ok && lambdaARN != "" {
		e.RotationLambdaARN = lambdaARN
	}
	if rules, ok := nr.Params["RotationRules"].(map[string]any); ok {
		if d, ok := rules["AutomaticallyAfterDays"].(float64); ok {
			days := int(d)
			if days < 1 || days > 1000 {
				return nil, model.NewProviderError("InvalidParameterException",
					"AutomaticallyAfterDays must be between 1 and 1000", 400)
			}
			e.AutoRotateAfterDays = days
		}
	}

	// Check for an in-progress rotation (AWSPENDING not also AWSCURRENT).
	versions, _ := p.store.ListVersions(ctx, e.SecretID)
	for _, v := range versions {
		hasPending := containsStageSlice(v.Stages, "AWSPENDING")
		hasCurrent := containsStageSlice(v.Stages, "AWSCURRENT")
		if hasPending && !hasCurrent {
			return nil, model.NewProviderError("InvalidRequestException",
				"A previous rotation request is still in progress", 400)
		}
	}

	// Get current secret value to copy into the new pending version.
	currentV, err := p.store.GetVersionByStage(ctx, e.SecretID, "AWSCURRENT")
	if err != nil {
		return nil, model.NewProviderError("InvalidRequestException", "no current version to rotate", 400)
	}

	pendingVersionID := newID()
	if err := p.store.PutVersion(ctx, VersionEntry{
		SecretID:     e.SecretID,
		VersionID:    pendingVersionID,
		SecretBinary: currentV.SecretBinary,
		IsBinary:     currentV.IsBinary,
		Stages:       []string{"AWSPENDING"},
	}); err != nil {
		return nil, fmt.Errorf("sm: put pending version: %w", err)
	}

	// rotateImmediately defaults to true.
	rotateNow := true
	if v, ok := nr.Params["RotateImmediately"].(bool); ok {
		rotateNow = v
	}
	if rotateNow {
		// Promote AWSPENDING → AWSCURRENT (PutVersion demotes old AWSCURRENT to AWSPREVIOUS).
		if err := p.store.PutVersion(ctx, VersionEntry{
			SecretID:     e.SecretID,
			VersionID:    pendingVersionID,
			SecretBinary: currentV.SecretBinary,
			IsBinary:     currentV.IsBinary,
			Stages:       []string{"AWSCURRENT"},
		}); err != nil {
			return nil, fmt.Errorf("sm: promote pending version: %w", err)
		}
		now := clock.Now()
		e.LastRotatedDate = &now
	}

	if err := p.store.UpdateSecret(ctx, e); err != nil {
		return nil, fmt.Errorf("sm: update secret rotation fields: %w", err)
	}

	secretARN := nr.ResourceID(model.RTSecretsManagerSecret, e.arnName())

	// Invoke rotation Lambda asynchronously when wired and rotating now.
	if rotateNow && e.RotationLambdaARN != "" && p.invoker != nil {
		go p.runRotationLambda(context.WithoutCancel(ctx), e.SecretID, secretARN, pendingVersionID, e.RotationLambdaARN)
	}

	return provider.OK(map[string]any{
		"ARN":       secretARN,
		"Name":      e.Name,
		"VersionId": pendingVersionID,
	}), nil
}

func (p *SecretProvider) CancelRotateSecret(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.resolveSecret(ctx, nr)
	if err != nil {
		return nil, err
	}

	// Remove AWSPENDING stage from any version that has it but not AWSCURRENT.
	versions, _ := p.store.ListVersions(ctx, e.SecretID)
	for _, v := range versions {
		if containsStageSlice(v.Stages, "AWSPENDING") && !containsStageSlice(v.Stages, "AWSCURRENT") {
			newStages := removeStageSlice(v.Stages, "AWSPENDING")
			_ = p.store.UpdateVersionStages(ctx, e.SecretID, v.VersionID, newStages)
		}
	}

	e.RotationLambdaARN = ""
	if err := p.store.UpdateSecret(ctx, e); err != nil {
		return nil, fmt.Errorf("sm: cancel rotation: %w", err)
	}

	secretARN := nr.ResourceID(model.RTSecretsManagerSecret, e.arnName())
	return provider.OK(map[string]any{
		"ARN":  secretARN,
		"Name": e.Name,
	}), nil
}

// ─── Resource policy ──────────────────────────────────────────────────────────

func (p *SecretProvider) GetResourcePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.resolveSecret(ctx, nr)
	if err != nil {
		return nil, err
	}
	if e.ResourcePolicy == "" {
		return nil, model.NewProviderError("ResourceNotFoundException", "resource policy not found for secret: "+e.Name, 400)
	}
	return provider.OK(map[string]any{
		"ARN":            nr.ResourceID(model.RTSecretsManagerSecret, e.arnName()),
		"Name":           e.Name,
		"ResourcePolicy": e.ResourcePolicy,
	}), nil
}

func (p *SecretProvider) PutResourcePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.resolveSecret(ctx, nr)
	if err != nil {
		return nil, err
	}
	policy, _ := nr.Params["ResourcePolicy"].(string)
	e.ResourcePolicy = policy
	if err := p.store.UpdateSecret(ctx, e); err != nil {
		return nil, fmt.Errorf("secretsmanager: put resource policy: %w", err)
	}
	return provider.OK(map[string]any{
		"ARN":  nr.ResourceID(model.RTSecretsManagerSecret, e.arnName()),
		"Name": e.Name,
	}), nil
}

func (p *SecretProvider) DeleteResourcePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.resolveSecret(ctx, nr)
	if err != nil {
		return nil, err
	}
	e.ResourcePolicy = ""
	if err := p.store.UpdateSecret(ctx, e); err != nil {
		return nil, fmt.Errorf("secretsmanager: delete resource policy: %w", err)
	}
	return provider.OK(map[string]any{
		"ARN":  nr.ResourceID(model.RTSecretsManagerSecret, e.arnName()),
		"Name": e.Name,
	}), nil
}

// ─── internal helpers ─────────────────────────────────────────────────────────

func (p *SecretProvider) resolveSecret(ctx context.Context, nr *model.NormalizedRequest) (SecretEntry, error) {
	id, _ := nr.Params["SecretId"].(string)
	if id == "" {
		return SecretEntry{}, model.NewProviderError("ValidationException", "SecretId is required", 400)
	}
	// Try by ID first, then by name (AWS accepts both).
	e, err := p.store.GetSecret(ctx, id)
	if errors.Is(err, ErrSecretNotFound) {
		e, err = p.store.GetSecretByName(ctx, nr.AccountID, id)
	}
	if errors.Is(err, ErrSecretNotFound) {
		return SecretEntry{}, model.NewProviderError("ResourceNotFoundException", "secret not found: "+id, 400)
	}
	if err != nil {
		return SecretEntry{}, fmt.Errorf("sm: resolve secret: %w", err)
	}
	return e, nil
}

func (p *SecretProvider) putValue(ctx context.Context, secretID, kmsKeyID string, raw []byte, name string, isBinary bool) error {
	return p.putValueWithID(ctx, secretID, kmsKeyID, raw, name, newID(), isBinary)
}

func (p *SecretProvider) putValueWithID(ctx context.Context, secretID, kmsKeyID string, raw []byte, name, versionID string, isBinary bool) error {
	return p.putValueWithStages(ctx, secretID, kmsKeyID, raw, name, versionID, isBinary, []string{"AWSCURRENT"})
}

func (p *SecretProvider) putValueWithStages(ctx context.Context, secretID, kmsKeyID string, raw []byte, name, versionID string, isBinary bool, stages []string) error {
	ct, err := p.encrypt(ctx, kmsKeyID, raw, name)
	if err != nil {
		return fmt.Errorf("sm: encrypt: %w", err)
	}
	return p.store.PutVersion(ctx, VersionEntry{
		SecretID:     secretID,
		VersionID:    versionID,
		SecretBinary: ct,
		IsBinary:     isBinary,
		Stages:       stages,
	})
}

func (p *SecretProvider) UpdateSecretVersionStage(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.resolveSecret(ctx, nr)
	if err != nil {
		return nil, err
	}
	versionStage, _ := nr.Params["VersionStage"].(string)
	if versionStage == "" {
		return nil, model.NewProviderError("ValidationException", "VersionStage is required", 400)
	}
	removeFromID, _ := nr.Params["RemoveFromVersionId"].(string)
	moveToID, _ := nr.Params["MoveToVersionId"].(string)

	if removeFromID != "" {
		removeFrom, err := p.store.GetVersion(ctx, e.SecretID, removeFromID)
		if err != nil {
			return nil, model.NewProviderError("ResourceNotFoundException", "version not found: "+removeFromID, 400)
		}
		newStages := removeStageSlice(removeFrom.Stages, versionStage)
		if versionStage == "AWSCURRENT" {
			if !containsStageSlice(newStages, "AWSPREVIOUS") {
				newStages = append(newStages, "AWSPREVIOUS")
			}
		}
		if err := p.store.UpdateVersionStages(ctx, e.SecretID, removeFromID, newStages); err != nil {
			return nil, fmt.Errorf("sm: remove stage: %w", err)
		}
	}

	if moveToID != "" {
		moveTo, err := p.store.GetVersion(ctx, e.SecretID, moveToID)
		if err != nil {
			return nil, model.NewProviderError("ResourceNotFoundException", "version not found: "+moveToID, 400)
		}
		if !containsStageSlice(moveTo.Stages, versionStage) {
			if versionStage == "AWSCURRENT" {
				// Remove AWSPREVIOUS from all other versions.
				all, _ := p.store.ListVersions(ctx, e.SecretID)
				for _, v := range all {
					if v.VersionID != moveToID && v.VersionID != removeFromID && containsStageSlice(v.Stages, "AWSPREVIOUS") {
						p.store.UpdateVersionStages(ctx, e.SecretID, v.VersionID, removeStageSlice(v.Stages, "AWSPREVIOUS"))
					}
				}
			}
			if err := p.store.UpdateVersionStages(ctx, e.SecretID, moveToID, append(moveTo.Stages, versionStage)); err != nil {
				return nil, fmt.Errorf("sm: add stage: %w", err)
			}
		}
	}

	secretARN := nr.ResourceID(model.RTSecretsManagerSecret, e.arnName())
	return provider.OK(map[string]any{"ARN": secretARN, "Name": e.Name}), nil
}

func removeStageSlice(stages []string, stage string) []string {
	out := make([]string, 0, len(stages))
	for _, s := range stages {
		if s != stage {
			out = append(out, s)
		}
	}
	return out
}

func containsStageSlice(stages []string, stage string) bool {
	for _, s := range stages {
		if s == stage {
			return true
		}
	}
	return false
}

func (p *SecretProvider) encrypt(ctx context.Context, kmsKeyID string, pt []byte, name string) ([]byte, error) {
	if p.kms == nil || kmsKeyID == "" {
		return pt, nil
	}
	encCtx := map[string]string{"SecretARN": name}
	return p.kms.Encrypt(ctx, kmsKeyID, pt, encCtx)
}

func (p *SecretProvider) decrypt(ctx context.Context, kmsKeyID string, ct []byte, name string) ([]byte, error) {
	if p.kms == nil || kmsKeyID == "" {
		return ct, nil
	}
	encCtx := map[string]string{"SecretARN": name}
	return p.kms.Decrypt(ctx, kmsKeyID, ct, encCtx)
}

func (p *SecretProvider) secretDetail(e SecretEntry, versions []VersionEntry, nr *model.NormalizedRequest) map[string]any {
	versionIDs := make(map[string][]string, len(versions))
	for _, v := range versions {
		versionIDs[v.VersionID] = v.Stages
	}
	tags := make([]map[string]string, 0, len(e.Tags))
	for k, v := range e.Tags {
		tags = append(tags, map[string]string{"Key": k, "Value": v})
	}
	// 3.7.3 — full DescribeSecret shape.
	d := map[string]any{
		"ARN":                nr.ResourceID(model.RTSecretsManagerSecret, e.arnName()),
		"Name":               e.Name,
		"Description":        e.Description,
		"KmsKeyId":           e.KMSKeyID,
		"Tags":               tags,
		"VersionIdsToStages": versionIDs,
		"CreatedDate":        e.CreatedAt.Unix(),
		"LastChangedDate":    e.UpdatedAt.Unix(),
		"RotationEnabled":    e.RotationEnabled,
		"RotationLambdaARN":  e.RotationLambdaARN,
		"ReplicationStatus":  []any{},
		"OwningService":      "",
	}
	if e.LastRotatedDate != nil {
		d["LastRotatedDate"] = e.LastRotatedDate.UnixMilli()
	}
	if e.LastAccessedDate != nil {
		d["LastAccessedDate"] = e.LastAccessedDate.UnixMilli()
	}
	if e.AutoRotateAfterDays > 0 {
		d["RotationRules"] = map[string]any{"AutomaticallyAfterDays": e.AutoRotateAfterDays}
	}
	if e.DeletedAt != nil {
		d["DeletedDate"] = e.DeletedAt.Unix()
	}
	return d
}

// secretSummary returns the subset of fields used by ListSecrets (same shape, no VersionIdsToStages).
func (p *SecretProvider) secretSummary(e SecretEntry, nr *model.NormalizedRequest) map[string]any {
	tags := make([]map[string]string, 0, len(e.Tags))
	for k, v := range e.Tags {
		tags = append(tags, map[string]string{"Key": k, "Value": v})
	}
	d := map[string]any{
		"ARN":               nr.ResourceID(model.RTSecretsManagerSecret, e.arnName()),
		"Name":              e.Name,
		"Description":       e.Description,
		"KmsKeyId":          e.KMSKeyID,
		"Tags":              tags,
		"CreatedDate":       e.CreatedAt.Unix(),
		"LastChangedDate":   e.UpdatedAt.Unix(),
		"RotationEnabled":   e.RotationEnabled,
		"RotationLambdaARN": e.RotationLambdaARN,
		"ReplicationStatus": []any{},
		"OwningService":     "",
	}
	if e.LastRotatedDate != nil {
		d["LastRotatedDate"] = e.LastRotatedDate.UnixMilli()
	}
	if e.LastAccessedDate != nil {
		d["LastAccessedDate"] = e.LastAccessedDate.UnixMilli()
	}
	if e.AutoRotateAfterDays > 0 {
		d["RotationRules"] = map[string]any{"AutomaticallyAfterDays": e.AutoRotateAfterDays}
	}
	if e.DeletedAt != nil {
		d["DeletedDate"] = e.DeletedAt.Unix()
	}
	return d
}

func newID() string {
	b := make([]byte, 16)
	io.ReadFull(rand.Reader, b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func extractTags(params map[string]any) map[string]string {
	raw, ok := params["Tags"]
	if !ok {
		return nil
	}
	out := make(map[string]string)
	switch v := raw.(type) {
	case []any:
		for _, item := range v {
			if m, ok := item.(map[string]any); ok {
				k, _ := m["Key"].(string)
				val, _ := m["Value"].(string)
				if k != "" {
					out[k] = val
				}
			}
		}
	case map[string]any:
		for k, val := range v {
			out[k] = fmt.Sprint(val)
		}
	}
	return out
}

// ─── P4.8: GetRandomPassword ──────────────────────────────────────────────────

func (p *SecretProvider) GetRandomPassword(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	passwordLength := 32
	if v, ok := nr.Params["PasswordLength"].(float64); ok {
		passwordLength = int(v)
	}
	if passwordLength < 1 || passwordLength > 4096 {
		return nil, model.NewProviderError("ValidationException",
			"PasswordLength must be between 1 and 4096", 400)
	}

	excludeChars, _ := nr.Params["ExcludeCharacters"].(string)
	excludeNumbers, _ := nr.Params["ExcludeNumbers"].(bool)
	excludePunctuation, _ := nr.Params["ExcludePunctuation"].(bool)
	excludeLowercase, _ := nr.Params["ExcludeLowercase"].(bool)
	excludeUppercase, _ := nr.Params["ExcludeUppercase"].(bool)
	includeSpace, _ := nr.Params["IncludeSpace"].(bool)
	requireEach, _ := nr.Params["RequireEachIncludedType"].(bool)

	const (
		lowercase   = "abcdefghijklmnopqrstuvwxyz"
		uppercase   = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		digits      = "0123456789"
		punctuation = `!"#$%&'()*+,-./:;<=>?@[\]^_{|}~` + "`"
	)

	var pool string
	var categories []string
	if !excludeLowercase {
		pool += lowercase
		categories = append(categories, lowercase)
	}
	if !excludeUppercase {
		pool += uppercase
		categories = append(categories, uppercase)
	}
	if !excludeNumbers {
		pool += digits
		categories = append(categories, digits)
	}
	if !excludePunctuation {
		pool += punctuation
		categories = append(categories, punctuation)
	}
	if includeSpace {
		pool += " "
		categories = append(categories, " ")
	}

	if excludeChars != "" {
		var filtered string
		for _, c := range pool {
			if !strings.ContainsRune(excludeChars, c) {
				filtered += string(c)
			}
		}
		pool = filtered
	}

	if pool == "" {
		return nil, model.NewProviderError("ValidationException",
			"No characters available to generate password", 400)
	}

	password := make([]byte, passwordLength)
	for i := range password {
		b := make([]byte, 1)
		rand.Read(b)
		password[i] = pool[int(b[0])%len(pool)]
	}

	if requireEach && len(categories) > 0 && passwordLength >= len(categories) {
		for i, cat := range categories {
			if i >= passwordLength {
				break
			}
			var filteredCat string
			for _, c := range cat {
				if excludeChars == "" || !strings.ContainsRune(excludeChars, c) {
					filteredCat += string(c)
				}
			}
			if filteredCat != "" {
				b := make([]byte, 1)
				rand.Read(b)
				password[i] = filteredCat[int(b[0])%len(filteredCat)]
			}
		}
		for i := len(password) - 1; i > 0; i-- {
			b := make([]byte, 1)
			rand.Read(b)
			j := int(b[0]) % (i + 1)
			password[i], password[j] = password[j], password[i]
		}
	}

	return provider.OK(map[string]any{
		"RandomPassword": string(password),
	}), nil
}

// ─── P4.9: BatchGetSecretValue ────────────────────────────────────────────────

func (p *SecretProvider) BatchGetSecretValue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	secretIDList := extractStringList(nr.Params, "SecretIdList")
	if len(secretIDList) > 20 {
		return nil, model.NewProviderError("ValidationException",
			"1 validation error detected: Value at 'secretIdList' failed to satisfy constraint: Member must have length less than or equal to 20", 400)
	}

	var secretValues []map[string]any
	var errs []map[string]any

	for _, secretID := range secretIDList {
		fakeNR := &model.NormalizedRequest{
			Params:     map[string]any{"SecretId": secretID},
			ResourceID: nr.ResourceID,
			Region:     nr.Region,
			AccountID:  nr.AccountID,
			Clock:      nr.Clock,
		}
		resp, err := p.GetSecretValue(ctx, fakeNR)
		if err != nil {
			var pe *model.ProviderError
			if errors.As(err, &pe) {
				errs = append(errs, map[string]any{
					"SecretId":  secretID,
					"ErrorCode": pe.Code,
					"Message":   pe.Message,
				})
			} else {
				errs = append(errs, map[string]any{
					"SecretId":  secretID,
					"ErrorCode": "InternalServiceError",
					"Message":   err.Error(),
				})
			}
			continue
		}
		secretValues = append(secretValues, resp.Data)
	}

	if secretValues == nil {
		secretValues = []map[string]any{}
	}
	if errs == nil {
		errs = []map[string]any{}
	}
	return provider.OK(map[string]any{
		"SecretValues": secretValues,
		"Errors":       errs,
	}), nil
}

func (p *SecretProvider) ValidateResourcePolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	policy, _ := nr.Params["ResourcePolicy"].(string)
	if policy == "" {
		return nil, model.NewProviderError("ValidationException", "ResourcePolicy is required", 400)
	}

	var errs []map[string]string

	var doc map[string]any
	if err := json.Unmarshal([]byte(policy), &doc); err != nil {
		errs = append(errs, map[string]string{"CheckName": "JSONSyntax", "ErrorMessage": "Invalid JSON: " + err.Error()})
		return provider.OK(map[string]any{
			"PolicyValidationPassed": false,
			"ValidationErrors":       errs,
		}), nil
	}

	// Validate Version
	if v, ok := doc["Version"].(string); ok {
		if v != "2012-10-17" && v != "2008-10-17" {
			errs = append(errs, map[string]string{
				"CheckName":    "PolicyVersion",
				"ErrorMessage": "Unrecognized policy version: " + v,
			})
		}
	}

	// Validate Statement
	stmts, ok := doc["Statement"]
	if !ok {
		errs = append(errs, map[string]string{"CheckName": "StatementPresent", "ErrorMessage": "Policy must contain Statement"})
	} else {
		stmtList, ok := stmts.([]any)
		if !ok {
			errs = append(errs, map[string]string{"CheckName": "StatementType", "ErrorMessage": "Statement must be an array"})
		} else {
			for i, s := range stmtList {
				sm, ok := s.(map[string]any)
				if !ok {
					errs = append(errs, map[string]string{"CheckName": "StatementFormat", "ErrorMessage": fmt.Sprintf("Statement[%d] must be an object", i)})
					continue
				}
				if _, hasEffect := sm["Effect"]; !hasEffect {
					errs = append(errs, map[string]string{"CheckName": "StatementEffect", "ErrorMessage": fmt.Sprintf("Statement[%d] must have Effect field", i)})
				}
			}
		}
	}

	return provider.OK(map[string]any{
		"PolicyValidationPassed": len(errs) == 0,
		"ValidationErrors":       errs,
	}), nil
}

func extractStringList(params map[string]any, key string) []string {
	raw, ok := params[key]
	if !ok {
		return nil
	}
	arr, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, v := range arr {
		out = append(out, fmt.Sprint(v))
	}
	return out
}
