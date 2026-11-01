package key

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	awsarn "jaiscloud/internal/aws/arn"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/resourcemgr"
)

const (
	ciphertextKeyIDLen = 36 // UUID length
)

// KeyProvider handles KMS API operations.
type KeyProvider struct {
	store KeyStore
	rm    *resourcemgr.Manager
	// serverDEK is the server data-encryption key used to protect per-KMS-key material.
	serverDEK []byte
}

// New constructs a KeyProvider.
// serverDEK is the 32-byte DEK from the bootstrap process.
func New(store KeyStore, rm *resourcemgr.Manager, serverDEK []byte) *KeyProvider {
	return &KeyProvider{store: store, rm: rm, serverDEK: serverDEK}
}

// Routes returns all KMS handler registrations.
func (p *KeyProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Key.CreateKey":                           p.CreateKey,
		"Key.DescribeKey":                         p.DescribeKey,
		"Key.EnableKey":                           p.EnableKey,
		"Key.DisableKey":                          p.DisableKey,
		"Key.ScheduleKeyDeletion":                 p.ScheduleKeyDeletion,
		"Key.CancelKeyDeletion":                   p.CancelKeyDeletion,
		"Key.ListKeys":                            p.ListKeys,
		"Key.TagResource":                         p.TagResource,
		"Key.UntagResource":                       p.UntagResource,
		"Key.ListResourceTags":                    p.ListResourceTags,
		"Key.CreateAlias":                         p.CreateAlias,
		"Key.DeleteAlias":                         p.DeleteAlias,
		"Key.ListAliases":                         p.ListAliases,
		"Key.UpdateAlias":                         p.UpdateAlias,
		"Key.CreateGrant":                         p.CreateGrant,
		"Key.RevokeGrant":                         p.RevokeGrant,
		"Key.RetireGrant":                         p.RetireGrant,
		"Key.ListGrants":                          p.ListGrants,
		"Key.ListRetirableGrants":                 p.ListRetirableGrants,
		"Key.ListKeyPolicies":                     p.ListKeyPolicies,
		"Key.DeriveSharedSecret":                  p.DeriveSharedSecret,
		"Key.Encrypt":                             p.Encrypt,
		"Key.Decrypt":                             p.Decrypt,
		"Key.GenerateDataKey":                     p.GenerateDataKey,
		"Key.GenerateDataKeyWithoutPlaintext":     p.GenerateDataKeyWithoutPlaintext,
		"Key.ReEncrypt":                           p.ReEncrypt,
		"Key.GetKeyPolicy":                        p.GetKeyPolicy,
		"Key.PutKeyPolicy":                        p.PutKeyPolicy,
		"Key.GetKeyRotationStatus":                p.GetKeyRotationStatus,
		"Key.EnableKeyRotation":                   p.EnableKeyRotation,
		"Key.DisableKeyRotation":                  p.DisableKeyRotation,
		"Key.Sign":                                p.Sign,
		"Key.Verify":                              p.Verify,
		"Key.GetPublicKey":                        p.GetPublicKey,
		"Key.RotateKeyOnDemand":                   p.RotateKeyOnDemand,
		"Key.GenerateDataKeyPair":                 p.GenerateDataKeyPair,
		"Key.GenerateDataKeyPairWithoutPlaintext": p.GenerateDataKeyPairWithoutPlaintext,
		"Key.GenerateMac":                         p.GenerateMac,
		"Key.VerifyMac":                           p.VerifyMac,
		"Key.GenerateRandom":                      p.GenerateRandom,
		// Key import (stub)
		"Key.GetParametersForImport":    p.GetParametersForImport,
		"Key.ImportKeyMaterial":         p.ImportKeyMaterial,
		"Key.DeleteImportedKeyMaterial": p.DeleteImportedKeyMaterial,
		// Description update
		"Key.UpdateKeyDescription": p.UpdateKeyDescription,
	}
}

// ─── Key lifecycle ────────────────────────────────────────────────────────────

func (p *KeyProvider) CreateKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID := newID()
	keyUsage, _ := nr.Params["KeyUsage"].(string)
	if keyUsage == "" {
		keyUsage = "ENCRYPT_DECRYPT"
	}
	keySpec, _ := nr.Params["KeySpec"].(string)
	if keySpec == "" {
		keySpec = "SYMMETRIC_DEFAULT"
	}
	desc, _ := nr.Params["Description"].(string)
	tags := extractTags(nr.Params)

	origin, _ := nr.Params["Origin"].(string)
	if origin == "" {
		origin = "AWS_KMS"
	}

	e := KeyEntry{
		AccountID:   nr.AccountID,
		Region:      nr.Region,
		KeyID:       keyID,
		Enabled:     true,
		Description: desc,
		KeyUsage:    keyUsage,
		KeySpec:     keySpec,
		Origin:      origin,
		Tags:        tags,
		CreatedAt:   clock.Now(),
	}

	// EXTERNAL origin: create key entry without any key material.
	if origin == "EXTERNAL" {
		if err := p.store.CreateKey(ctx, e); err != nil {
			return nil, fmt.Errorf("kms: create key: %w", err)
		}
		keyARN := nr.ResourceID(model.RTKMSKey, keyID)
		return provider.OK(map[string]any{
			"KeyMetadata": keyMetadata(e, keyARN, nr.Region, nr.AccountID),
		}), nil
	}

	if isAsymmetricSpec(keySpec) {
		privDER, pubDER, err := generateAsymmetricKey(keySpec)
		if err != nil {
			return nil, model.NewProviderError("ValidationException", err.Error(), 400)
		}
		encPriv, err := encryptData(p.serverDEK, privDER, []byte(keyID))
		if err != nil {
			return nil, fmt.Errorf("kms: encrypt private key: %w", err)
		}
		e.PrivateKey = encPriv
		e.PublicKey = pubDER
	} else {
		// Symmetric or HMAC key: generate random material at the correct size for the spec.
		keyMaterial, err := generateHMACMaterial(keySpec)
		if err != nil {
			return nil, fmt.Errorf("kms: generate key material: %w", err)
		}
		encMaterial, err := encryptData(p.serverDEK, keyMaterial, []byte(keyID))
		if err != nil {
			return nil, fmt.Errorf("kms: encrypt key material: %w", err)
		}
		e.KeyMaterial = encMaterial
	}
	if err := p.store.CreateKey(ctx, e); err != nil {
		return nil, fmt.Errorf("kms: create key: %w", err)
	}

	keyARN := nr.ResourceID(model.RTKMSKey, keyID)
	return provider.OK(map[string]any{
		"KeyMetadata": keyMetadata(e, keyARN, nr.Region, nr.AccountID),
	}), nil
}

func (p *KeyProvider) DescribeKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	keyARN := nr.ResourceID(model.RTKMSKey, keyID)
	return provider.OK(map[string]any{
		"KeyMetadata": keyMetadata(e, keyARN, nr.Region, nr.AccountID),
	}), nil
}

func (p *KeyProvider) EnableKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setEnabled(ctx, nr, true)
}

func (p *KeyProvider) DisableKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setEnabled(ctx, nr, false)
}

func (p *KeyProvider) setEnabled(ctx context.Context, nr *model.NormalizedRequest, enabled bool) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	e.Enabled = enabled
	if err := p.store.UpdateKey(ctx, e); err != nil {
		return nil, fmt.Errorf("kms: update key: %w", err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *KeyProvider) ScheduleKeyDeletion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	pendingDays := int64(30)
	if v, ok := nr.Params["PendingWindowInDays"]; ok {
		switch n := v.(type) {
		case float64:
			pendingDays = int64(n)
		case json.Number:
			pendingDays, _ = n.Int64()
		}
	}
	if pendingDays < 7 || pendingDays > 30 {
		return nil, model.NewProviderError("ValidationException",
			fmt.Sprintf("PendingWindowInDays should be between 7 and 30, but it is %d", pendingDays), 400)
	}

	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if e.PendingDeletion {
		return nil, model.NewProviderError("KMSInvalidStateException", "key is already pending deletion: "+keyID, 400)
	}

	deletionDate := nr.Clock.Now().Add(time.Duration(pendingDays) * 24 * time.Hour)
	e.Enabled = false
	e.PendingDeletion = true
	e.DeletionDate = deletionDate
	if err := p.store.UpdateKey(ctx, e); err != nil {
		return nil, fmt.Errorf("kms: schedule deletion: %w", err)
	}
	keyARN := nr.ResourceID(model.RTKMSKey, keyID)
	return provider.OK(map[string]any{
		"KeyId":               keyARN,
		"KeyState":            "PendingDeletion",
		"DeletionDate":        deletionDate.Unix(),
		"PendingWindowInDays": pendingDays,
	}), nil
}

func (p *KeyProvider) CancelKeyDeletion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if !e.PendingDeletion {
		return nil, model.NewProviderError("KMSInvalidStateException", "key is not pending deletion: "+keyID, 400)
	}
	e.PendingDeletion = false
	e.DeletionDate = time.Time{}
	e.Enabled = false // AWS: CancelKeyDeletion transitions to Disabled, not Enabled
	if err := p.store.UpdateKey(ctx, e); err != nil {
		return nil, fmt.Errorf("kms: cancel key deletion: %w", err)
	}
	keyARN := nr.ResourceID(model.RTKMSKey, keyID)
	return provider.OK(map[string]any{"KeyId": keyARN}), nil
}

func (p *KeyProvider) ListKeys(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keys, err := p.store.ListKeys(ctx, nr.AccountID)
	if err != nil {
		return nil, fmt.Errorf("kms: list keys: %w", err)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].KeyID < keys[j].KeyID })
	marker, _ := nr.Params["Marker"].(string)
	limit := kmsLimit(nr.Params)
	start := kmsMarkerIndex(keys, marker, func(i int) string { return keys[i].KeyID })
	page := keys[start:]
	truncated := false
	if len(page) > limit {
		page, truncated = page[:limit], true
	}
	items := make([]map[string]any, 0, len(page))
	for _, e := range page {
		items = append(items, map[string]any{
			"KeyId":  e.KeyID,
			"KeyArn": nr.ResourceID(model.RTKMSKey, e.KeyID),
		})
	}
	resp := map[string]any{"Keys": items, "Truncated": truncated}
	if truncated {
		resp["NextMarker"] = page[len(page)-1].KeyID
	}
	return provider.OK(resp), nil
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func (p *KeyProvider) TagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	newTags := extractTags(nr.Params)
	if e.Tags == nil {
		e.Tags = make(map[string]string)
	}
	for k, v := range newTags {
		e.Tags[k] = v
	}
	if err := p.store.UpdateKey(ctx, e); err != nil {
		return nil, fmt.Errorf("kms: tag resource: %w", err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *KeyProvider) UntagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if tagKeys, ok := nr.Params["TagKeys"].([]any); ok {
		for _, k := range tagKeys {
			delete(e.Tags, fmt.Sprint(k))
		}
	}
	if err := p.store.UpdateKey(ctx, e); err != nil {
		return nil, fmt.Errorf("kms: untag resource: %w", err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *KeyProvider) ListResourceTags(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	type kv struct{ k, v string }
	pairs := make([]kv, 0, len(e.Tags))
	for k, v := range e.Tags {
		pairs = append(pairs, kv{k, v})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].k < pairs[j].k })
	marker, _ := nr.Params["Marker"].(string)
	limit := kmsLimit(nr.Params)
	start := kmsMarkerIndex(pairs, marker, func(i int) string { return pairs[i].k })
	page := pairs[start:]
	truncated := false
	if len(page) > limit {
		page, truncated = page[:limit], true
	}
	tags := make([]map[string]string, 0, len(page))
	for _, p := range page {
		tags = append(tags, map[string]string{"TagKey": p.k, "TagValue": p.v})
	}
	resp := map[string]any{"Tags": tags, "Truncated": truncated}
	if truncated {
		resp["NextMarker"] = page[len(page)-1].k
	}
	return provider.OK(resp), nil
}

// ─── Aliases ──────────────────────────────────────────────────────────────────

func (p *KeyProvider) CreateAlias(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	aliasName, _ := nr.Params["AliasName"].(string)
	targetKeyID, _ := nr.Params["TargetKeyId"].(string)
	if aliasName == "" || targetKeyID == "" {
		return nil, model.NewProviderError("ValidationException", "AliasName and TargetKeyId are required", 400)
	}
	if !strings.HasPrefix(aliasName, "alias/") {
		return nil, model.NewProviderError("ValidationException", "AliasName must begin with 'alias/'", 400)
	}
	resolvedID, err := p.resolveKeyIDStr(ctx, targetKeyID)
	if err != nil {
		return nil, err
	}
	if err := p.store.CreateAlias(ctx, AliasEntry{AccountID: nr.AccountID, Region: nr.Region, AliasName: aliasName, TargetKeyID: resolvedID}); err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			return nil, model.NewProviderError("AlreadyExistsException", "alias already exists: "+aliasName, 400)
		}
		return nil, fmt.Errorf("kms: create alias: %w", err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *KeyProvider) DeleteAlias(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	aliasName, _ := nr.Params["AliasName"].(string)
	if err := p.store.DeleteAlias(ctx, aliasName); err != nil {
		if errors.Is(err, ErrAliasNotFound) {
			return nil, model.NewProviderError("NotFoundException", "alias not found: "+aliasName, 400)
		}
		return nil, fmt.Errorf("kms: delete alias: %w", err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *KeyProvider) UpdateAlias(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	aliasName, _ := nr.Params["AliasName"].(string)
	newTarget, _ := nr.Params["TargetKeyId"].(string)
	resolvedID, err := p.resolveKeyIDStr(ctx, newTarget)
	if err != nil {
		return nil, err
	}
	if err := p.store.DeleteAlias(ctx, aliasName); err != nil && !errors.Is(err, ErrAliasNotFound) {
		return nil, fmt.Errorf("kms: update alias delete: %w", err)
	}
	if err := p.store.CreateAlias(ctx, AliasEntry{AccountID: nr.AccountID, Region: nr.Region, AliasName: aliasName, TargetKeyID: resolvedID}); err != nil {
		return nil, fmt.Errorf("kms: update alias create: %w", err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *KeyProvider) ListAliases(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, _ := nr.Params["KeyId"].(string)
	if keyID != "" {
		var err error
		keyID, err = p.resolveKeyIDStr(ctx, keyID)
		if err != nil {
			return nil, err
		}
	}
	aliases, err := p.store.ListAliases(ctx, keyID)
	if err != nil {
		return nil, fmt.Errorf("kms: list aliases: %w", err)
	}
	sort.Slice(aliases, func(i, j int) bool { return aliases[i].AliasName < aliases[j].AliasName })
	marker, _ := nr.Params["Marker"].(string)
	limit := kmsLimit(nr.Params)
	start := kmsMarkerIndex(aliases, marker, func(i int) string { return aliases[i].AliasName })
	page := aliases[start:]
	truncated := false
	if len(page) > limit {
		page, truncated = page[:limit], true
	}
	items := make([]map[string]any, 0, len(page))
	for _, a := range page {
		items = append(items, map[string]any{
			"AliasName":   a.AliasName,
			"TargetKeyId": a.TargetKeyID,
			"AliasArn":    nr.ResourceID(model.RTKMSAlias, strings.TrimPrefix(a.AliasName, "alias/")),
		})
	}
	resp := map[string]any{"Aliases": items, "Truncated": truncated}
	if truncated {
		resp["NextMarker"] = page[len(page)-1].AliasName
	}
	return provider.OK(resp), nil
}

// ─── Key import stubs ─────────────────────────────────────────────────────────

func (p *KeyProvider) GetParametersForImport(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if e.Origin != "EXTERNAL" {
		return nil, model.NewProviderError("UnsupportedOperationException",
			"GetParametersForImport is only supported for keys with Origin=EXTERNAL", 400)
	}
	// Return dummy wrapping key and import token (base64-encoded random bytes).
	dummyKey := make([]byte, 256)
	io.ReadFull(rand.Reader, dummyKey)
	token := make([]byte, 32)
	io.ReadFull(rand.Reader, token)
	return provider.OK(map[string]any{
		"KeyId":             keyID,
		"PublicKey":         base64.StdEncoding.EncodeToString(dummyKey),
		"ImportToken":       base64.StdEncoding.EncodeToString(token),
		"ParametersValidTo": clock.Now().Add(24 * time.Hour).Unix(),
		"KeySpec":           "RSA_2048",
		"WrappingAlgorithm": "RSAES_OAEP_SHA_256",
	}), nil
}

func (p *KeyProvider) ImportKeyMaterial(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return nil, model.NewProviderError("UnsupportedOperationException",
		"Key material import is not supported in this emulator", 400)
}

func (p *KeyProvider) DeleteImportedKeyMaterial(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return nil, model.NewProviderError("UnsupportedOperationException",
		"Key material import is not supported in this emulator", 400)
}

// validGrantOperations is the set of allowed KMS grant operations.
var validGrantOperations = map[string]bool{
	"CreateKey": true, "Decrypt": true, "Encrypt": true,
	"GenerateDataKey": true, "GenerateDataKeyWithoutPlaintext": true,
	"ReEncryptFrom": true, "ReEncryptTo": true,
	"Sign": true, "Verify": true, "GetPublicKey": true,
	"CreateGrant": true, "RetireGrant": true, "DescribeKey": true,
	"GenerateDataKeyPair": true, "GenerateDataKeyPairWithoutPlaintext": true,
}

// ─── Grants ───────────────────────────────────────────────────────────────────

func (p *KeyProvider) CreateGrant(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	granteeARN, _ := nr.Params["GranteePrincipal"].(string)
	retiringPrincipal, _ := nr.Params["RetiringPrincipal"].(string)
	name, _ := nr.Params["Name"].(string)
	ops := extractStringList(nr.Params, "Operations")

	// Validate operations.
	for _, op := range ops {
		if !validGrantOperations[op] {
			return nil, model.NewProviderError("ValidationException", "Invalid grant operation: "+op, 400)
		}
	}

	// Idempotency by name: if Name is set and a grant with same name+keyID exists, return it.
	if name != "" {
		existing, lerr := p.store.ListGrants(ctx, keyID)
		if lerr == nil {
			for _, g := range existing {
				if g.Name == name && g.KeyID == keyID {
					return provider.OK(map[string]any{
						"GrantId":    g.GrantID,
						"GrantToken": g.Token,
					}), nil
				}
			}
		}
	}

	token := newID()
	grantID := newID()
	keyARN := nr.ResourceID(model.RTKMSKey, keyID)
	e := GrantEntry{
		AccountID:         nr.AccountID,
		Region:            nr.Region,
		GrantID:           grantID,
		KeyID:             keyID,
		KeyArn:            keyARN,
		GranteeARN:        granteeARN,
		RetiringPrincipal: retiringPrincipal,
		Name:              name,
		Operations:        ops,
		Token:             token,
		IssuingAccount:    nr.AccountID,
		CreationDate:      clock.Now(),
	}
	if err := p.store.CreateGrant(ctx, e); err != nil {
		return nil, fmt.Errorf("kms: create grant: %w", err)
	}
	return provider.OK(map[string]any{
		"GrantId":    grantID,
		"GrantToken": token,
	}), nil
}

func (p *KeyProvider) RevokeGrant(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	grantID, _ := nr.Params["GrantId"].(string)
	if err := p.store.RevokeGrant(ctx, grantID); err != nil {
		if errors.Is(err, ErrGrantNotFound) {
			return nil, model.NewProviderError("NotFoundException", "grant not found: "+grantID, 400)
		}
		return nil, fmt.Errorf("kms: revoke grant: %w", err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *KeyProvider) RetireGrant(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	grantToken, _ := nr.Params["GrantToken"].(string)
	grantID, _ := nr.Params["GrantId"].(string)

	if grantID == "" && grantToken == "" {
		return nil, model.NewProviderError("ValidationException", "GrantId or GrantToken is required", 400)
	}

	// Token-based retire: look up the grant by token then revoke it.
	if grantToken != "" {
		g, err := p.store.GetGrantByToken(ctx, grantToken)
		if err != nil {
			if errors.Is(err, ErrGrantNotFound) {
				return nil, model.NewProviderError("NotFoundException", "grant not found for token", 400)
			}
			return nil, fmt.Errorf("kms: retire grant by token: %w", err)
		}
		if revokeErr := p.store.RevokeGrant(ctx, g.GrantID); revokeErr != nil && !errors.Is(revokeErr, ErrGrantNotFound) {
			return nil, fmt.Errorf("kms: retire grant: %w", revokeErr)
		}
		return provider.OK(map[string]any{}), nil
	}

	// GrantId-based retire.
	if err := p.store.RevokeGrant(ctx, grantID); err != nil {
		if errors.Is(err, ErrGrantNotFound) {
			// Retire is idempotent — already retired/not found is OK.
			return provider.OK(map[string]any{}), nil
		}
		return nil, fmt.Errorf("kms: retire grant: %w", err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *KeyProvider) ListGrants(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	grants, err := p.store.ListGrants(ctx, keyID)
	if err != nil {
		return nil, fmt.Errorf("kms: list grants: %w", err)
	}

	// Apply optional filters.
	if filterGrantID, ok := nr.Params["GrantId"].(string); ok && filterGrantID != "" {
		filtered := grants[:0]
		for _, g := range grants {
			if g.GrantID == filterGrantID {
				filtered = append(filtered, g)
			}
		}
		grants = filtered
	}
	if filterGrantee, ok := nr.Params["GranteePrincipal"].(string); ok && filterGrantee != "" {
		filtered := grants[:0]
		for _, g := range grants {
			if g.GranteeARN == filterGrantee {
				filtered = append(filtered, g)
			}
		}
		grants = filtered
	}

	sort.Slice(grants, func(i, j int) bool { return grants[i].GrantID < grants[j].GrantID })
	marker, _ := nr.Params["Marker"].(string)
	limit := kmsLimit(nr.Params)
	start := kmsMarkerIndex(grants, marker, func(i int) string { return grants[i].GrantID })
	page := grants[start:]
	truncated := false
	if len(page) > limit {
		page, truncated = page[:limit], true
	}
	items := grantItems(page)
	resp := map[string]any{"Grants": items, "Truncated": truncated}
	if truncated {
		resp["NextMarker"] = page[len(page)-1].GrantID
	}
	return provider.OK(resp), nil
}

// ─── Crypto operations ────────────────────────────────────────────────────────

func (p *KeyProvider) Encrypt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if nr.Origin == model.OriginInternal {
		return p.encryptInternal(ctx, nr)
	}
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	if err := p.checkKeyPolicy(ctx, keyID, nr.AccountID, nr.Region, "kms:Encrypt"); err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if e.PendingDeletion {
		return nil, model.NewProviderError("KMSInvalidStateException", "key is pending deletion", 400)
	}
	if !e.Enabled {
		return nil, model.NewProviderError("DisabledException", "key is disabled", 400)
	}
	if err := checkKeyUsage(nr.ResourceID(model.RTKMSKey, keyID), e.KeyUsage, "Encrypt", "ENCRYPT_DECRYPT"); err != nil {
		return nil, err
	}
	ptB64, _ := nr.Params["Plaintext"].(string)
	pt, err := base64.StdEncoding.DecodeString(ptB64)
	if err != nil {
		return nil, model.NewProviderError("ValidationException", "Plaintext must be base64-encoded", 400)
	}

	// Asymmetric RSA ENCRYPT_DECRYPT key: use RSAES_OAEP.
	if len(e.PrivateKey) > 0 && len(e.KeyMaterial) == 0 {
		algo, _ := nr.Params["EncryptionAlgorithm"].(string)
		if algo == "" {
			algo = "RSAES_OAEP_SHA_256"
		}
		// Validate plaintext size against RSA modulus constraints.
		modulusBytes := rsaModulusBytes(e.KeySpec)
		if modulusBytes > 0 {
			var maxPlaintext int
			switch algo {
			case "RSAES_OAEP_SHA_1":
				maxPlaintext = modulusBytes - 42
			default: // RSAES_OAEP_SHA_256
				maxPlaintext = modulusBytes - 66
			}
			if len(pt) > maxPlaintext {
				return nil, model.NewProviderError("InvalidPlaintextException",
					"The plaintext is too long to be encrypted using the specified key.", 400)
			}
		}
		ct, err := rsaEncryptOAEP(e.PublicKey, pt, algo)
		if err != nil {
			return nil, fmt.Errorf("kms: rsa encrypt: %w", err)
		}
		blob := buildCiphertextBlob(keyID, nr.AccountID, nr.Region, ct)
		return provider.OK(map[string]any{
			"KeyId":               nr.ResourceID(model.RTKMSKey, keyID),
			"CiphertextBlob":      base64.StdEncoding.EncodeToString(blob),
			"EncryptionAlgorithm": algo,
		}), nil
	}

	// Validate symmetric plaintext size.
	if len(pt) > 4096 {
		return nil, model.NewProviderError("InvalidPlaintextException",
			"The plaintext is too long to be encrypted using the specified key.", 400)
	}

	encCtx := extractEncCtx(nr.Params)
	aad := marshalEncCtx(encCtx)

	// Decrypt per-key material, then encrypt the plaintext.
	keyMat, err := decryptData(p.serverDEK, e.KeyMaterial, []byte(keyID))
	if err != nil {
		return nil, fmt.Errorf("kms: load key material: %w", err)
	}
	ct, err := encryptData(keyMat, pt, aad)
	if err != nil {
		return nil, fmt.Errorf("kms: encrypt: %w", err)
	}
	// Prepend key ID so Decrypt can identify the key without caller providing it.
	blob := buildCiphertextBlob(keyID, nr.AccountID, nr.Region, ct)
	return provider.OK(map[string]any{
		"KeyId":               nr.ResourceID(model.RTKMSKey, keyID),
		"CiphertextBlob":      base64.StdEncoding.EncodeToString(blob),
		"EncryptionAlgorithm": "SYMMETRIC_DEFAULT",
	}), nil
}

func (p *KeyProvider) Decrypt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if nr.Origin == model.OriginInternal {
		return p.decryptInternal(ctx, nr)
	}
	ctB64, _ := nr.Params["CiphertextBlob"].(string)
	blob, err := base64.StdEncoding.DecodeString(ctB64)
	if err != nil {
		return nil, model.NewProviderError("ValidationException", "CiphertextBlob must be base64-encoded", 400)
	}
	keyID, blobAccount, _, ct, err := parseCiphertextBlob(blob)
	if err != nil {
		return nil, model.NewProviderError("InvalidCiphertextException", "invalid ciphertext blob", 400)
	}
	// Reject cross-account decryption: the blob was encrypted in a different account.
	if blobAccount != "" && blobAccount != nr.AccountID {
		return nil, model.NewProviderError("IncorrectKeyException",
			"The key ID in the request does not identify a CMK that can perform this operation.", 400)
	}
	if err := p.checkKeyPolicy(ctx, keyID, nr.AccountID, nr.Region, "kms:Decrypt"); err != nil {
		return nil, err
	}
	// If the caller supplied a KeyId, verify it matches the key embedded in the blob.
	if callerKeyID, _ := nr.Params["KeyId"].(string); callerKeyID != "" {
		resolvedCallerKeyID, resolveErr := p.resolveKeyIDStr(ctx, callerKeyID)
		if resolveErr != nil {
			return nil, resolveErr
		}
		if resolvedCallerKeyID != keyID {
			return nil, model.NewProviderError("IncorrectKeyException",
				"The key ID in the request does not identify a CMK that can perform this operation.", 400)
		}
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if e.PendingDeletion {
		return nil, model.NewProviderError("KMSInvalidStateException", "key is pending deletion", 400)
	}
	if !e.Enabled {
		return nil, model.NewProviderError("DisabledException", "key is disabled", 400)
	}
	if err := checkKeyUsage(nr.ResourceID(model.RTKMSKey, keyID), e.KeyUsage, "Decrypt", "ENCRYPT_DECRYPT"); err != nil {
		return nil, err
	}

	// Asymmetric RSA ENCRYPT_DECRYPT key.
	if len(e.PrivateKey) > 0 && len(e.KeyMaterial) == 0 {
		algo, _ := nr.Params["EncryptionAlgorithm"].(string)
		if algo == "" {
			algo = "RSAES_OAEP_SHA_256"
		}
		pt, err := rsaDecryptOAEP(p.serverDEK, e.PrivateKey, keyID, ct, algo)
		if err != nil {
			return nil, model.NewProviderError("InvalidCiphertextException", "decryption failed", 400)
		}
		return provider.OK(map[string]any{
			"KeyId":               nr.ResourceID(model.RTKMSKey, keyID),
			"Plaintext":           base64.StdEncoding.EncodeToString(pt),
			"EncryptionAlgorithm": algo,
		}), nil
	}

	encCtx := extractEncCtx(nr.Params)
	aad := marshalEncCtx(encCtx)

	// Try current key material first, then previous materials (for rotated keys).
	allMaterials := append([][]byte{e.KeyMaterial}, e.PreviousKeyMaterials...)
	var pt []byte
	for _, mat := range allMaterials {
		keyMat, matErr := decryptData(p.serverDEK, mat, []byte(keyID))
		if matErr != nil {
			continue
		}
		if decrypted, decErr := decryptData(keyMat, ct, aad); decErr == nil {
			pt = decrypted
			break
		}
	}
	if pt == nil {
		return nil, model.NewProviderError("InvalidCiphertextException", "decryption failed", 400)
	}
	return provider.OK(map[string]any{
		"KeyId":               nr.ResourceID(model.RTKMSKey, keyID),
		"Plaintext":           base64.StdEncoding.EncodeToString(pt),
		"EncryptionAlgorithm": "SYMMETRIC_DEFAULT",
	}), nil
}

func (p *KeyProvider) GenerateDataKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if nr.Origin == model.OriginInternal {
		return p.generateDataKeyInternal(ctx, nr)
	}
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	if err := p.checkKeyPolicy(ctx, keyID, nr.AccountID, nr.Region, "kms:GenerateDataKey"); err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if e.PendingDeletion {
		return nil, model.NewProviderError("KMSInvalidStateException", "key is pending deletion", 400)
	}
	if !e.Enabled {
		return nil, model.NewProviderError("DisabledException", "key is disabled", 400)
	}
	if err := checkKeyUsage(nr.ResourceID(model.RTKMSKey, keyID), e.KeyUsage, "GenerateDataKey", "ENCRYPT_DECRYPT"); err != nil {
		return nil, err
	}

	bits := 256
	if spec, _ := nr.Params["KeySpec"].(string); spec == "AES_128" {
		bits = 128
	}
	dataKey := make([]byte, bits/8)
	if _, err := io.ReadFull(rand.Reader, dataKey); err != nil {
		return nil, fmt.Errorf("kms: generate data key: %w", err)
	}

	encCtx := extractEncCtx(nr.Params)
	aad := marshalEncCtx(encCtx)
	keyMat, err := decryptData(p.serverDEK, e.KeyMaterial, []byte(keyID))
	if err != nil {
		return nil, fmt.Errorf("kms: load key material: %w", err)
	}
	ct, err := encryptData(keyMat, dataKey, aad)
	if err != nil {
		return nil, fmt.Errorf("kms: encrypt data key: %w", err)
	}
	blob := buildCiphertextBlob(keyID, nr.AccountID, nr.Region, ct)
	return provider.OK(map[string]any{
		"KeyId":               nr.ResourceID(model.RTKMSKey, keyID),
		"Plaintext":           base64.StdEncoding.EncodeToString(dataKey),
		"CiphertextBlob":      base64.StdEncoding.EncodeToString(blob),
		"EncryptionAlgorithm": "SYMMETRIC_DEFAULT",
	}), nil
}

func (p *KeyProvider) GenerateDataKeyWithoutPlaintext(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	resp, err := p.GenerateDataKey(ctx, nr)
	if err != nil {
		return nil, err
	}
	// Remove the plaintext from the response.
	delete(resp.Data, "Plaintext")
	return resp, nil
}

func (p *KeyProvider) ReEncrypt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// Decrypt with source key, then re-encrypt with destination key.
	ctB64, _ := nr.Params["CiphertextBlob"].(string)
	blob, err := base64.StdEncoding.DecodeString(ctB64)
	if err != nil {
		return nil, model.NewProviderError("ValidationException", "CiphertextBlob must be base64-encoded", 400)
	}
	srcKeyID, _, _, ct, err := parseCiphertextBlob(blob)
	if err != nil {
		return nil, model.NewProviderError("InvalidCiphertextException", "invalid source ciphertext", 400)
	}

	if err := p.checkKeyPolicy(ctx, srcKeyID, nr.AccountID, nr.Region, "kms:ReEncryptFrom"); err != nil {
		return nil, err
	}
	srcKey, err := p.store.GetKey(ctx, srcKeyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if err := checkKeyUsage(nr.ResourceID(model.RTKMSKey, srcKeyID), srcKey.KeyUsage, "ReEncrypt", "ENCRYPT_DECRYPT"); err != nil {
		return nil, err
	}
	srcEncCtx := extractEncCtxFrom(nr.Params, "SourceEncryptionContext")
	srcAAD := marshalEncCtx(srcEncCtx)
	srcMat, err := decryptData(p.serverDEK, srcKey.KeyMaterial, []byte(srcKeyID))
	if err != nil {
		return nil, fmt.Errorf("kms: reencrypt load src material: %w", err)
	}
	pt, err := decryptData(srcMat, ct, srcAAD)
	if err != nil {
		return nil, model.NewProviderError("InvalidCiphertextException", "decryption failed", 400)
	}

	dstKeyIDParam, _ := nr.Params["DestinationKeyId"].(string)
	dstKeyID, err := p.resolveKeyIDStr(ctx, dstKeyIDParam)
	if err != nil {
		return nil, err
	}
	if err := p.checkKeyPolicy(ctx, dstKeyID, nr.AccountID, nr.Region, "kms:ReEncryptTo"); err != nil {
		return nil, err
	}
	dstKey, err := p.store.GetKey(ctx, dstKeyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if err := checkKeyUsage(nr.ResourceID(model.RTKMSKey, dstKeyID), dstKey.KeyUsage, "ReEncrypt", "ENCRYPT_DECRYPT"); err != nil {
		return nil, err
	}
	dstEncCtx := extractEncCtxFrom(nr.Params, "DestinationEncryptionContext")
	dstAAD := marshalEncCtx(dstEncCtx)
	dstMat, err := decryptData(p.serverDEK, dstKey.KeyMaterial, []byte(dstKeyID))
	if err != nil {
		return nil, fmt.Errorf("kms: reencrypt load dst material: %w", err)
	}
	newCT, err := encryptData(dstMat, pt, dstAAD)
	if err != nil {
		return nil, fmt.Errorf("kms: reencrypt: %w", err)
	}
	newBlob := buildCiphertextBlob(dstKeyID, nr.AccountID, nr.Region, newCT)
	return provider.OK(map[string]any{
		"KeyId":               nr.ResourceID(model.RTKMSKey, dstKeyID),
		"SourceKeyId":         nr.ResourceID(model.RTKMSKey, srcKeyID),
		"CiphertextBlob":      base64.StdEncoding.EncodeToString(newBlob),
		"EncryptionAlgorithm": "SYMMETRIC_DEFAULT",
	}), nil
}

// ─── Key policy ───────────────────────────────────────────────────────────────

const defaultKeyPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"kms:*","Resource":"*"}]}`

// checkKeyPolicy returns an AccessDeniedException if the caller is not permitted to perform action on keyID.
// callerPrincipal defaults to arn:aws:iam::<accountID>:root.
func (p *KeyProvider) checkKeyPolicy(ctx context.Context, keyID, accountID, region, action string) error {
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil // key not found is handled by the caller
	}
	policy := e.Policy
	if policy == "" {
		policy = defaultKeyPolicy
	}
	caller := fmt.Sprintf("arn:aws:iam::%s:root", accountID) //nolint:hardcoded-arn dynamic account
	if !evalKeyPolicy(policy, caller, action) {
		resource := fmt.Sprintf("arn:aws:kms:%s:%s:key/%s", region, accountID, keyID) //nolint:hardcoded-arn dynamic account+region
		return model.NewProviderError("AccessDeniedException",
			"User: "+caller+" is not authorized to perform: "+action+" on resource: "+resource, 400)
	}
	return nil
}

func (p *KeyProvider) GetKeyPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	policy := e.Policy
	if policy == "" {
		policy = defaultKeyPolicy
	}
	return provider.OK(map[string]any{"Policy": policy}), nil
}

func (p *KeyProvider) PutKeyPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	policy, _ := nr.Params["Policy"].(string)
	e.Policy = policy
	if err := p.store.UpdateKey(ctx, e); err != nil {
		return nil, fmt.Errorf("kms: put key policy: %w", err)
	}
	return provider.OK(map[string]any{}), nil
}

// ─── Key rotation ─────────────────────────────────────────────────────────────

func (p *KeyProvider) GetKeyRotationStatus(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	return provider.OK(map[string]any{
		"KeyRotationEnabled":   e.RotationEnabled,
		"RotationPeriodInDays": e.RotationPeriodInDays,
	}), nil
}

func (p *KeyProvider) EnableKeyRotation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	e.RotationEnabled = true
	if days, ok := nr.Params["RotationPeriodInDays"]; ok {
		if d, ok := days.(float64); ok && d > 0 {
			e.RotationPeriodInDays = int(d)
		}
	}
	if e.RotationPeriodInDays == 0 {
		e.RotationPeriodInDays = 365
	}
	return provider.OK(map[string]any{}), p.store.UpdateKey(ctx, e)
}

func (p *KeyProvider) DisableKeyRotation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	e.RotationEnabled = false
	return provider.OK(map[string]any{}), p.store.UpdateKey(ctx, e)
}

// RotateKeyOnDemand rotates a SYMMETRIC_DEFAULT key by generating new material,
// saving the old material in PreviousKeyMaterials for decryption of old ciphertexts.
func (p *KeyProvider) RotateKeyOnDemand(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if e.PendingDeletion {
		return nil, model.NewProviderError("KMSInvalidStateException", "key is pending deletion", 400)
	}
	if isAsymmetricSpec(e.KeySpec) {
		return nil, model.NewProviderError("UnsupportedOperationException", "on-demand rotation is only supported for SYMMETRIC_DEFAULT keys", 400)
	}
	const onDemandRotationLimit = 10
	if len(e.PreviousKeyMaterials) >= onDemandRotationLimit {
		return nil, model.NewProviderError("LimitExceededException",
			"You have exceeded the maximum number of on-demand rotations (10)", 400)
	}
	newMat, err := Generate32()
	if err != nil {
		return nil, fmt.Errorf("kms: generate rotation material: %w", err)
	}
	encNew, err := encryptData(p.serverDEK, newMat, []byte(keyID))
	if err != nil {
		return nil, fmt.Errorf("kms: encrypt new material: %w", err)
	}
	e.PreviousKeyMaterials = append(e.PreviousKeyMaterials, e.KeyMaterial)
	e.KeyMaterial = encNew
	keyARN := nr.ResourceID(model.RTKMSKey, keyID)
	return provider.OK(map[string]any{"KeyId": keyARN}), p.store.UpdateKey(ctx, e)
}

// ─── Asymmetric operations ────────────────────────────────────────────────────

func (p *KeyProvider) Sign(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	if err := p.checkKeyPolicy(ctx, keyID, nr.AccountID, nr.Region, "kms:Sign"); err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if e.PendingDeletion {
		return nil, model.NewProviderError("KMSInvalidStateException", "key is pending deletion", 400)
	}
	if !e.Enabled {
		return nil, model.NewProviderError("DisabledException", "key is disabled", 400)
	}
	if !isAsymmetricSpec(e.KeySpec) {
		return nil, model.NewProviderError("UnsupportedOperationException", "Sign is only supported for asymmetric keys", 400)
	}
	if err := checkKeyUsage(nr.ResourceID(model.RTKMSKey, keyID), e.KeyUsage, "Sign", "SIGN_VERIFY"); err != nil {
		return nil, err
	}
	msgB64, _ := nr.Params["Message"].(string)
	msg, err := base64.StdEncoding.DecodeString(msgB64)
	if err != nil {
		return nil, model.NewProviderError("ValidationException", "Message must be base64-encoded", 400)
	}
	sigAlgo, _ := nr.Params["SigningAlgorithm"].(string)
	msgType, _ := nr.Params["MessageType"].(string)
	if msgType == "" {
		msgType = "RAW"
	}
	privDER, err := decryptData(p.serverDEK, e.PrivateKey, []byte(keyID))
	if err != nil {
		return nil, fmt.Errorf("kms: load private key: %w", err)
	}
	sig, err := signData(privDER, msg, sigAlgo, msgType)
	if err != nil {
		return nil, model.NewProviderError("ValidationException", err.Error(), 400)
	}
	return provider.OK(map[string]any{
		"KeyId":            nr.ResourceID(model.RTKMSKey, keyID),
		"Signature":        base64.StdEncoding.EncodeToString(sig),
		"SigningAlgorithm": sigAlgo,
	}), nil
}

func (p *KeyProvider) Verify(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	if err := p.checkKeyPolicy(ctx, keyID, nr.AccountID, nr.Region, "kms:Verify"); err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if e.PendingDeletion {
		return nil, model.NewProviderError("KMSInvalidStateException", "key is pending deletion", 400)
	}
	if !e.Enabled {
		return nil, model.NewProviderError("DisabledException", "key is disabled", 400)
	}
	if !isAsymmetricSpec(e.KeySpec) {
		return nil, model.NewProviderError("UnsupportedOperationException", "Verify is only supported for asymmetric keys", 400)
	}
	if err := checkKeyUsage(nr.ResourceID(model.RTKMSKey, keyID), e.KeyUsage, "Verify", "SIGN_VERIFY"); err != nil {
		return nil, err
	}
	msgB64, _ := nr.Params["Message"].(string)
	msg, err := base64.StdEncoding.DecodeString(msgB64)
	if err != nil {
		return nil, model.NewProviderError("ValidationException", "Message must be base64-encoded", 400)
	}
	sigB64, _ := nr.Params["Signature"].(string)
	sig, err := base64.StdEncoding.DecodeString(sigB64)
	if err != nil {
		return nil, model.NewProviderError("ValidationException", "Signature must be base64-encoded", 400)
	}
	sigAlgo, _ := nr.Params["SigningAlgorithm"].(string)
	msgType, _ := nr.Params["MessageType"].(string)
	if msgType == "" {
		msgType = "RAW"
	}
	verifyErr := verifySignature(e.PublicKey, msg, sig, sigAlgo, msgType)
	if verifyErr != nil {
		return nil, model.NewProviderError("KMSInvalidSignatureException",
			"The request was rejected because the specified signature could not be verified.", 400)
	}
	return provider.OK(map[string]any{
		"KeyId":            nr.ResourceID(model.RTKMSKey, keyID),
		"SignatureValid":   true,
		"SigningAlgorithm": sigAlgo,
	}), nil
}

func (p *KeyProvider) GetPublicKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if e.PendingDeletion {
		return nil, model.NewProviderError("KMSInvalidStateException", "key is pending deletion", 400)
	}
	if !isAsymmetricSpec(e.KeySpec) {
		return nil, model.NewProviderError("UnsupportedOperationException", "GetPublicKey is only supported for asymmetric keys", 400)
	}
	resp := map[string]any{
		"KeyId":     nr.ResourceID(model.RTKMSKey, keyID),
		"KeySpec":   e.KeySpec,
		"KeyUsage":  e.KeyUsage,
		"PublicKey": base64.StdEncoding.EncodeToString(e.PublicKey),
	}
	if e.KeyUsage == "SIGN_VERIFY" {
		resp["SigningAlgorithms"] = signingAlgorithmsForSpec(e.KeySpec)
	} else {
		resp["EncryptionAlgorithms"] = encryptionAlgorithmsForSpec(e.KeySpec)
	}
	return provider.OK(resp), nil
}

// ─── P4.5: GenerateDataKeyPair ────────────────────────────────────────────────

func (p *KeyProvider) GenerateDataKeyPair(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if e.KeyUsage != "ENCRYPT_DECRYPT" {
		return nil, model.NewProviderError("InvalidKeyUsageException",
			"GenerateDataKeyPair requires a key with KeyUsage ENCRYPT_DECRYPT", 400)
	}
	if e.KeySpec != "SYMMETRIC_DEFAULT" {
		return nil, model.NewProviderError("InvalidKeyUsageException",
			"GenerateDataKeyPair is not supported for asymmetric CMKs", 400)
	}
	if !e.Enabled {
		return nil, model.NewProviderError("DisabledException", "key is disabled", 400)
	}
	if e.PendingDeletion {
		return nil, model.NewProviderError("KMSInvalidStateException", "key is pending deletion", 400)
	}

	keyPairSpec, _ := nr.Params["KeyPairSpec"].(string)
	if keyPairSpec == "" {
		return nil, model.NewProviderError("ValidationException", "KeyPairSpec is required", 400)
	}

	privDER, pubDER, genErr := generateAsymmetricKey(keyPairSpec)
	if genErr != nil {
		return nil, model.NewProviderError("ValidationException", genErr.Error(), 400)
	}

	encCtx := extractEncCtx(nr.Params)
	aad := marshalEncCtx(encCtx)

	keyMat, matErr := decryptData(p.serverDEK, e.KeyMaterial, []byte(keyID))
	if matErr != nil {
		return nil, fmt.Errorf("kms: load key material: %w", matErr)
	}
	encPriv, encErr := encryptData(keyMat, privDER, aad)
	if encErr != nil {
		return nil, fmt.Errorf("kms: encrypt private key: %w", encErr)
	}
	blob := buildCiphertextBlob(keyID, nr.AccountID, nr.Region, encPriv)

	keyARN := nr.ResourceID(model.RTKMSKey, keyID)
	return provider.OK(map[string]any{
		"KeyId":                    keyARN,
		"KeyPairSpec":              keyPairSpec,
		"PublicKey":                base64.StdEncoding.EncodeToString(pubDER),
		"PrivateKeyPlaintext":      base64.StdEncoding.EncodeToString(privDER),
		"PrivateKeyCiphertextBlob": base64.StdEncoding.EncodeToString(blob),
	}), nil
}

func (p *KeyProvider) GenerateDataKeyPairWithoutPlaintext(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	resp, err := p.GenerateDataKeyPair(ctx, nr)
	if err != nil {
		return nil, err
	}
	delete(resp.Data, "PrivateKeyPlaintext")
	return resp, nil
}

// ─── P4.6: GenerateMac / VerifyMac ───────────────────────────────────────────

func (p *KeyProvider) GenerateMac(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if e.KeyUsage != "GENERATE_VERIFY_MAC" {
		return nil, model.NewProviderError("InvalidKeyUsageException",
			fmt.Sprintf("key usage is %s which is not valid for GenerateMac", e.KeyUsage), 400)
	}
	if !e.Enabled {
		return nil, model.NewProviderError("DisabledException", "key is disabled", 400)
	}
	if e.PendingDeletion {
		return nil, model.NewProviderError("KMSInvalidStateException", "key is pending deletion", 400)
	}

	msgB64, _ := nr.Params["Message"].(string)
	msg, decErr := base64.StdEncoding.DecodeString(msgB64)
	if decErr != nil {
		return nil, model.NewProviderError("ValidationException", "Message must be base64-encoded", 400)
	}
	if len(msg) > 4096 {
		return nil, model.NewProviderError("ValidationException",
			"1 validation error detected: Value at 'message' failed to satisfy constraint: Member must have length less than or equal to 4096", 400)
	}

	macAlgo, _ := nr.Params["MacAlgorithm"].(string)
	if valErr := validateMacAlgorithm(e.KeySpec, macAlgo); valErr != nil {
		if strings.Contains(valErr.Error(), "not compatible") {
			return nil, model.NewProviderError("InvalidKeyUsageException", valErr.Error(), 400)
		}
		return nil, model.NewProviderError("ValidationException", valErr.Error(), 400)
	}

	keyMat, matErr := decryptData(p.serverDEK, e.KeyMaterial, []byte(keyID))
	if matErr != nil {
		return nil, fmt.Errorf("kms: load key material: %w", matErr)
	}

	mac, macErr := computeHMAC(keyMat, msg, macAlgo)
	if macErr != nil {
		return nil, fmt.Errorf("kms: compute mac: %w", macErr)
	}

	keyARN := nr.ResourceID(model.RTKMSKey, keyID)
	return provider.OK(map[string]any{
		"Mac":          base64.StdEncoding.EncodeToString(mac),
		"MacAlgorithm": macAlgo,
		"KeyId":        keyARN,
	}), nil
}

func (p *KeyProvider) VerifyMac(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if e.KeyUsage != "GENERATE_VERIFY_MAC" {
		return nil, model.NewProviderError("InvalidKeyUsageException",
			fmt.Sprintf("key usage is %s which is not valid for VerifyMac", e.KeyUsage), 400)
	}
	if !e.Enabled {
		return nil, model.NewProviderError("DisabledException", "key is disabled", 400)
	}

	msgB64, _ := nr.Params["Message"].(string)
	msg, _ := base64.StdEncoding.DecodeString(msgB64)
	if len(msg) > 4096 {
		return nil, model.NewProviderError("ValidationException", "message too long", 400)
	}
	macAlgo, _ := nr.Params["MacAlgorithm"].(string)
	providedMacB64, _ := nr.Params["Mac"].(string)
	providedMac, _ := base64.StdEncoding.DecodeString(providedMacB64)

	if valErr := validateMacAlgorithm(e.KeySpec, macAlgo); valErr != nil {
		if strings.Contains(valErr.Error(), "not compatible") {
			return nil, model.NewProviderError("InvalidKeyUsageException", valErr.Error(), 400)
		}
		return nil, model.NewProviderError("ValidationException", valErr.Error(), 400)
	}

	keyMat, matErr := decryptData(p.serverDEK, e.KeyMaterial, []byte(keyID))
	if matErr != nil {
		return nil, fmt.Errorf("kms: load key material: %w", matErr)
	}

	computed, _ := computeHMAC(keyMat, msg, macAlgo)
	if !hmacEqual(computed, providedMac) {
		return nil, model.NewProviderError("KMSInvalidMacException", "MAC verification failed", 400)
	}

	keyARN := nr.ResourceID(model.RTKMSKey, keyID)
	return provider.OK(map[string]any{
		"KeyId":        keyARN,
		"MacValid":     true,
		"MacAlgorithm": macAlgo,
	}), nil
}

// ─── P4.7: GenerateRandom ─────────────────────────────────────────────────────

func (p *KeyProvider) GenerateRandom(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	numBytes := 0
	if v, ok := nr.Params["NumberOfBytes"]; ok {
		switch n := v.(type) {
		case float64:
			numBytes = int(n)
		case json.Number:
			i, _ := n.Int64()
			numBytes = int(i)
		}
	} else {
		return nil, model.NewProviderError("ValidationException", "NumberOfBytes is required.", 400)
	}
	if numBytes < 1 {
		return nil, model.NewProviderError("ValidationException",
			fmt.Sprintf("1 validation error detected: Value '%d' at 'numberOfBytes' failed to satisfy constraint: Member must have value greater than or equal to 1", numBytes), 400)
	}
	if numBytes > 1024 {
		return nil, model.NewProviderError("ValidationException",
			fmt.Sprintf("1 validation error detected: Value '%d' at 'numberOfBytes' failed to satisfy constraint: Member must have value less than or equal to 1024", numBytes), 400)
	}

	rb := make([]byte, numBytes)
	io.ReadFull(rand.Reader, rb)
	return provider.OK(map[string]any{
		"Plaintext": base64.StdEncoding.EncodeToString(rb),
	}), nil
}

// ─── KeyEncryptor interface (injected into SecretProvider etc.) ───────────────

// Encrypt implements model.KeyEncryptor.
func (p *KeyProvider) Encrypt2(ctx context.Context, keyID string, pt []byte, encCtx map[string]string) ([]byte, error) {
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if !e.Enabled {
		return nil, fmt.Errorf("kms: key disabled")
	}
	aad := marshalEncCtx(encCtx)
	keyMat, err := decryptData(p.serverDEK, e.KeyMaterial, []byte(keyID))
	if err != nil {
		return nil, fmt.Errorf("kms: load key material: %w", err)
	}
	ct, err := encryptData(keyMat, pt, aad)
	if err != nil {
		return nil, fmt.Errorf("kms: encrypt: %w", err)
	}
	// Extract account+region from keyID ARN if available (internal cross-service calls).
	encAccount, encRegion := "", ""
	if parsed, aErr := awsarn.Parse(keyID); aErr == nil {
		encAccount, encRegion = parsed.AccountID, parsed.Region
	}
	return buildCiphertextBlob(keyID, encAccount, encRegion, ct), nil
}

// Decrypt2 implements model.KeyEncryptor.
func (p *KeyProvider) Decrypt2(ctx context.Context, _ string, ctBlob []byte, encCtx map[string]string) ([]byte, error) {
	keyID, _, _, ct, err := parseCiphertextBlob(ctBlob)
	if err != nil {
		return nil, fmt.Errorf("kms: invalid ciphertext blob")
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if !e.Enabled {
		return nil, fmt.Errorf("kms: key disabled")
	}
	aad := marshalEncCtx(encCtx)
	keyMat, err := decryptData(p.serverDEK, e.KeyMaterial, []byte(keyID))
	if err != nil {
		return nil, fmt.Errorf("kms: load key material: %w", err)
	}
	return decryptData(keyMat, ct, aad)
}

// GenerateDataKey2 implements model.KeyEncryptor.
func (p *KeyProvider) GenerateDataKey2(ctx context.Context, keyID string, bits int) (ptDEK, ctDEK []byte, err error) {
	fakeNR := &model.NormalizedRequest{
		Params:     map[string]any{"KeyId": keyID},
		ResourceID: func(_, name string) string { return name },
	}
	resp, err := p.GenerateDataKey(ctx, fakeNR)
	if err != nil {
		return nil, nil, err
	}
	ptB64, _ := resp.Data["Plaintext"].(string)
	ctB64, _ := resp.Data["CiphertextBlob"].(string)
	ptDEK, _ = base64.StdEncoding.DecodeString(ptB64)
	ctDEK, _ = base64.StdEncoding.DecodeString(ctB64)
	return ptDEK, ctDEK, nil
}

// AsKeyEncryptor returns the KeyProvider as a model.KeyEncryptor by wrapping it.
func (p *KeyProvider) AsKeyEncryptor() model.KeyEncryptor {
	return &keyEncryptorAdapter{p}
}

type keyEncryptorAdapter struct{ kp *KeyProvider }

func (a *keyEncryptorAdapter) Encrypt(ctx context.Context, keyID string, pt []byte, encCtx map[string]string) ([]byte, error) {
	return a.kp.Encrypt2(ctx, keyID, pt, encCtx)
}
func (a *keyEncryptorAdapter) Decrypt(ctx context.Context, keyID string, ct []byte, encCtx map[string]string) ([]byte, error) {
	return a.kp.Decrypt2(ctx, keyID, ct, encCtx)
}
func (a *keyEncryptorAdapter) GenerateDataKey(ctx context.Context, keyID string, bits int) ([]byte, []byte, error) {
	return a.kp.GenerateDataKey2(ctx, keyID, bits)
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func (p *KeyProvider) resolveKeyID(ctx context.Context, nr *model.NormalizedRequest) (string, error) {
	raw, _ := nr.Params["KeyId"].(string)
	return p.resolveKeyIDStr(ctx, raw)
}

func (p *KeyProvider) resolveKeyIDStr(ctx context.Context, raw string) (string, error) {
	if raw == "" {
		return "", model.NewProviderError("ValidationException", "KeyId is required", 400)
	}
	// Already a bare UUID.
	if len(raw) == ciphertextKeyIDLen && !strings.Contains(raw, ":") && !strings.Contains(raw, "/") {
		return raw, nil
	}
	// ARN: arn:aws:kms:region:account:key/UUID
	if strings.Contains(raw, ":key/") {
		parts := strings.Split(raw, "/")
		return parts[len(parts)-1], nil
	}
	// Alias ARN: arn:aws:kms:region:account:alias/name
	if strings.Contains(raw, ":alias/") {
		idx := strings.Index(raw, ":alias/")
		raw = raw[idx+1:] // "alias/name"
	}
	// alias/name
	if strings.HasPrefix(raw, "alias/") {
		a, err := p.store.GetAlias(ctx, raw)
		if err != nil {
			if errors.Is(err, ErrAliasNotFound) {
				return "", model.NewProviderError("NotFoundException", "alias not found: "+raw, 400)
			}
			return "", fmt.Errorf("kms: get alias: %w", err)
		}
		return a.TargetKeyID, nil
	}
	return raw, nil
}

func (p *KeyProvider) keyErr(err error) error {
	if errors.Is(err, ErrKeyNotFound) {
		return model.NewProviderError("NotFoundException", "key not found", 400)
	}
	if errors.Is(err, ErrKeyDisabled) {
		return model.NewProviderError("DisabledException", "key is disabled", 400)
	}
	return err
}

// checkKeyUsage returns InvalidKeyUsageException if keyUsage != required.
// Error format matches AWS: "<keyARN> key usage is <keyUsage> which is not valid for <operation>."
func checkKeyUsage(keyARN, keyUsage, operation, required string) error {
	if keyUsage != required {
		return model.NewProviderError("InvalidKeyUsageException",
			fmt.Sprintf("%s key usage is %s which is not valid for %s.", keyARN, keyUsage, operation), 400)
	}
	return nil
}

func newID() string {
	b := make([]byte, 16)
	io.ReadFull(rand.Reader, b)
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant bits
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// buildCiphertextBlob encodes a v2 blob:
// [0x02][keyUUID(36)][account_len(2)][account][region_len(2)][region][ciphertext]
// account and region may be empty strings (for internal/cross-service paths).
func buildCiphertextBlob(keyID, account, region string, ct []byte) []byte {
	accountB := []byte(account)
	regionB := []byte(region)
	size := 1 + ciphertextKeyIDLen + 2 + len(accountB) + 2 + len(regionB) + len(ct)
	blob := make([]byte, size)
	pos := 0
	blob[pos] = 0x02
	pos++
	copy(blob[pos:], keyID)
	pos += ciphertextKeyIDLen
	binary.BigEndian.PutUint16(blob[pos:], uint16(len(accountB)))
	pos += 2
	copy(blob[pos:], accountB)
	pos += len(accountB)
	binary.BigEndian.PutUint16(blob[pos:], uint16(len(regionB)))
	pos += 2
	copy(blob[pos:], regionB)
	pos += len(regionB)
	copy(blob[pos:], ct)
	return blob
}

// parseCiphertextBlob decodes a ciphertext blob, supporting v1 (no version byte) and v2 (version 0x02).
// Returns keyID, account, region, ciphertext. For v1 blobs, account and region are "".
func parseCiphertextBlob(blob []byte) (keyID, account, region string, ct []byte, err error) {
	if len(blob) == 0 {
		return "", "", "", nil, fmt.Errorf("ciphertext blob too short")
	}
	// v2 blob: first byte is 0x02 (not a valid hex char for UUID).
	if blob[0] == 0x02 {
		minLen := 1 + ciphertextKeyIDLen + 2 + 2 // version + uuid + account_len + region_len
		if len(blob) < minLen {
			return "", "", "", nil, fmt.Errorf("ciphertext blob too short")
		}
		pos := 1
		keyID = string(blob[pos : pos+ciphertextKeyIDLen])
		pos += ciphertextKeyIDLen
		aLen := int(binary.BigEndian.Uint16(blob[pos:]))
		pos += 2
		if len(blob) < pos+aLen+2 {
			return "", "", "", nil, fmt.Errorf("ciphertext blob truncated in account field")
		}
		account = string(blob[pos : pos+aLen])
		pos += aLen
		rLen := int(binary.BigEndian.Uint16(blob[pos:]))
		pos += 2
		if len(blob) < pos+rLen {
			return "", "", "", nil, fmt.Errorf("ciphertext blob truncated in region field")
		}
		region = string(blob[pos : pos+rLen])
		pos += rLen
		ct = blob[pos:]
		return keyID, account, region, ct, nil
	}
	// v1 legacy blob: [keyUUID(36)][ciphertext]
	if len(blob) < ciphertextKeyIDLen+ivLen+tagLen {
		return "", "", "", nil, fmt.Errorf("ciphertext blob too short")
	}
	keyID = string(blob[:ciphertextKeyIDLen])
	ct = blob[ciphertextKeyIDLen:]
	return keyID, "", "", ct, nil
}

// marshalEncCtx serialises an encryption context map deterministically (sorted keys).
// Used as AES-GCM additional data so different contexts produce different ciphertexts.
func marshalEncCtx(m map[string]string) []byte {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		pair, _ := json.Marshal(map[string]string{k: m[k]})
		parts = append(parts, string(pair))
	}
	return []byte(strings.Join(parts, ","))
}

func extractEncCtx(params map[string]any) map[string]string {
	return extractEncCtxFrom(params, "EncryptionContext")
}

func extractEncCtxFrom(params map[string]any, key string) map[string]string {
	raw, ok := params[key]
	if !ok {
		return nil
	}
	switch v := raw.(type) {
	case map[string]any:
		out := make(map[string]string, len(v))
		for k, val := range v {
			out[k] = fmt.Sprint(val)
		}
		return out
	case map[string]string:
		return v
	}
	return nil
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
				k, _ := m["TagKey"].(string)
				val, _ := m["TagValue"].(string)
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

func keyMetadata(e KeyEntry, arn, region, accountID string) map[string]any {
	createdAt := e.CreatedAt
	if createdAt.IsZero() {
		createdAt = clock.Now()
	}
	m := map[string]any{
		"KeyId":        e.KeyID,
		"Arn":          arn,
		"Enabled":      e.Enabled,
		"Description":  e.Description,
		"KeyUsage":     e.KeyUsage,
		"KeySpec":      e.KeySpec,
		"Origin":       e.Origin,
		"KeyState":     keyState(e),
		"AWSAccountId": accountID,
		"CreationDate": createdAt.Unix(),
		"MultiRegion":  e.MultiRegion,
		"KeyManager":   "CUSTOMER",
	}
	if algos := encryptionAlgorithmsForSpec(e.KeySpec); len(algos) > 0 {
		m["EncryptionAlgorithms"] = algos
	}
	if algos := signingAlgorithmsForSpec(e.KeySpec); len(algos) > 0 {
		m["SigningAlgorithms"] = algos
	}
	if e.PendingDeletion && !e.DeletionDate.IsZero() {
		m["DeletionDate"] = e.DeletionDate.Unix()
	}
	return m
}

func keyState(e KeyEntry) string {
	if e.PendingDeletion {
		return "PendingDeletion"
	}
	if e.Enabled {
		return "Enabled"
	}
	return "Disabled"
}

// ensure hex import is used
var _ = hex.EncodeToString

// ─── New operations (2.1-2.5) ────────────────────────────────────────────────

func (p *KeyProvider) ListRetirableGrants(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	principal, _ := nr.Params["RetiringPrincipal"].(string)
	if principal == "" {
		return nil, model.NewProviderError("ValidationException", "RetiringPrincipal is required", 400)
	}
	all, err := p.store.ListGrants(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("kms: list retirable grants: %w", err)
	}
	var filtered []GrantEntry
	for _, g := range all {
		if g.RetiringPrincipal == principal {
			filtered = append(filtered, g)
		}
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].GrantID < filtered[j].GrantID })
	marker, _ := nr.Params["Marker"].(string)
	limit := kmsLimit(nr.Params)
	start := kmsMarkerIndex(filtered, marker, func(i int) string { return filtered[i].GrantID })
	page := filtered[start:]
	truncated := false
	if len(page) > limit {
		page, truncated = page[:limit], true
	}
	resp := map[string]any{"Grants": grantItems(page), "Truncated": truncated}
	if truncated {
		resp["NextMarker"] = page[len(page)-1].GrantID
	}
	return provider.OK(resp), nil
}

func (p *KeyProvider) ListKeyPolicies(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if _, err := p.resolveKeyID(ctx, nr); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"PolicyNames": []string{"default"}, "Truncated": false}), nil
}

func (p *KeyProvider) DeriveSharedSecret(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if e.PendingDeletion {
		return nil, model.NewProviderError("KMSInvalidStateException", "key is pending deletion", 400)
	}
	if !e.Enabled {
		return nil, model.NewProviderError("DisabledException", "key is disabled", 400)
	}
	if !strings.HasPrefix(e.KeySpec, "ECC_") {
		return nil, model.NewProviderError("InvalidKeyUsageException", "DeriveSharedSecret requires an ECC key", 400)
	}
	peerB64, _ := nr.Params["PublicKey"].(string)
	peerPubDER, err := base64.StdEncoding.DecodeString(peerB64)
	if err != nil {
		return nil, model.NewProviderError("ValidationException", "PublicKey must be base64-encoded DER", 400)
	}
	shared, err := ecdhSharedSecret(p.serverDEK, e.PrivateKey, keyID, peerPubDER)
	if err != nil {
		return nil, model.NewProviderError("InvalidKeyUsageException", "ECDH key agreement failed: "+err.Error(), 400)
	}
	return provider.OK(map[string]any{
		"KeyId":                 nr.ResourceID(model.RTKMSKey, keyID),
		"SharedSecret":          base64.StdEncoding.EncodeToString(shared),
		"KeyAgreementAlgorithm": "ECDH",
	}), nil
}

// ─── pagination helpers ───────────────────────────────────────────────────────

func kmsLimit(params map[string]any) int {
	limit := 100
	switch v := params["Limit"].(type) {
	case float64:
		if int(v) > 0 {
			limit = int(v)
		}
	case json.Number:
		if n, err := v.Int64(); err == nil && n > 0 {
			limit = int(n)
		}
	}
	if limit > 1000 {
		limit = 1000
	}
	return limit
}

// kmsMarkerIndex returns the slice index to start from given a marker string.
// keyFn extracts the sort key from element i. Returns 0 when marker is empty or not found.
func kmsMarkerIndex[T any](items []T, marker string, keyFn func(int) string) int {
	if marker == "" || len(items) == 0 {
		return 0
	}
	for i := range items {
		if keyFn(i) > marker {
			return i
		}
	}
	return len(items) // beyond last element → empty page
}

func (p *KeyProvider) UpdateKeyDescription(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, err := p.resolveKeyID(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.store.GetKey(ctx, keyID)
	if err != nil {
		return nil, p.keyErr(err)
	}
	desc, _ := nr.Params["Description"].(string)
	e.Description = desc
	if err := p.store.UpdateKey(ctx, e); err != nil {
		return nil, fmt.Errorf("kms: update key description: %w", err)
	}
	return provider.OK(map[string]any{}), nil
}

func grantItems(grants []GrantEntry) []map[string]any {
	items := make([]map[string]any, 0, len(grants))
	for _, g := range grants {
		items = append(items, map[string]any{
			"GrantId":           g.GrantID,
			"KeyId":             g.KeyID,
			"GranteePrincipal":  g.GranteeARN,
			"RetiringPrincipal": g.RetiringPrincipal,
			"Operations":        g.Operations,
			"Name":              g.Name,
			"IssuingAccount":    g.IssuingAccount,
			"CreationDate":      g.CreationDate.Unix(),
			"GrantToken":        g.Token,
		})
	}
	return items
}
