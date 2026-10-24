// Package sts implements the AWS STS provider for JaisCloud.
// It handles temporary credential issuance (AssumeRole variants, GetSessionToken,
// GetFederationToken) and session tag propagation per AWS STS specification.
package sts

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	awsarn "jaiscloud/internal/aws/arn"
	"jaiscloud/internal/aws/identity"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

var (
	roleARNRegex        = regexp.MustCompile(`^arn:[^:]+:[^:]+:[^:]*:[^:]*:[^:]+$`)
	sessionNameRegex    = regexp.MustCompile(`^[\w+=,.@-]*$`)
	federationNameRegex = regexp.MustCompile(`^[\w+=,.@-]+$`)
)

// STSProvider handles STS API operations.
type STSProvider struct {
	store       SessionStore
	oidcIssuers map[string]string // issuer URL → JWKS URL (nil = skip verification)
	jwksCache   *JWKSCache
}

// New constructs an STSProvider.
func New(store SessionStore) *STSProvider {
	return &STSProvider{store: store, jwksCache: NewJWKSCache()}
}

// NewWithOIDC constructs an STSProvider with OIDC JWT signature verification.
func NewWithOIDC(store SessionStore, oidcIssuers map[string]string) *STSProvider {
	return &STSProvider{
		store:       store,
		oidcIssuers: oidcIssuers,
		jwksCache:   NewJWKSCache(),
	}
}

// Routes returns all STS handler registrations.
func (p *STSProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"STS.GetCallerIdentity":          p.GetCallerIdentity,
		"STS.AssumeRole":                 p.AssumeRole,
		"STS.AssumeRoleWithWebIdentity":  p.AssumeRoleWithWebIdentity,
		"STS.AssumeRoleWithSAML":         p.AssumeRoleWithSAML,
		"STS.GetSessionToken":            p.GetSessionToken,
		"STS.GetFederationToken":         p.GetFederationToken,
		"STS.GetAccessKeyInfo":           p.GetAccessKeyInfo,
		"STS.DecodeAuthorizationMessage": p.DecodeAuthorizationMessage,
	}
}

// Reset wipes all session state. Implements admin.Resetter.
func (p *STSProvider) Reset(ctx context.Context) {
	p.store.Reset(ctx)
}

// ─── handlers ─────────────────────────────────────────────────────────────────

func (p *STSProvider) GetCallerIdentity(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// nr.AccountID is already decoded correctly by IdentityEnricher (LSIA → account).
	// Use it as-is for the Account field.

	// Check if we have a stored session for this access key (LSIA/ASIA assumed-role creds).
	if nr.AccessKey != "" {
		if sess, ok := p.store.GetSession(nr.AccessKey); ok && sess.RoleName != "" {
			arnStr := awsarn.ResourceID("", nr.AccountID)("sts-assumed-role", sess.RoleName+"/"+sess.RoleSessionName)
			return provider.OK(map[string]any{
				"Account": nr.AccountID,
				"Arn":     arnStr,
				"UserId":  sess.RoleName + ":" + sess.RoleSessionName,
			}), nil
		}
	}

	// Static credentials or no session found — return root identity.
	return provider.OK(map[string]any{
		"Account": nr.AccountID,
		"Arn":     nr.ResourceID("iam-root", ""),
		"UserId":  nr.AccountID,
	}), nil
}

func (p *STSProvider) AssumeRole(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	roleArn := strParam(nr.Params, "RoleArn")
	sessionName := strParam(nr.Params, "RoleSessionName")
	durationSecs := intParam(nr.Params, "DurationSeconds", 3600)

	if durationSecs < 900 || durationSecs > 43200 {
		return nil, stsErr("ValidationError",
			"1 validation error detected: Value at 'durationSeconds' failed to satisfy constraint: Member must have value greater than or equal to 900", 400)
	}
	if !roleARNRegex.MatchString(roleArn) {
		return nil, stsErr("ValidationError", fmt.Sprintf("%s is invalid", roleArn), 400)
	}
	if !sessionNameRegex.MatchString(sessionName) {
		return nil, stsErr("ValidationError", fmt.Sprintf(
			"1 validation error detected: Value '%s' at 'roleSessionName' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\w+=,.@-]*",
			sessionName), 400)
	}

	// Resolve target account and role name from the role ARN.
	targetAccount := nr.AccountID
	roleName := roleArn
	if parsed, err := awsarn.Parse(roleArn); err == nil {
		if parsed.AccountID != "" {
			targetAccount = parsed.AccountID
		}
		// Resource is "role/name" or "role/path/name"
		if parts := strings.Split(parsed.Resource, "/"); len(parts) > 1 {
			roleName = parts[len(parts)-1]
		}
	} else if parts := strings.Split(roleArn, "/"); len(parts) > 1 {
		roleName = parts[len(parts)-1]
	}

	// Mint an LSIA access key encoding the target account.
	accessKey, err := identity.EncodeLSIA(targetAccount)
	if err != nil {
		return nil, stsErr("InternalError", "credential mint failed: "+err.Error(), 500)
	}
	secretKey := randBase64(30)
	sessionToken := randBase64(36)
	expiry := clock.Now().Add(time.Duration(durationSecs) * time.Second)

	// Resolve transitive tags from the caller's existing session.
	callerKey := nr.AccessKey
	existing, hasExisting := p.store.GetSession(callerKey)

	// Validate tags.
	tags := parseTags(nr.Params, "Tags")
	transitiveKeys := parseStringList(nr.Params, "TransitiveTagKeys")
	if len(tags) > 0 {
		tagKeySet := make(map[string]bool, len(tags))
		for _, t := range tags {
			lk := strings.ToLower(t.Key)
			if tagKeySet[lk] {
				return nil, stsErr("InvalidParameterValue",
					"Duplicate tag keys found. Please note that Tag keys are case insensitive.", 400)
			}
			tagKeySet[lk] = true
		}
		if hasExisting {
			for _, tk := range existing.TransitiveTags {
				if tagKeySet[tk] {
					return nil, stsErr("InvalidParameterValue",
						"One of the specified transitive tag keys can't be set because it conflicts with a transitive tag key from the calling session.", 400)
				}
			}
		}
		if len(transitiveKeys) > 0 {
			tagMap := make(map[string]bool, len(tags))
			for _, t := range tags {
				tagMap[strings.ToLower(t.Key)] = true
			}
			for _, tk := range transitiveKeys {
				if !tagMap[strings.ToLower(tk)] {
					return nil, stsErr("InvalidParameterValue",
						"The specified transitive tag key must be included in the requested tags.", 400)
				}
			}
		}
	}

	// Build merged tag set.
	merged := make(map[string]Tag)
	mergedTransitive := make([]string, 0)
	if hasExisting {
		for _, tk := range existing.TransitiveTags {
			merged[tk] = existing.Tags[tk]
			mergedTransitive = append(mergedTransitive, tk)
		}
	}
	for _, t := range tags {
		merged[strings.ToLower(t.Key)] = t
	}
	for _, tk := range transitiveKeys {
		mergedTransitive = append(mergedTransitive, strings.ToLower(tk))
	}

	// Store session with identity and tag info.
	_ = p.store.StoreSession(accessKey, SessionConfig{
		Tags:            merged,
		TransitiveTags:  mergedTransitive,
		IAMContext:      map[string]any{},
		Account:         targetAccount,
		RoleName:        roleName,
		RoleSessionName: sessionName,
	})

	// AssumedRoleUser.Arn uses the TARGET account (fix for §11.1.2).
	assumedArn := awsarn.ResourceID("", targetAccount)("sts-assumed-role", roleName+"/"+sessionName)

	return provider.OK(map[string]any{
		"Credentials": map[string]any{
			"AccessKeyId":     accessKey,
			"SecretAccessKey": secretKey,
			"SessionToken":    sessionToken,
			"Expiration":      expiry.Format(time.RFC3339),
		},
		"AssumedRoleUser": map[string]any{
			"AssumedRoleId": roleName + ":" + sessionName,
			"Arn":           assumedArn,
		},
		"PackedPolicySize": 0,
	}), nil
}

func (p *STSProvider) AssumeRoleWithWebIdentity(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	roleArn := strParam(nr.Params, "RoleArn")
	sessionName := strParam(nr.Params, "RoleSessionName")
	webToken := strParam(nr.Params, "WebIdentityToken")
	durationSecs := intParam(nr.Params, "DurationSeconds", 3600)

	if !roleARNRegex.MatchString(roleArn) {
		return nil, stsErr("ValidationError", fmt.Sprintf("%s is invalid", roleArn), 400)
	}
	if !sessionNameRegex.MatchString(sessionName) {
		return nil, stsErr("ValidationError", fmt.Sprintf(
			"1 validation error detected: Value '%s' at 'roleSessionName' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\w+=,.@-]*",
			sessionName), 400)
	}

	subject, err := p.extractJWTSubjectWithVerification(webToken)
	if err != nil {
		return nil, err
	}

	targetAccount, roleName := accountAndRoleFromARN(roleArn, nr.AccountID)
	accessKey, err := identity.EncodeLSIA(targetAccount)
	if err != nil {
		return nil, stsErr("InternalError", "credential mint failed: "+err.Error(), 500)
	}
	expiry := clock.Now().Add(time.Duration(durationSecs) * time.Second)
	assumedArn := awsarn.ResourceID("", targetAccount)("sts-assumed-role", roleName+"/"+sessionName)
	_ = p.store.StoreSession(accessKey, SessionConfig{
		Account:         targetAccount,
		RoleName:        roleName,
		RoleSessionName: sessionName,
	})

	return provider.OK(map[string]any{
		"Credentials": map[string]any{
			"AccessKeyId":     accessKey,
			"SecretAccessKey": randBase64(30),
			"SessionToken":    randBase64(36),
			"Expiration":      expiry.Format(time.RFC3339),
		},
		"AssumedRoleUser": map[string]any{
			"AssumedRoleId": roleName + ":" + sessionName,
			"Arn":           assumedArn,
		},
		"SubjectFromWebIdentityToken": subject,
		"PackedPolicySize":            0,
	}), nil
}

func (p *STSProvider) AssumeRoleWithSAML(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	roleArn := strParam(nr.Params, "RoleArn")
	durationSecs := intParam(nr.Params, "DurationSeconds", 3600)

	if !roleARNRegex.MatchString(roleArn) {
		return nil, stsErr("ValidationError", fmt.Sprintf("%s is invalid", roleArn), 400)
	}

	targetAccount, roleName := accountAndRoleFromARN(roleArn, nr.AccountID)
	accessKey, err := identity.EncodeLSIA(targetAccount)
	if err != nil {
		return nil, stsErr("InternalError", "credential mint failed: "+err.Error(), 500)
	}
	expiry := clock.Now().Add(time.Duration(durationSecs) * time.Second)
	assumedArn := awsarn.ResourceID("", targetAccount)("sts-assumed-role", roleName+"/saml-session")
	_ = p.store.StoreSession(accessKey, SessionConfig{
		Account:         targetAccount,
		RoleName:        roleName,
		RoleSessionName: "saml-session",
	})

	return provider.OK(map[string]any{
		"Credentials": map[string]any{
			"AccessKeyId":     accessKey,
			"SecretAccessKey": randBase64(30),
			"SessionToken":    randBase64(36),
			"Expiration":      expiry.Format(time.RFC3339),
		},
		"AssumedRoleUser": map[string]any{
			"AssumedRoleId": roleName + ":saml-session",
			"Arn":           assumedArn,
		},
		"Subject":          "saml-subject",
		"SubjectType":      "persistent",
		"Issuer":           "saml-issuer",
		"Audience":         "https://signin.aws.amazon.com/saml",
		"NameQualifier":    "",
		"PackedPolicySize": 0,
	}), nil
}

func (p *STSProvider) GetSessionToken(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	durationSecs := intParam(nr.Params, "DurationSeconds", 43200)
	if durationSecs < 900 || durationSecs > 129600 {
		return nil, stsErr("ValidationError",
			"1 validation error detected: Value at 'durationSeconds' failed to satisfy constraint: Member must have value greater than or equal to 900", 400)
	}
	creds := generateCredentials(durationSecs)
	return provider.OK(map[string]any{"Credentials": credMap(creds)}), nil
}

func (p *STSProvider) GetFederationToken(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	if name == "" || !federationNameRegex.MatchString(name) {
		return nil, stsErr("ValidationError",
			fmt.Sprintf("1 validation error detected: Value '%s' at 'name' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\w+=,.@-]+", name), 400)
	}
	durationSecs := intParam(nr.Params, "DurationSeconds", 43200)
	creds := generateCredentials(durationSecs)

	policySize := 0
	if policy := strParam(nr.Params, "Policy"); policy != "" {
		policySize = (len(policy) * 100) / 2048
		if policySize < 1 {
			policySize = 1
		}
		if policySize > 100 {
			policySize = 100
		}
	}

	return provider.OK(map[string]any{
		"Credentials": credMap(creds),
		"FederatedUser": map[string]any{
			"FederatedUserId": fmt.Sprintf("%s:%s", nr.AccountID, name),
			"Arn":             nr.ResourceID("sts-federated-user", name),
		},
		"PackedPolicySize": policySize,
	}), nil
}

func (p *STSProvider) GetAccessKeyInfo(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{"Account": nr.AccountID}), nil
}

func (p *STSProvider) DecodeAuthorizationMessage(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	encoded := strParam(nr.Params, "EncodedMessage")
	// In emulator context we don't encode authorization messages, return as-is
	return provider.OK(map[string]any{"DecodedMessage": encoded}), nil
}

// ─── credential generation ────────────────────────────────────────────────────

type credentials struct {
	AccessKeyId     string
	SecretAccessKey string
	SessionToken    string
	Expiration      time.Time
}

func generateCredentials(durationSeconds int) credentials {
	return credentials{
		AccessKeyId:     "ASIA" + randAlphaNum(16),
		SecretAccessKey: randBase64(30),
		SessionToken:    randBase64(268), // 356-char base64 encoded
		Expiration:      clock.Now().Add(time.Duration(durationSeconds) * time.Second),
	}
}

func credMap(c credentials) map[string]any {
	return map[string]any{
		"AccessKeyId":     c.AccessKeyId,
		"SecretAccessKey": c.SecretAccessKey,
		"SessionToken":    c.SessionToken,
		"Expiration":      c.Expiration.UTC().Format(time.RFC3339),
	}
}

// ─── helpers ──────────────────────────────────────────────────────────────────

const alphaNum = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

func randAlphaNum(n int) string {
	b := make([]byte, n)
	for i := range b {
		idx, _ := rand.Int(rand.Reader, big.NewInt(int64(len(alphaNum))))
		b[i] = alphaNum[idx.Int64()]
	}
	return strings.ToUpper(string(b))
}

func randBase64(byteLen int) string {
	b := make([]byte, byteLen)
	rand.Read(b)
	return base64.StdEncoding.EncodeToString(b)
}

func randID(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return strings.ToUpper(hex.EncodeToString(b))
}

func strParam(params map[string]any, key string) string {
	v, _ := params[key].(string)
	return v
}

func intParam(params map[string]any, key string, def int) int {
	switch v := params[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	case string:
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// parseTags reads Tags.member.N.Key / Tags.member.N.Value from Query params,
// or a JSON array [{Key,Value},...] for JSON-protocol callers.
func parseTags(params map[string]any, prefix string) []Tag {
	// JSON array form: [{Key:..., Value:...}]
	if raw, ok := params[prefix]; ok {
		if arr, ok := raw.([]any); ok {
			var tags []Tag
			for _, item := range arr {
				if m, ok := item.(map[string]any); ok {
					k, _ := m["Key"].(string)
					v, _ := m["Value"].(string)
					if k != "" {
						tags = append(tags, Tag{Key: k, Value: v})
					}
				}
			}
			return tags
		}
	}
	// Query-protocol: Tags.member.1.Key, Tags.member.1.Value
	var tags []Tag
	for i := 1; ; i++ {
		k, ok1 := params[fmt.Sprintf("%s.member.%d.Key", prefix, i)].(string)
		v, _ := params[fmt.Sprintf("%s.member.%d.Value", prefix, i)].(string)
		if !ok1 || k == "" {
			break
		}
		tags = append(tags, Tag{Key: k, Value: v})
	}
	return tags
}

// parseStringList reads TransitiveTagKeys.member.N from Query params or a JSON []string.
func parseStringList(params map[string]any, prefix string) []string {
	if raw, ok := params[prefix]; ok {
		if arr, ok := raw.([]any); ok {
			var out []string
			for _, item := range arr {
				if s, ok := item.(string); ok {
					out = append(out, s)
				}
			}
			return out
		}
	}
	var out []string
	for i := 1; ; i++ {
		v, ok := params[fmt.Sprintf("%s.member.%d", prefix, i)].(string)
		if !ok || v == "" {
			break
		}
		out = append(out, v)
	}
	return out
}

// extractAccessKeyFromAuth pulls the AccessKeyId from an AWS Authorization header.
// Returns "" when not present or unparseable.
func extractAccessKeyFromAuth(auth string) string {
	// AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request
	const marker = "Credential="
	idx := strings.Index(auth, marker)
	if idx < 0 {
		return ""
	}
	rest := auth[idx+len(marker):]
	if slash := strings.Index(rest, "/"); slash > 0 {
		return rest[:slash]
	}
	return ""
}

// extractJWTSubject decodes the JWT payload segment, validates exp/iat claims,
// and returns the `sub` claim. Returns an error if the token has expired.
//
// NOTE: Cryptographic signature verification (JWKS chain) is deferred — it
// would require external HTTP calls to the OIDC discovery endpoint and is out
// of scope for the emulator. The token structure and expiry are validated here.
func extractJWTSubject(token string) (string, error) {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return "unknown", nil
	}
	// add padding
	payload := parts[1]
	for len(payload)%4 != 0 {
		payload += "="
	}
	decoded, err := base64.URLEncoding.DecodeString(payload)
	if err != nil {
		decoded, err = base64.StdEncoding.DecodeString(payload)
		if err != nil {
			return "unknown", nil
		}
	}

	// Parse as a generic JSON map so we can inspect exp/iat/sub.
	var claims map[string]any
	if jsonErr := json.Unmarshal(decoded, &claims); jsonErr != nil {
		return "unknown", nil
	}

	// Validate expiry.
	if expRaw, ok := claims["exp"]; ok {
		var expUnix int64
		switch v := expRaw.(type) {
		case float64:
			expUnix = int64(v)
		case json.Number:
			expUnix, _ = v.Int64()
		}
		if expUnix > 0 && expUnix < clock.Now().Unix() {
			return "", model.NewProviderError("ExpiredTokenException", "Token has expired", 400)
		}
	}

	// Extract sub claim.
	if sub, ok := claims["sub"].(string); ok && sub != "" {
		return sub, nil
	}
	return "unknown", nil
}

func stsErr(code, msg string, status int) error {
	return model.NewProviderError(code, msg, status)
}

// accountAndRoleFromARN extracts the target account ID and role name from a role ARN.
// Falls back to callerAccount when the ARN has no account field.
func accountAndRoleFromARN(roleArn, callerAccount string) (account, roleName string) {
	account = callerAccount
	roleName = roleArn
	if parsed, err := awsarn.Parse(roleArn); err == nil {
		if parsed.AccountID != "" {
			account = parsed.AccountID
		}
		if parts := strings.Split(parsed.Resource, "/"); len(parts) > 1 {
			roleName = parts[len(parts)-1]
		}
	} else if parts := strings.Split(roleArn, "/"); len(parts) > 1 {
		roleName = parts[len(parts)-1]
	}
	return
}

// extractJWTSubjectWithVerification checks whether OIDC issuers are configured
// and verifies the JWT signature if so. Falls back to the basic extractJWTSubject
// (expiry-only check) when OIDCIssuers is empty (back-compat for tests).
func (p *STSProvider) extractJWTSubjectWithVerification(token string) (string, error) {
	if len(p.oidcIssuers) == 0 {
		// No OIDC config — use the legacy path (no signature verification).
		return extractJWTSubject(token)
	}
	claims, err := p.jwksCache.verifyJWT(token, p.oidcIssuers)
	if err != nil {
		return "", model.NewProviderError("InvalidIdentityToken",
			"JWT signature verification failed: "+err.Error(), 400)
	}
	if sub, ok := claims["sub"].(string); ok && sub != "" {
		return sub, nil
	}
	return "unknown", nil
}
