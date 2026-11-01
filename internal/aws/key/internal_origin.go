package key

import (
	"context"
	"encoding/base64"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// These serve origin=internal callers (Secrets Manager and SSM encrypting what
// they store) with the behaviour of Encrypt2/Decrypt2/GenerateDataKey2: the same
// ciphertext format and key checks as before the registry became the single
// entry point, so values stored earlier still decrypt. The parameters mirror the
// wire actions (base64 strings), so a plugin that replaces them sees the shapes
// it knows.

func (p *KeyProvider) encryptInternal(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	keyID, _ := nr.Params["KeyId"].(string)
	pt, err := base64.StdEncoding.DecodeString(strParam(nr.Params, "Plaintext"))
	if err != nil {
		return nil, model.NewProviderError("ValidationException", "Plaintext must be base64-encoded", 400)
	}
	ct, err := p.Encrypt2(ctx, keyID, pt, extractEncCtx(nr.Params))
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"KeyId": keyID, "CiphertextBlob": base64.StdEncoding.EncodeToString(ct),
	}), nil
}

func (p *KeyProvider) decryptInternal(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ct, err := base64.StdEncoding.DecodeString(strParam(nr.Params, "CiphertextBlob"))
	if err != nil {
		return nil, model.NewProviderError("ValidationException", "CiphertextBlob must be base64-encoded", 400)
	}
	pt, err := p.Decrypt2(ctx, "", ct, extractEncCtx(nr.Params))
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"Plaintext": base64.StdEncoding.EncodeToString(pt)}), nil
}

func (p *KeyProvider) generateDataKeyInternal(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bits := 256
	if strParam(nr.Params, "KeySpec") == "AES_128" {
		bits = 128
	}
	pt, ct, err := p.GenerateDataKey2(ctx, strParam(nr.Params, "KeyId"), bits)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"Plaintext":      base64.StdEncoding.EncodeToString(pt),
		"CiphertextBlob": base64.StdEncoding.EncodeToString(ct),
	}), nil
}

func strParam(params map[string]any, key string) string {
	s, _ := params[key].(string)
	return s
}
