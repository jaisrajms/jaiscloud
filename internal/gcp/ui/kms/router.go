package kmsui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the Cloud KMS UI API. Key rings and
// crypto keys are addressed under their location, matching the wire API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	// Key rings.
	r.Get("/locations/{location}/keyRings", h.ListKeyRings)
	r.Post("/locations/{location}/keyRings", h.CreateKeyRing)
	r.Get("/locations/{location}/keyRings/{keyRing}", h.GetKeyRing)
	r.Get("/locations/{location}/keyRings/{keyRing}/iam", h.GetKeyRingIam)
	r.Put("/locations/{location}/keyRings/{keyRing}/iam", h.SetKeyRingIam)
	r.Post("/locations/{location}/keyRings/{keyRing}/iam/testIamPermissions", h.TestKeyRingIam)

	// Crypto keys.
	r.Get("/locations/{location}/keyRings/{keyRing}/cryptoKeys", h.ListCryptoKeys)
	r.Post("/locations/{location}/keyRings/{keyRing}/cryptoKeys", h.CreateCryptoKey)
	r.Get("/locations/{location}/keyRings/{keyRing}/cryptoKeys/{key}", h.GetCryptoKey)
	r.Get("/locations/{location}/keyRings/{keyRing}/cryptoKeys/{key}/iam", h.GetCryptoKeyIam)
	r.Put("/locations/{location}/keyRings/{keyRing}/cryptoKeys/{key}/iam", h.SetCryptoKeyIam)
	r.Post("/locations/{location}/keyRings/{keyRing}/cryptoKeys/{key}/iam/testIamPermissions", h.TestCryptoKeyIam)
	r.Post("/locations/{location}/keyRings/{keyRing}/cryptoKeys/{key}/setPrimary", h.SetPrimaryVersion)

	// Versions.
	r.Get("/locations/{location}/keyRings/{keyRing}/cryptoKeys/{key}/versions", h.ListVersions)
	r.Post("/locations/{location}/keyRings/{keyRing}/cryptoKeys/{key}/versions", h.CreateVersion)
	r.Get("/locations/{location}/keyRings/{keyRing}/cryptoKeys/{key}/versions/{version}", h.GetVersion)
	r.Post("/locations/{location}/keyRings/{keyRing}/cryptoKeys/{key}/versions/{version}/destroy", h.DestroyVersion)
	r.Post("/locations/{location}/keyRings/{keyRing}/cryptoKeys/{key}/versions/{version}/disable", h.DisableVersion)
	r.Post("/locations/{location}/keyRings/{keyRing}/cryptoKeys/{key}/versions/{version}/enable", h.EnableVersion)

	// Crypto operations.
	r.Post("/locations/{location}/keyRings/{keyRing}/cryptoKeys/{key}/encrypt", h.Encrypt)
	r.Post("/locations/{location}/keyRings/{keyRing}/cryptoKeys/{key}/decrypt", h.Decrypt)
	r.Post("/locations/{location}/keyRings/{keyRing}/cryptoKeys/{key}/versions/{version}/asymmetricSign", h.AsymmetricSign)
	r.Post("/locations/{location}/keyRings/{keyRing}/cryptoKeys/{key}/versions/{version}/asymmetricDecrypt", h.AsymmetricDecrypt)
	r.Post("/locations/{location}/keyRings/{keyRing}/cryptoKeys/{key}/versions/{version}/macSign", h.MacSign)
	r.Post("/locations/{location}/keyRings/{keyRing}/cryptoKeys/{key}/versions/{version}/macVerify", h.MacVerify)
	r.Get("/locations/{location}/keyRings/{keyRing}/cryptoKeys/{key}/versions/{version}/publicKey", h.GetPublicKey)

	return r
}
