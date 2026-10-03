//go:build gcp_conformance

package gcpconformance

// ActionWords splits an emulator registry action into its CamelCase words
// (e.g. "ObjectsInsert" -> ["Objects","Insert"], "CryptoKeyVersionDisable" ->
// ["Crypto","Key","Version","Disable"]).
//
// It is a thin exported wrapper over the coverage resolver's splitter so
// callers outside this package — namely the fidelity-matrix generator — can
// classify mutating actions on word boundaries without duplicating the
// CamelCase logic. Word-boundary matching is what keeps "GetSettings" from
// being mistaken for a "Set" operation (the word there is "Settings").
func ActionWords(action string) []string { return splitCamel(action) }
