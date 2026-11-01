//go:build gcp_conformance

package gcpconformance

import "encoding/base64"

// iamUploadCertificatePEM is a fixed, self-signed X.509 v3 certificate wrapping
// a 2048-bit RSA public key. projects.serviceAccounts.keys.upload documents
// that publicKeyData "Must be an RSA public key that is wrapped in an X.509 v3
// certificate. Include the first line, -----BEGIN CERTIFICATE-----, and the
// last line, -----END CERTIFICATE-----", so the curated chain and the
// synthesized Discovery probe upload the documented form. There is no private
// key: only the public half is needed, and the emulator stores only public
// material for an uploaded key.
const iamUploadCertificatePEM = `-----BEGIN CERTIFICATE-----
MIIDFTCCAf2gAwIBAgIBATANBgkqhkiG9w0BAQsFADArMSkwJwYDVQQDEyBqYWlz
Y2xvdWQtY29uZm9ybWFuY2Uta2V5LXVwbG9hZDAgFw0yMDAxMDEwMDAwMDBaGA8y
MTIwMDEwMTAwMDAwMFowKzEpMCcGA1UEAxMgamFpc2Nsb3VkLWNvbmZvcm1hbmNl
LWtleS11cGxvYWQwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQClqc7R
L4yAdypO24B8K1AjxW3AU0C96iLbkNYkwDNP8Nq7TfZ7cwCJVUw/C3isF/PhsqBh
dgm2NgIJ1+Icb0TXxrlsAJgxx5sDXvx36Z3Sl/je+egXzu8hdUwE6bOf8yHCaJGj
yneXZ7lTvhC8kLUpmkePfiilyWghAvptexgvppOt/ld5a55E80o8s3WZZsur+LQ4
KFvehxYObTs6tb/LAkfFe48GHDYcA2L5mIeaCSCvYY26cTB6kzfyLoBIXCC4oWeC
Sh/v2AXk9LPyLic9/wE7ltKRdK+CKK9jzYT+fsGlHdfONG5vM67tvWruBs2ZYsvR
4fo536rPraImGEGxAgMBAAGjQjBAMA4GA1UdDwEB/wQEAwIChDAPBgNVHRMBAf8E
BTADAQH/MB0GA1UdDgQWBBQ3+27KIh9rgbDOZffLTPhWeXhgqjANBgkqhkiG9w0B
AQsFAAOCAQEAHDpamC0IlBzn0d2ITtH/pviVhhH71DWfOb8sIBs0wN8kkFff8a0C
f+4bkXoi2Jf3Et/ngYD7389avalCFJhOZD5CL2dvDM8RF+IWZTaJisVb3MnOlzsf
40/N7kufVCqScyYJdL7HO3qSd5azqr39OQpIV2VPSGMPaRSUIJrRogsBHsi7BXMk
eCdwcK8rTg/KUVDGkDB4wXEDEgTCDRbfGTNVivc7zYijqTARbNpTX1CfX+XlODcX
DRC9EnvtrVClzg6SkWbSolyvkxha/I757RxqKIVRs24c1fGSlD3d09h9Bd6TAwd5
dUfEHG36iMciWiI5EFI3/Yj0jeH8z6B8ow==
-----END CERTIFICATE-----
`

// iamUploadPublicKeyData is the base64 publicKeyData the upload scenarios send:
// the certificate bytes above, as the Discovery `format: byte` field requires.
func iamUploadPublicKeyData() string {
	return base64.StdEncoding.EncodeToString([]byte(iamUploadCertificatePEM))
}
