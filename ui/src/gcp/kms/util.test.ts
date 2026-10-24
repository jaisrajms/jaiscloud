import { describe, expect, it } from 'vitest'
import {
  base64Decode,
  base64Encode,
  base64ToBytes,
  bytesToBase64,
  digestAlgorithmFor,
  digestFieldFor,
  isAsymmetricAlgorithm,
  isAsymmetricDecryptAlgorithm,
  isMacAlgorithm,
  isSignAlgorithm,
  shaDigestBase64,
} from './util'

describe('base64Encode', () => {
  it('encodes ASCII', () => {
    expect(base64Encode('hello')).toBe('aGVsbG8=')
  })

  it('encodes multi-byte UTF-8 without Latin-1 mangling', () => {
    expect(base64Encode('héllo')).toBe('aMOpbGxv')
    expect(base64Encode('🐑')).toBe('8J+QkQ==')
  })

  it('encodes the empty string', () => {
    expect(base64Encode('')).toBe('')
  })
})

describe('base64Decode', () => {
  it('round-trips text', () => {
    expect(base64Decode(base64Encode('héllo 🐑'))).toBe('héllo 🐑')
  })
})

describe('digestAlgorithmFor', () => {
  it('reads the SHA suffix', () => {
    expect(digestAlgorithmFor('RSA_SIGN_PKCS1_2048_SHA256')).toBe('SHA-256')
    expect(digestAlgorithmFor('EC_SIGN_P384_SHA384')).toBe('SHA-384')
    expect(digestAlgorithmFor('RSA_SIGN_PSS_4096_SHA512')).toBe('SHA-512')
  })

  it('returns undefined for symmetric/MAC/unknown algorithms', () => {
    expect(digestAlgorithmFor('GOOGLE_SYMMETRIC_ENCRYPTION')).toBeUndefined()
    expect(digestAlgorithmFor('HMAC_SHA256')).toBeUndefined()
    expect(digestAlgorithmFor(undefined)).toBeUndefined()
  })
})

describe('digestFieldFor', () => {
  it('maps the algorithm to the Cloud KMS Digest field', () => {
    expect(digestFieldFor('SHA-256')).toBe('sha256')
    expect(digestFieldFor('SHA-384')).toBe('sha384')
    expect(digestFieldFor('SHA-512')).toBe('sha512')
  })
})

describe('shaDigestBase64', () => {
  it('hashes with WebCrypto', async () => {
    expect(await shaDigestBase64('hello', 'SHA-256')).toBe(
      'LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=',
    )
    expect(await shaDigestBase64('', 'SHA-256')).toBe(
      '47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=',
    )
    expect(await shaDigestBase64('hello', 'SHA-384')).toBe(
      'WeF0h3dEjGnea4ANejO7+5/xtGPkQ1TDVTvNucZm+pASWjx5+QOXvfX2oT3oKGhP',
    )
    expect(await shaDigestBase64('hello', 'SHA-512')).toBe(
      'm3HSJL1i83hdltRq0+o9czGb+8KJDKra4t/3JRlnPKcjI8PZm6XBHXx6zG4UuMXaDEZjR1wuXDre9G9zvN7AQw==',
    )
  })
})

describe('bytes/base64', () => {
  it('round-trips arbitrary bytes', () => {
    const bytes = new Uint8Array([0, 1, 127, 128, 255])
    expect(Array.from(base64ToBytes(bytesToBase64(bytes)))).toEqual([0, 1, 127, 128, 255])
  })
})

describe('algorithm classification', () => {
  it('classifies RSA/EC/HMAC algorithms', () => {
    expect(isAsymmetricAlgorithm('RSA_SIGN_PKCS1_2048_SHA256')).toBe(true)
    expect(isAsymmetricAlgorithm('EC_SIGN_P256_SHA256')).toBe(true)
    expect(isAsymmetricAlgorithm('EC_SIGN_ED25519')).toBe(true)
    expect(isAsymmetricAlgorithm('HMAC_SHA256')).toBe(false)
    expect(isSignAlgorithm('RSA_SIGN_PKCS1_2048_SHA256')).toBe(true)
    expect(isSignAlgorithm('EC_SIGN_P384_SHA384')).toBe(true)
    expect(isSignAlgorithm('RSA_SIGN_RAW_PKCS1_2048')).toBe(false)
    expect(isSignAlgorithm('EC_SIGN_ED25519')).toBe(false)
    expect(isSignAlgorithm('RSA_DECRYPT_OAEP_2048_SHA256')).toBe(false)
    expect(isAsymmetricDecryptAlgorithm('RSA_DECRYPT_OAEP_2048_SHA256')).toBe(true)
    expect(isMacAlgorithm('HMAC_SHA256')).toBe(true)
    expect(isMacAlgorithm('GOOGLE_SYMMETRIC_ENCRYPTION')).toBe(false)
  })
})
