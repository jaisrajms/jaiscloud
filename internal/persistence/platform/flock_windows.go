//go:build windows

package platform

// AcquireDataDirLock is a no-op on Windows — exclusive file locking is not enforced.
func AcquireDataDirLock(dataDir string) (release func() error, err error) {
	return func() error { return nil }, nil
}
