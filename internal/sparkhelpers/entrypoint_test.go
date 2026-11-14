package sparkhelpers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEntryPointArgs_JarFileURIs(t *testing.T) {
	pre, jar := EntryPointArgs(JarEntryPoint{
		JarURI:      "gs://b/a.jar",
		MainClass:   "Main",
		JarFileURIs: []string{"gs://b/x.jar", "gs://b/y.jar"},
	})
	require.Equal(t, []string{"--class", "Main", "--jars", "gs://b/x.jar,gs://b/y.jar"}, pre)
	require.Equal(t, "gs://b/a.jar", jar)

	// No jarFileUris → unchanged argv (existing behavior preserved).
	pre, _ = EntryPointArgs(JarEntryPoint{JarURI: "gs://b/a.jar", MainClass: "Main"})
	require.Equal(t, []string{"--class", "Main"}, pre)
}

// TestEntryPointArgs_JarClassOnlyPromotesFirstLibrary covers a Dataproc
// class-only job (mainClass + jarFileUris, no mainJarFileUri): the first library
// jar becomes the primary resource so spark-submit does not swallow the first
// submitted arg as its application resource, and the remaining libraries stay
// --jars.
func TestEntryPointArgs_JarClassOnlyPromotesFirstLibrary(t *testing.T) {
	// Class + two libraries: L1 is the primary resource, L2 stays --jars.
	pre, jar := EntryPointArgs(JarEntryPoint{
		MainClass:   "C",
		JarFileURIs: []string{"L1", "L2"},
	})
	require.Equal(t, []string{"--class", "C", "--jars", "L2"}, pre)
	require.Equal(t, "L1", jar)

	// Class + one library: nothing remains, so --jars is omitted entirely.
	pre, jar = EntryPointArgs(JarEntryPoint{
		MainClass:   "C",
		JarFileURIs: []string{"L1"},
	})
	require.Equal(t, []string{"--class", "C"}, pre)
	require.Equal(t, "L1", jar)

	// No class: the promoted primary resource is still set, the args after it
	// are the program's.
	pre, jar = EntryPointArgs(JarEntryPoint{JarFileURIs: []string{"L1", "L2", "L3"}})
	require.Equal(t, []string{"--jars", "L2,L3"}, pre)
	require.Equal(t, "L1", jar)

	// No libraries (DPS2): unchanged — an empty primary resource, exactly as
	// before this fix.
	pre, jar = EntryPointArgs(JarEntryPoint{MainClass: "C"})
	require.Equal(t, []string{"--class", "C"}, pre)
	require.Empty(t, jar)
}

func TestEntryPointArgs_PythonJarFileURIs(t *testing.T) {
	pre, script := EntryPointArgs(PythonEntryPoint{
		MainPythonFile: "gs://b/main.py",
		PyFiles:        []string{"gs://b/dep.py"},
		JarFileURIs:    []string{"gs://b/conn.jar"},
	})
	require.Equal(t, []string{"--py-files", "gs://b/dep.py", "--jars", "gs://b/conn.jar"}, pre)
	require.Equal(t, "gs://b/main.py", script)
}
