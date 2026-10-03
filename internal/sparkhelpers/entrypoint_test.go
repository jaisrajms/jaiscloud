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

func TestEntryPointArgs_PythonJarFileURIs(t *testing.T) {
	pre, script := EntryPointArgs(PythonEntryPoint{
		MainPythonFile: "gs://b/main.py",
		PyFiles:        []string{"gs://b/dep.py"},
		JarFileURIs:    []string{"gs://b/conn.jar"},
	})
	require.Equal(t, []string{"--py-files", "gs://b/dep.py", "--jars", "gs://b/conn.jar"}, pre)
	require.Equal(t, "gs://b/main.py", script)
}
