package sparkhelpers

import (
	"sort"
	"strings"
)

// EntryPointArgs returns (preJarArgs []string, jarOrScript string) for spark-submit.
//
//   - JAR:    preJarArgs=["--class", MainClass] (empty if no MainClass) then
//     ["--jars", joined(JarFileURIs)] if any, jarOrScript=JarURI
//   - Python: preJarArgs=["--py-files", joined(PyFiles)] if PyFiles non-empty
//     then ["--jars", joined(JarFileURIs)] if any, jarOrScript=MainPythonFile
//   - R:      preJarArgs=nil, jarOrScript=MainRFile
//   - SQL:    preJarArgs=["--jars", …]["--hivevar", …] then "-e"/"-f", jarOrScript=""
//     (spark-sql is a spark-submit wrapper; the SQL flags are passed through to
//     the SQL CLI, so no primary resource is set)
func EntryPointArgs(ep EntryPoint) (preJarArgs []string, jarOrScript string) {
	switch e := ep.(type) {
	case JarEntryPoint:
		if e.MainClass != "" {
			preJarArgs = []string{"--class", e.MainClass}
		}
		if len(e.JarFileURIs) > 0 {
			preJarArgs = append(preJarArgs, "--jars", strings.Join(e.JarFileURIs, ","))
		}
		jarOrScript = e.JarURI
	case PythonEntryPoint:
		if len(e.PyFiles) > 0 {
			preJarArgs = append(preJarArgs, "--py-files", strings.Join(e.PyFiles, ","))
		}
		if len(e.JarFileURIs) > 0 {
			preJarArgs = append(preJarArgs, "--jars", strings.Join(e.JarFileURIs, ","))
		}
		jarOrScript = e.MainPythonFile
	case REntryPoint:
		jarOrScript = e.MainRFile
	case SqlEntryPoint:
		if len(e.JarFileURIs) > 0 {
			preJarArgs = append(preJarArgs, "--jars", strings.Join(e.JarFileURIs, ","))
		}
		// Sort the hivevar keys so the argv is deterministic regardless of map
		// iteration order.
		keys := make([]string, 0, len(e.HiveVars))
		for k := range e.HiveVars {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			preJarArgs = append(preJarArgs, "--hivevar", k+"="+e.HiveVars[k])
		}
		switch {
		case e.FileURI != "":
			preJarArgs = append(preJarArgs, "-f", e.FileURI)
		case len(e.Queries) > 0:
			preJarArgs = append(preJarArgs, "-e", strings.Join(e.Queries, ";\n"))
		}
	}
	return
}
