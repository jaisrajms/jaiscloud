// paginationcheck is a static-analysis CLI that inspects all List*/Describe*
// methods on *Provider receivers under a given directory tree and reports any
// that appear to lack pagination support.
//
// Usage:
//
//	go run tools/lint/paginationcheck/main.go [dir]          # dir defaults to internal/aws/provider
//	go run tools/lint/paginationcheck/main.go ./internal/aws/provider/...
//
// Exit code 1 when violations are found, 0 otherwise.
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// paginationKeywords are substrings whose presence in a method body's raw
// source text indicates the implementation handles pagination in some form.
// AWS idioms: Paginate (helper), Marker (S3/DynamoDB cursor), NextToken,
// IsTruncated, pagination. (package). GCP idioms: paging. (the shared
// internal/gcp/paging helper) and the pageToken/nextPageToken cursor fields.
var paginationKeywords = []string{
	"Paginate",
	"Marker",
	"NextToken",
	"IsTruncated",
	"pagination.",
	"paging.",
	"nextPageToken",
	"pageToken",
}

func main() {
	dir := "internal/aws/provider"
	if len(os.Args) > 1 {
		dir = os.Args[1]
	}
	// Strip trailing /... used by Make targets — we always walk recursively.
	dir = strings.TrimSuffix(dir, "/...")
	dir = strings.TrimSuffix(dir, "...")

	violations, err := check(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "paginationcheck: %v\n", err)
		os.Exit(2)
	}

	for _, v := range violations {
		fmt.Println(v)
	}

	if len(violations) > 0 {
		os.Exit(1)
	}
}

// check walks root recursively, parses every non-test .go file in the tree
// together (so cross-file helper delegation is visible), and returns one
// diagnostic string per List/Describe method that appears to be missing
// pagination.
func check(root string) ([]string, error) {
	fset := token.NewFileSet()

	var goFiles []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}
		goFiles = append(goFiles, path)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walking %q: %w", root, err)
	}

	// Aggregate all *Provider methods across the tree: method name → body
	// source + receiver variable name. A List/Describe method in one file may
	// delegate pagination to a helper in another file.
	providerMethods := map[string]string{}
	recvNames := map[string]string{}
	type listMethod struct{ fd *ast.FuncDecl }
	var listMethods []listMethod

	for _, path := range goFiles {
		src, err := os.ReadFile(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "paginationcheck: read error in %s: %v\n", path, err)
			continue
		}
		f, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			fmt.Fprintf(os.Stderr, "paginationcheck: parse error in %s: %v\n", path, err)
			continue
		}
		ast.Inspect(f, func(n ast.Node) bool {
			fd, ok := n.(*ast.FuncDecl)
			if !ok || fd.Recv == nil || len(fd.Recv.List) == 0 {
				return true
			}
			if !strings.HasSuffix(extractTypeName(fd.Recv.List[0].Type), "Provider") {
				return true
			}
			providerMethods[fd.Name.Name] = bodySource(fset, src, fd.Body)
			if len(fd.Recv.List[0].Names) > 0 {
				recvNames[fd.Name.Name] = fd.Recv.List[0].Names[0].Name
			}
			if (strings.HasPrefix(fd.Name.Name, "List") || strings.HasPrefix(fd.Name.Name, "Describe")) &&
				fd.Body != nil && len(fd.Body.List) > 0 {
				listMethods = append(listMethods, listMethod{fd: fd})
			}
			return true
		})
	}

	var violations []string
	for _, lm := range listMethods {
		if hasPagination(providerMethods, recvNames[lm.fd.Name.Name], providerMethods[lm.fd.Name.Name], map[string]bool{}) {
			continue
		}
		pos := fset.Position(lm.fd.Pos())
		violations = append(violations, fmt.Sprintf(
			"%s:%d: %s may be missing pagination",
			pos.Filename, pos.Line, lm.fd.Name.Name,
		))
	}
	return violations, nil
}

// hasPagination reports whether a method body contains a pagination keyword,
// or delegates to a same-receiver helper method that does (transitively, with a
// visited set to avoid cycles).
func hasPagination(methods map[string]string, recv, body string, visited map[string]bool) bool {
	for _, kw := range paginationKeywords {
		if strings.Contains(body, kw) {
			return true
		}
	}
	if recv == "" {
		return false
	}
	// Follow same-receiver helper calls: recv.methodName(
	re := regexp.MustCompile(`\b` + regexp.QuoteMeta(recv) + `\.([a-zA-Z_][a-zA-Z0-9_]*)\s*\(`)
	for _, m := range re.FindAllStringSubmatch(body, -1) {
		name := m[1]
		if visited[name] {
			continue
		}
		helperBody, ok := methods[name]
		if !ok {
			continue
		}
		visited[name] = true
		if hasPagination(methods, recv, helperBody, visited) {
			return true
		}
	}
	return false
}

// bodySource returns the raw source text of a block statement.
func bodySource(fset *token.FileSet, src []byte, body *ast.BlockStmt) string {
	if body == nil {
		return ""
	}
	bodyStart := fset.Position(body.Pos()).Offset
	bodyEnd := fset.Position(body.End()).Offset
	if bodyEnd > len(src) {
		bodyEnd = len(src)
	}
	return string(src[bodyStart:bodyEnd])
}

// extractTypeName returns the base identifier from a receiver type expression.
// Handles *T, T, and generic T[P] (returns "").
func extractTypeName(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.StarExpr:
		return extractTypeName(t.X)
	case *ast.Ident:
		return t.Name
	case *ast.IndexExpr:
		return extractTypeName(t.X)
	default:
		return ""
	}
}
