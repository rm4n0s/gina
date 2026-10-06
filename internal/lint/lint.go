// Package lint enforces the "no goroutines, no channels" rule (SPEC §2) with a
// purely syntactic check over Go source files.
package lint

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type Finding struct {
	Pos token.Position
	Msg string
}

func (f Finding) String() string { return f.Pos.String() + ": " + f.Msg }

var deniedImports = []string{"os/signal", "context", "net/http", "net/rpc", "os/exec", "golang.org/x/sync"}

var deniedTimeFuncs = map[string]bool{"After": true, "AfterFunc": true, "Tick": true, "NewTimer": true, "NewTicker": true}

// CheckSource lints one source file (src may be a string, []byte or nil to read the file).
func CheckSource(filename string, src any) ([]Finding, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, src, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	var out []Finding
	add := func(p token.Pos, msg string) { out = append(out, Finding{fset.Position(p), msg}) }

	for _, imp := range f.Imports {
		path, _ := strconv.Unquote(imp.Path.Value)
		for _, d := range deniedImports {
			if path == d || strings.HasPrefix(path, d+"/") {
				add(imp.Pos(), "import of "+path+" is not allowed (starts goroutines or needs channels)")
			}
		}
	}
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.GoStmt:
			add(x.Pos(), "go statement")
		case *ast.ChanType:
			add(x.Pos(), "channel type")
		case *ast.SendStmt:
			add(x.Pos(), "channel send")
		case *ast.SelectStmt:
			add(x.Pos(), "select statement")
		case *ast.UnaryExpr:
			if x.Op == token.ARROW {
				add(x.Pos(), "channel receive")
			}
		case *ast.CallExpr:
			if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "time" && deniedTimeFuncs[sel.Sel.Name] {
					add(x.Pos(), "time."+sel.Sel.Name+" uses channels or goroutines")
				}
				if sel.Sel.Name == "Parallel" && len(x.Args) == 0 {
					add(x.Pos(), "t.Parallel is not allowed")
				}
			}
		case *ast.SelectorExpr:
			if id, ok := x.X.(*ast.Ident); ok && id.Name == "sync" && x.Sel.Name == "WaitGroup" {
				add(x.Pos(), "sync.WaitGroup is not allowed")
			}
		}
		return true
	})
	return out, nil
}

// CheckDir lints every .go file under root, skipping testdata, vendor and dot dirs.
func CheckDir(root string) ([]Finding, error) {
	var out []Finding
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path != root && (name == "testdata" || name == "vendor" || strings.HasPrefix(name, ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".go") {
			fs, err := CheckSource(path, nil)
			if err != nil {
				return err
			}
			out = append(out, fs...)
		}
		return nil
	})
	sort.SliceStable(out, func(i, j int) bool { return out[i].Pos.String() < out[j].Pos.String() })
	return out, err
}
