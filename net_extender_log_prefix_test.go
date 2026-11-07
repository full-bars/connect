package connect

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestExtenderLogLiteralsCarryBridge(t *testing.T) {
	files, err := filepath.Glob("net_extender*.go")
	if err != nil {
		t.Fatal(err)
	}
	serverFiles, err := filepath.Glob("extender/*.go")
	if err != nil {
		t.Fatal(err)
	}
	files = append(files, serverFiles...)
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || len(call.Args) == 0 {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || (selector.Sel.Name != "Infof" && selector.Sel.Name != "Errorf" && selector.Sel.Name != "Printf") {
				return true
			}
			literal, ok := call.Args[0].(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			text, err := strconv.Unquote(literal.Value)
			if err == nil && strings.HasPrefix(text, "[extender]") {
				t.Errorf("%s: extender log lacks bridge", fset.Position(literal.Pos()))
			}
			return true
		})
	}
}
