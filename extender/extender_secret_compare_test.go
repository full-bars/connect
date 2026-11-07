package extender

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/urnetwork/connect/protocol"
)

func TestAllowedSecretComparesInConstantTime(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "extender.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	calls := map[string]bool{}
	found := false
	for _, decl := range file.Decls {
		method, ok := decl.(*ast.FuncDecl)
		if !ok || method.Recv == nil || method.Name.Name != "IsAllowedSecret" {
			continue
		}
		found = true
		ast.Inspect(method.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if ident, ok := selector.X.(*ast.Ident); ok {
				calls[ident.Name+"."+selector.Sel.Name] = true
			}
			return true
		})
	}
	if !found || !calls["hmac.Equal"] {
		t.Fatal("IsAllowedSecret must compare signatures with hmac.Equal")
	}
	for _, name := range []string{"bytes.Equal", "slices.Equal", "reflect.DeepEqual"} {
		if calls[name] {
			t.Errorf("early-exit comparison %s", name)
		}
	}
}

func TestAllowedSecretRefusesPartialSignatures(t *testing.T) {
	server := &ExtenderServer{allowedSecrets: []string{testSecret}}
	header := &protocol.ExtenderHeader{Timestamp: 1, Nonce: []byte("synthetic-nonce-0001")}
	mac := hmac.New(sha256.New, []byte(testSecret))
	timestamp := make([]byte, 8)
	binary.BigEndian.PutUint64(timestamp, header.Timestamp)
	mac.Write(timestamp)
	mac.Write(header.Nonce)
	signature := mac.Sum(nil)
	header.Signature = signature
	if !server.IsAllowedSecret(header) {
		t.Fatal("valid signature refused")
	}
	flipped := bytes.Clone(signature)
	flipped[len(flipped)-1] ^= 1
	for _, invalid := range [][]byte{signature[:len(signature)-1], append(bytes.Clone(signature), 0), nil, flipped} {
		header.Signature = invalid
		if server.IsAllowedSecret(header) {
			t.Fatal("invalid signature admitted")
		}
	}
}
