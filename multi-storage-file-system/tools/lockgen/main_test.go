package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

func TestGlobalsMutexSelector_shapes(t *testing.T) {
	cases := []struct {
		src string
		ok  bool
		op  string
	}{
		{`package p; func _() { globals.Lock() }`, true, "Lock"},
		{`package p; func _() { globals.Unlock() }`, true, "Unlock"},
		{`package p; func _() { (globals).Lock() }`, true, "Lock"},
		{`package p; func _() { (globals).Unlock() }`, true, "Unlock"},
		{`package p; func _() { (&globals).Lock() }`, true, "Lock"},
		{`package p; func _() { ( ( globals ) ).Unlock() }`, true, "Unlock"},
		{`package p; func _() { globals.TryLock() }`, false, ""},
		{`package p; func _() { other.Lock() }`, false, ""},
	}
	for _, tc := range cases {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, "t.go", tc.src, 0)
		if err != nil {
			t.Fatalf("parse %q: %v", tc.src, err)
		}
		fun := firstCallFun(t, f)
		_, op, ok := globalsMutexSelector(fun)
		if ok != tc.ok {
			t.Fatalf("%q: ok want %v got %v (op=%q)", tc.src, tc.ok, ok, op)
		}
		if tc.ok && op != tc.op {
			t.Fatalf("%q: op want %q got %q", tc.src, tc.op, op)
		}
	}
}

func firstCallFun(t *testing.T, f *ast.File) ast.Expr {
	t.Helper()
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Body == nil {
			continue
		}
		for _, st := range fd.Body.List {
			es, ok := st.(*ast.ExprStmt)
			if !ok {
				continue
			}
			call, ok := es.X.(*ast.CallExpr)
			if !ok {
				continue
			}
			return call.Fun
		}
	}
	t.Fatal("no call")
	return nil
}
