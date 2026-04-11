// Lockgen rewrites globals.Lock() / globals.Unlock() to globalsLock("site") / globalsUnlock(),
// and refreshes existing globalsLock("…") site strings.
// The file globals_lock.go is never modified (embedded sync.Mutex calls used by globalsLock).
// Run from the package directory:
//
//	go generate ./...
//
// Or: go run ./tools/lockgen -dir .
package main

import (
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

func main() {
	dir := flag.String("dir", ".", "directory containing *.go files (package root)")
	dry := flag.Bool("dry-run", false, "print planned edits only, do not write files")
	flag.Parse()

	if err := run(*dir, *dry); err != nil {
		fmt.Fprintf(os.Stderr, "lockgen: %v\n", err)
		os.Exit(1)
	}
}

// Matches the const line anywhere in globals_lock.go (must appear after the import block — valid Go).
var globalsLockSiteCountConst = regexp.MustCompile(`const\s+globalsLockSiteCount\s*=\s*\d+`)

var globalsLockMaxSiteKeyLenConst = regexp.MustCompile(`const\s+globalsLockMaxSiteKeyLen\s*=\s*\d+`)

// Prefilled globalsLockMaxHoldBySite map (between markers; replaced entirely by lockgen).
var lockgenGlobalsLockMaxHoldBySiteBlock = regexp.MustCompile(`(?s)// lockgen-begin: globalsLockMaxHoldBySite\n.*?// lockgen-end: globalsLockMaxHoldBySite`)

func run(dir string, dry bool) error {
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			// Skip vendor and hidden dirs (.git, etc.). Root "." must not match HasPrefix(".", ".").
			if name == "vendor" || name == "tools" || (name != "." && strings.HasPrefix(name, ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		// globals_lock.go must keep globals.Lock/TryLock/Unlock for the real mutex implementation.
		if filepath.Base(path) == "globals_lock.go" {
			return nil
		}
		changed, newSrc, err := processFile(path)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		if !changed {
			return nil
		}
		if dry {
			fmt.Printf("=== %s (would write %d bytes)\n", path, len(newSrc))
			return nil
		}
		fi, statErr := os.Stat(path)
		perm := os.FileMode(0o644)
		if statErr == nil {
			perm = fi.Mode() & 0o777
		}
		return os.WriteFile(path, newSrc, perm)
	})
	if err != nil {
		return err
	}

	sites, err := uniqueGlobalsLockSites(dir)
	if err != nil {
		return err
	}
	if dry {
		maxKeyLen := maxGlobalsLockSiteKeyLen(sites)
		fmt.Printf("=== globalsLockSiteCount = %d (unique globalsLock(\"…\") sites; would write globals_lock.go)\n", len(sites))
		fmt.Printf("=== globalsLockMaxSiteKeyLen = %d (max len(site) in bytes; would write globals_lock.go)\n", maxKeyLen)
		return nil
	}
	return writeGlobalsLockGenerated(dir, sites)
}

// uniqueGlobalsLockSites parses all package .go files and returns sorted distinct string literals
// passed to globalsLock (after lockgen rewrites call sites in the first walk).
func uniqueGlobalsLockSites(dir string) ([]string, error) {
	unique := make(map[string]struct{})
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if name == "vendor" || name == "tools" || (name != "." && strings.HasPrefix(name, ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		if filepath.Base(path) == "globals_lock.go" {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(n ast.Node) bool {
			ce, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := ce.Fun.(*ast.Ident)
			if !ok || id.Name != "globalsLock" || len(ce.Args) < 1 {
				return true
			}
			lit, ok := ce.Args[0].(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			s, err := strconv.Unquote(lit.Value)
			if err != nil {
				return true
			}
			unique[s] = struct{}{}
			return true
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(unique))
	for s := range unique {
		out = append(out, s)
	}
	sort.Strings(out)
	return out, nil
}

func maxGlobalsLockSiteKeyLen(sites []string) int {
	maxLen := 0
	for _, s := range sites {
		if n := len(s); n > maxLen {
			maxLen = n
		}
	}
	return maxLen
}

func buildGlobalsLockMaxHoldMapDecl(sites []string) string {
	var b strings.Builder
	b.WriteString("// lockgen-begin: globalsLockMaxHoldBySite\n")
	b.WriteString("var globalsLockMaxHoldBySite = map[string]globalsLockSiteStats{\n")
	for _, s := range sites {
		b.WriteString("\t")
		b.WriteString(strconv.Quote(s))
		b.WriteString(": {HoldCnt: 0, HoldSum: 0, HoldMax: 0},\n")
	}
	b.WriteString("}\n")
	b.WriteString("// lockgen-end: globalsLockMaxHoldBySite")
	return b.String()
}

func writeGlobalsLockGenerated(dir string, sites []string) error {
	p := filepath.Join(dir, "globals_lock.go")
	src, err := os.ReadFile(p)
	if err != nil {
		return fmt.Errorf("globals_lock.go: %w", err)
	}
	n := len(sites)
	maxKeyLen := maxGlobalsLockSiteKeyLen(sites)
	countLine := fmt.Sprintf("const globalsLockSiteCount = %d", n)
	maxKeyLine := fmt.Sprintf("const globalsLockMaxSiteKeyLen = %d", maxKeyLen)
	srcStr := string(src)
	if !globalsLockSiteCountConst.MatchString(srcStr) {
		return errors.New("globals_lock.go: no match for 'const globalsLockSiteCount = <int>'; add placeholder const for lockgen")
	}
	if !globalsLockMaxSiteKeyLenConst.MatchString(srcStr) {
		return errors.New("globals_lock.go: no match for 'const globalsLockMaxSiteKeyLen = <int>'; add placeholder const for lockgen")
	}
	out := globalsLockSiteCountConst.ReplaceAllString(srcStr, countLine)
	out = globalsLockMaxSiteKeyLenConst.ReplaceAllString(out, maxKeyLine)
	if !lockgenGlobalsLockMaxHoldBySiteBlock.MatchString(out) {
		return errors.New("globals_lock.go: no match for lockgen markers // lockgen-begin: globalsLockMaxHoldBySite ... // lockgen-end: globalsLockMaxHoldBySite")
	}
	out = lockgenGlobalsLockMaxHoldBySiteBlock.ReplaceAllString(out, buildGlobalsLockMaxHoldMapDecl(sites))
	formatted, err := format.Source([]byte(out))
	if err != nil {
		return fmt.Errorf("globals_lock.go: format: %w", err)
	}
	fi, statErr := os.Stat(p)
	perm := os.FileMode(0o644)
	if statErr == nil {
		perm = fi.Mode() & 0o777
	}
	return os.WriteFile(p, formatted, perm)
}

func processFile(path string) (changed bool, out []byte, err error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return false, nil, err
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	if err != nil {
		return false, nil, err
	}
	parent := buildParentMap(file)

	var locks []*ast.CallExpr
	ast.Inspect(file, func(n ast.Node) bool {
		ce, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if _, op, ok := globalsMutexSelector(ce.Fun); ok {
			switch op {
			case "Lock", "Unlock":
				locks = append(locks, ce)
			}
			return true
		}
		// Already migrated: refresh site string on each go generate.
		if id, ok := ce.Fun.(*ast.Ident); ok && id.Name == "globalsLock" {
			if len(ce.Args) < 1 {
				return true
			}
			if lit, ok := ce.Args[0].(*ast.BasicLit); ok && lit.Kind == token.STRING {
				locks = append(locks, ce)
			}
		}
		return true
	})

	edits := 0
	for _, ce := range locks {
		switch fun := ce.Fun.(type) {
		case *ast.Ident:
			if fun.Name != "globalsLock" {
				continue
			}
			site := lockSiteLabel(fset, path, parent, ce)
			old := ce.Args[0].(*ast.BasicLit)
			ce.Args[0] = &ast.BasicLit{
				ValuePos: old.ValuePos,
				Kind:     token.STRING,
				Value:    strconv.Quote(site),
			}
			edits++
		case *ast.SelectorExpr:
			sel := fun
			switch sel.Sel.Name {
			case "Lock":
				site := lockSiteLabel(fset, path, parent, ce)
				ce.Fun = &ast.Ident{NamePos: sel.Sel.NamePos, Name: "globalsLock"}
				ce.Args = []ast.Expr{&ast.BasicLit{
					ValuePos: sel.Sel.Pos(),
					Kind:     token.STRING,
					Value:    strconv.Quote(site),
				}}
				edits++
			case "Unlock":
				ce.Fun = &ast.Ident{NamePos: sel.Sel.NamePos, Name: "globalsUnlock"}
				ce.Args = nil
				edits++
			}
		}
	}

	if edits == 0 {
		return false, nil, nil
	}
	var buf strings.Builder
	if err := format.Node(&buf, fset, file); err != nil {
		return false, nil, err
	}
	return true, []byte(buf.String()), nil
}

// buildParentMap records the immediate AST parent of each node (stdlib pattern using ast.Inspect).
func buildParentMap(file *ast.File) map[ast.Node]ast.Node {
	parent := make(map[ast.Node]ast.Node)
	var stack []ast.Node
	ast.Inspect(file, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		if len(stack) > 0 {
			parent[n] = stack[len(stack)-1]
		}
		stack = append(stack, n)
		return true
	})
	return parent
}

// lockSiteLabel builds a stable site id for each globalsLock / globals.Lock call.
//
// Format: "<file>:<line>:<col>:<scope>"
//   - line:col are from the call token (unique per call site; same for named funcs and closures).
//   - scope is a human hint: func/method name, "funcLit@<line>", or "line<N>" at file scope.
//
// Leading with line:col keeps behavior consistent whether the lock sits in a named func or a func literal.
func lockSiteLabel(fset *token.FileSet, filePath string, parent map[ast.Node]ast.Node, call *ast.CallExpr) string {
	base := filepath.Base(filePath)
	callPos := fset.Position(call.Pos())

	scope := ""
	for cur := ast.Node(call); cur != nil; cur = parent[cur] {
		switch n := cur.(type) {
		case *ast.FuncDecl:
			scope = funcDeclName(n)
			goto haveScope
		case *ast.FuncLit:
			scope = fmt.Sprintf("funcLit@%d", fset.Position(n.Pos()).Line)
			goto haveScope
		}
	}
haveScope:
	if scope == "" {
		scope = fmt.Sprintf("line%d", callPos.Line)
	}

	return fmt.Sprintf("%s:%d:%d:%s", base, callPos.Line, callPos.Column, scope)
}

// stripParens removes wrapping parentheses from an expression.
func stripParens(e ast.Expr) ast.Expr {
	for {
		p, ok := e.(*ast.ParenExpr)
		if !ok {
			return e
		}
		e = p.X
	}
}

// globalsMutexSelector reports whether fun is a call target of the form
// globals.Lock / globals.Unlock, including (globals).Lock and (&globals).Lock,
// which parse with a non-*ast.Ident receiver.
func globalsMutexSelector(fun ast.Expr) (sel *ast.SelectorExpr, op string, ok bool) {
	s, ok := fun.(*ast.SelectorExpr)
	if !ok {
		return nil, "", false
	}
	switch s.Sel.Name {
	case "Lock", "Unlock":
	default:
		return nil, "", false
	}
	if !isGlobalsVarReceiver(s.X) {
		return nil, "", false
	}
	return s, s.Sel.Name, true
}

func isGlobalsVarReceiver(x ast.Expr) bool {
	x = stripParens(x)
	switch v := x.(type) {
	case *ast.Ident:
		return v.Name == "globals"
	case *ast.UnaryExpr:
		if v.Op != token.AND {
			return false
		}
		inner := stripParens(v.X)
		id, ok := inner.(*ast.Ident)
		return ok && id.Name == "globals"
	default:
		return false
	}
}

func funcDeclName(fd *ast.FuncDecl) string {
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return fd.Name.Name
	}
	rt := typeString(fd.Recv.List[0].Type)
	return "(" + rt + ")." + fd.Name.Name
}

func typeString(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.StarExpr:
		return "*" + typeString(t.X)
	case *ast.SelectorExpr:
		return typeString(t.X) + "." + t.Sel.Name
	default:
		return "?"
	}
}
