package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
)

func main() {
	args := os.Args[1:]
	if len(args) == 2 && args[0] == "--" {
		args = args[1:]
	}
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "usage: %s <types.go>\n", os.Args[0])
		os.Exit(2)
	}

	path := args[0]
	src, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read %s: %v\n", path, err)
		os.Exit(1)
	}

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse %s: %v\n", path, err)
		os.Exit(1)
	}

	blocks := make([]declBlock, 0, len(file.Decls))
	firstStart := -1
	suffixStart := len(src)
	inTypeSection := false

	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !inTypeSection {
			if ok && gen.Tok == token.TYPE {
				inTypeSection = true
			} else {
				continue
			}
		}

		if !(ok && gen.Tok == token.TYPE) {
			suffixStart = offsetOf(fset, decl.Pos())
			break
		}

		startPos := decl.Pos()
		if gen.Doc != nil {
			startPos = gen.Doc.Pos()
		}
		start := offsetOf(fset, startPos)
		end := offsetOf(fset, decl.End())
		if firstStart == -1 {
			firstStart = start
		}
		blocks = append(blocks, declBlock{
			name: firstTypeName(gen),
			text: bytes.TrimSpace(src[start:end]),
		})
	}

	if len(blocks) == 0 {
		return
	}

	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].name < blocks[j].name
	})

	var out bytes.Buffer
	out.Write(src[:firstStart])
	for i, block := range blocks {
		if i > 0 {
			out.WriteString("\n\n")
		}
		out.Write(block.text)
	}

	if suffixStart < len(src) {
		out.WriteString("\n\n")
		out.Write(bytes.TrimLeft(src[suffixStart:], "\n"))
	} else {
		out.WriteByte('\n')
	}

	if err := os.WriteFile(path, out.Bytes(), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write %s: %v\n", path, err)
		os.Exit(1)
	}
}

type declBlock struct {
	name string
	text []byte
}

func firstTypeName(gen *ast.GenDecl) string {
	if len(gen.Specs) == 0 {
		return ""
	}
	spec, ok := gen.Specs[0].(*ast.TypeSpec)
	if !ok {
		return ""
	}
	return spec.Name.Name
}

func offsetOf(fset *token.FileSet, pos token.Pos) int {
	return fset.Position(pos).Offset
}
