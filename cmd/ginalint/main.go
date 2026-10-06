// Command ginalint fails if any Go file under the given directories (default ".")
// uses goroutines, channels, or a denylisted package.
package main

import (
	"fmt"
	"os"
	"strings"

	"gina/internal/lint"
)

func main() {
	roots := os.Args[1:]
	if len(roots) == 0 {
		roots = []string{"."}
	}
	bad := 0
	for _, r := range roots {
		r = strings.TrimSuffix(strings.TrimSuffix(r, "..."), "/")
		if r == "" {
			r = "."
		}
		fs, err := lint.CheckDir(r)
		if err != nil {
			fmt.Fprintln(os.Stderr, "ginalint:", err)
			os.Exit(2)
		}
		for _, f := range fs {
			fmt.Println(f)
			bad++
		}
	}
	if bad > 0 {
		fmt.Fprintf(os.Stderr, "ginalint: %d violation(s)\n", bad)
		os.Exit(1)
	}
	fmt.Println("ginalint: ok")
}
