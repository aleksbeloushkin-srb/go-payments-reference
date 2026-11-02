// Command testreport reads `go test -json` from stdin, prints the usual test
// output, and turns every failed test into a GitHub Actions error annotation,
// so a failure is visible on the run page without opening the job log. It
// exits 1 if any test or package failed.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type event struct {
	Action  string
	Package string
	Test    string
	Output  string
}

func main() {
	failed := false
	output := map[string][]string{} // package/test -> output lines
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		var e event
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			fmt.Println(sc.Text()) // not JSON: build errors and the like
			continue
		}
		key := e.Package + "/" + e.Test
		switch e.Action {
		case "output":
			fmt.Print(e.Output)
			output[key] = append(output[key], e.Output)
		case "fail":
			failed = true
			name := e.Package
			if e.Test != "" {
				name += "." + e.Test
			}
			fmt.Printf("::error title=%s failed::%s\n", name, annotation(output[key]))
		}
	}
	if err := sc.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "testreport:", err)
		os.Exit(1)
	}
	if failed {
		os.Exit(1)
	}
}

// annotation keeps the last lines of a test's output, the part that usually
// holds the assertion, in the single-line form annotations require.
func annotation(lines []string) string {
	const keep = 12
	if len(lines) > keep {
		lines = lines[len(lines)-keep:]
	}
	s := strings.TrimSpace(strings.Join(lines, ""))
	s = strings.ReplaceAll(s, "%", "%25")
	s = strings.ReplaceAll(s, "\r", "%0D")
	return strings.ReplaceAll(s, "\n", "%0A")
}
