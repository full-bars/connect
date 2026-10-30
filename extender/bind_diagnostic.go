package extender

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"syscall"
	"time"
)

var bindOwnerPattern = regexp.MustCompile(`\(\("([^"\\]+)",pid=([0-9]+)`)

// The lookup is best effort and bounded. Unprivileged ss may not show an owner.
func bindOwnerDetail(transport string, port int, err error) string {
	if !errors.Is(err, syscall.EADDRINUSE) {
		return ""
	}
	fallback := "; port already in use by another process"
	flag := "-u"
	if transport == "tcp" {
		flag = "-t"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	output, lookupErr := exec.CommandContext(ctx, "ss", "-H", "-l", "-n", "-p", flag, fmt.Sprintf("sport = :%d", port)).Output()
	if lookupErr != nil {
		return fallback
	}
	match := bindOwnerPattern.FindSubmatch(output)
	if len(match) != 3 {
		return fallback
	}
	name := strings.Join(strings.Fields(string(match[1])), "_")
	return fmt.Sprintf("; port already in use owner=%s pid=%s", name, match[2])
}
