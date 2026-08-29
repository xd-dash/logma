package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/xd-dash/logma/serverless/callbacks/gdrive"
)

func main() {
	uploader, err := gdrive.NewFromEnv()
	if err != nil {
		fatal(err)
	}

	payload := strings.TrimSpace(strings.Join(os.Args[1:], " "))
	if payload == "" {
		data, err := bufio.NewReader(os.Stdin).ReadString('\n')
		if err != nil && len(data) == 0 {
			fatal(fmt.Errorf("read callback payload: %w", err))
		}
		payload = strings.TrimSpace(data)
	}
	if payload == "" {
		fatal(fmt.Errorf("Google Drive callback payload is empty"))
	}

	if err := uploader.Handle(context.Background(), payload); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "logma-gdrive-callback:", err)
	os.Exit(1)
}
