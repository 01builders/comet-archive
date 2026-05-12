package main

import (
	"fmt"
	"os"

	"github.com/01builders/cometbft-archive/internal/cli"
)

func main() {
	cmd, err := cli.NewRootCommand()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
