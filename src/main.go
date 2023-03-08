package main

import (
    "fmt"
    "os"
    "io"
    "log"
)

func main() {
    m := NewMain()
    if err := m.Run(os.Args[1:]...); err != nil {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
    }
}

type Main struct {
    Logger *log.Logger
    Stdin  io.Reader
    Stdout io.Writer
    Stderr io.Writer
}

func NewMain() *Main {
    return &Main{
	Logger: log.New(os.Stderr, "[dataMigrate] ", log.LstdFlags),
	Stdin:  os.Stdin,
	Stdout: os.Stdout,
	Stderr: os.Stderr,
    }
}

func (m *Main) Run(args ...string) error {
    if len(args) > 0 {
	cmd := NewDataMigrateCommand()
	if err := cmd.Run(args...); err != nil {
	    return fmt.Errorf("dataMigrate: %s", err)
	}
    }
    return nil
}
