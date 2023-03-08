/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
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
