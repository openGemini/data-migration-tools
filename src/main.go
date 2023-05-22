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
    "os"
)

var logger *Logger

func main() {
    logger = NewLogger()
    defer logger.close()
    logger.LogString("Data migrate tool starting", TOCONSOLE, LEVEL_INFO)
    if err := Run(os.Args[1:]...); err != nil {
        logger.LogError(err)
        os.Exit(1)
    }
}

func Run(args ...string) error {
    if len(args) > 0 {
        cmd := NewDataMigrateCommand()
        if err := cmd.Run(args...); err != nil {
            return err
        }
    }
    return nil
}
