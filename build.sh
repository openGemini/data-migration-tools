#!/bin/bash
go mod tidy

num=$#

if [ "$num" -gt "0" ]; then
    GOOS=linux GOARCH=amd64 go build -mod=mod -o dataMigrate-linux-amd64 ./
    GOOS=linux GOARCH=arm64 go build -mod=mod -o dataMigrate-linux-arm64 ./
    GOOS=darwin GOARCH=amd64 go build -mod=mod -o dataMigrate-darwin-amd64 ./
    GOOS=darwin GOARCH=arm64 go build -mod=mod -o dataMigrate-darwin-arm64 ./
    GOOS=windows GOARCH=amd64 go build -mod=mod -o dataMigrate-windows-amd64.exe ./

    version=0.4.0
    mv dataMigrate-linux-amd64 dataMigrate
    tar -cvzf dataMigrate-$version-linux-amd64.tar.gz ./dataMigrate
    rm -rf ./dataMigrate

    mv dataMigrate-linux-arm64 dataMigrate
    tar -cvzf dataMigrate-$version-linux-arm64.tar.gz ./dataMigrate
    rm -rf ./dataMigrate

    mv dataMigrate-darwin-arm64 dataMigrate
    tar -cvzf dataMigrate-$version-darwin-arm64.tar.gz ./dataMigrate
    rm -rf ./dataMigrate

    mv dataMigrate-darwin-amd64 dataMigrate
    tar -cvzf dataMigrate-$version-darwin-amd64.tar.gz ./dataMigrate
    rm -rf ./dataMigrate

    mv dataMigrate-windows-amd64.exe dataMigrate.exe
    zip -o dataMigrate-$version-windows-amd64.zip ./dataMigrate.exe
    rm -rf ./dataMigrate.exe
else
    go build .
fi
