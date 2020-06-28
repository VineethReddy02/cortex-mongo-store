#!/bin/bash

cd grpc/ && protoc --go_out=plugins=grpc:. *.proto && cd ..
go build -o bin/cortex-mongo-store github.com/VineethReddy02/cortex-mongo-store
