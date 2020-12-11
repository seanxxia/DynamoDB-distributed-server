#!/bin/bash
# shellcheck disable=SC2068
rm -rf ./bin/DynamoTest*
go install ./src/mydynamotest/...
cd src/mydynamotest
ginkgo -nodes=5
