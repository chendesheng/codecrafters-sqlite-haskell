#!/bin/bash
export TESTER_DIR=$(pwd)
export CODECRAFTERS_SUBMISSION_DIR=$(pwd)
export CODECRAFTERS_TEST_CASES_JSON=$(cat test_cases.json)
(cd $GOPATH/sqlite-tester/ && ./dist/main.out)

