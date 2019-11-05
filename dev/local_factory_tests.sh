#!/bin/bash

find . -name *.pyc -delete
source "dev/env_develop"

echo
echo "----------------------------------------------------------------------"
echo "Running Factory tests"
echo "----------------------------------------------------------------------"
echo

python dev/run_factory_tests.py
