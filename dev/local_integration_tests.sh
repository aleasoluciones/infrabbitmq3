#!/bin/bash

find . -name *pyc* -delete
source "dev/env_develop"

echo
echo "----------------------------------------------------------------------"
echo "Running Local Integration tests"
echo "----------------------------------------------------------------------"
echo
INTEGRATION_TESTS=`find . -maxdepth 2 -type d -name "integration_specs"`
mamba -f progress $INTEGRATION_TESTS
MAMBA_RETCODE=$?

exit $MAMBA_RETCODE
