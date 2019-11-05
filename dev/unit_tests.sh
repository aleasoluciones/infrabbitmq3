#!/bin/bash

echo
echo "----------------------------------------------------------------------"
echo "Running Specs"
echo "----------------------------------------------------------------------"
echo
mamba -f progress `find . -maxdepth 2 -type d -name "specs"`
MAMBA_RETCODE=$?

exit $MAMBA_RETCODE
