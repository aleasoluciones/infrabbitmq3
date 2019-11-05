#!/bin/bash

find . -name *.pyc -delete

dev/unit_tests.sh
UNIT_TESTS_RETCODE=$?

dev/integration_tests.sh
INTEGRATION_TESTS_RETCODE=$?

dev/factory_tests.sh
FACTORY_TEST=$?

exit $UNIT_TESTS_RETCODE || $INTEGRATION_TESTS_RETCODE || $FACTORY_TEST

