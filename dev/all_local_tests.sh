#!/bin/bash

find . -name *.pyc -delete

dev/unit_tests.sh
UNIT_TESTS_RETCODE=$?

dev/local_integration_tests.sh
LOCAL_INTEGRATION_TESTS_RETCODE=$?

dev/factory_tests.sh
FACTORY_TEST=$?

exit $(( $UNIT_TESTS_RETCODE || $LOCAL_INTEGRATION_TESTS_RETCODE || $FACTORY_TEST ))
