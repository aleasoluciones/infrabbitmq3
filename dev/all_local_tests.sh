#!/bin/bash

dev/unit_tests.sh && \
dev/local_integration_tests.sh && \
dev/factory_tests.sh
