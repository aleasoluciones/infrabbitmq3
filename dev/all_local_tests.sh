#!/bin/bash

dev/unit_tests.sh && \
dev/local_integration_tests.sh && \
dev/local_factory_tests.sh
