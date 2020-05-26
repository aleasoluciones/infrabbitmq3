#!/bin/bash

find . -name *.pyc -delete
source "dev/env_develop"

echo "Starting rabbitmq container..."
docker run -d --hostname infrabbit --name infrabbit -e RABBITMQ_DEFAULT_USER=infrabbit -e RABBITMQ_DEFAULT_PASS=infrabbit -p 15642:15672 -p 5642:5672 aleasoluciones/rabbitmq-delayed-message:0.2
sleep 5
echo -n "."
sleep 5
echo -n ".."
sleep 5
echo -n "..."
sleep 5
echo -n "...."
echo "Ready!"

echo
echo "----------------------------------------------------------------------"
echo "Running Factory tests"
echo "----------------------------------------------------------------------"
echo

python dev/run_factory_tests.py
FACTORY_TEST_RETCODE=$?

sleep 1
IMAGE_ID=$(docker stop $(docker ps | grep infrabbit | awk '{print $1}'))
docker rm $IMAGE_ID

exit $FACTORY_TEST_RETCODE
