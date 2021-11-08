#!/bin/bash

SCRIPTPATH=`dirname $(realpath $0)`
. ${SCRIPTPATH}/env_develop

docker-compose -f ${SCRIPTPATH}/infrabbitmq3_devdocker/docker-compose.yml stop && \
docker-compose -f ${SCRIPTPATH}/infrabbitmq3_devdocker/docker-compose.yml rm -f
