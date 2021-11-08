#!/bin/bash

find . -name *pyc* -delete

SCRIPTPATH=`dirname $(realpath $0)`
. ${SCRIPTPATH}/env_develop

docker-compose -f ${SCRIPTPATH}/infrabbitmq3_devdocker/docker-compose.yml up -d

# Wait for rabbit and redis ports to be available
TIMEOUT=30
for port in ${BROKER_MANAGEMENT_PORT} ${BROKER_PORT}; do
    printf "Checking port ${port} ... "
    if [[ $(uname) == 'Linux' ]]; then
        timeout ${TIMEOUT} bash -c "until echo > /dev/tcp/localhost/${port}; do sleep 0.5; done" > /dev/null 2>&1
        [[ $? -eq 0 ]] && echo -e '\e[32mOK\e[0m' || echo -e '\e[31mNOK\e[0m'
    elif [[ -x $(command -v nc) ]]; then
        timeout ${TIMEOUT} bash -c "until nc -vz ${DOCKER_HOST_IP} ${port}; do sleep 0.5; done" > /dev/null 2>&1
        [[ $? -eq 0 ]] && echo -e '\e[32mOK\e[0m' || echo -e '\e[31mNOK\e[0m'
    else
        echo -e "Unable to check port"
    fi
done;
