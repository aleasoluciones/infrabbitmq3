# infrabbitmq3

[![Build status](https://secure.travis-ci.org/aleasoluciones/infrabbitmq3.svg?branch=master)](https://secure.travis-ci.org/aleasoluciones/infrabbitmq3)

Infrastructure RabbitMQ Client.
This is the migration from infrabbitmq which uses python2.7 and puka to a new infrabbitmq using python 3.x and pika

## Installation

### Requirements

Add python 3.7 or above to your Linux host
```
$ sudo add-apt-repository ppa:deadsnakes/ppa
$ sudo apt-get update
$ sudo apt-get install python-virtualenv python3.7 python3.7-dev
```

### Configure virtual environment
You can use virtualenv wrapper (mkvirtualenv)
```
$ mkvirtualenv -p $(which python3) infrabbitmq3
```
Or default virtualenv
```
$ cd /home/USER/DEV_WORKSPACE/infrabbitmq3
$ virtualenv -p $(which python3.7) infrabbitmq3_ve
$ source infrabbitmq3_ve/bin/activate
```

Remember that virtualenv creates the virtual_env_directory directly at the actual path
Remember to exclude virtualenv directory from git

### Install dependencies
Inside your virtualenv, run `dev/setup_venv.sh`

### Set environment
Inside your virtualenv, run `source dev/env_develops`

### Run at localhost
Start a rabbitmq server 
```
$ dev/start_local_dependencies.sh
```

Stop a rabbitmq server 
```
$ dev/stop_local_dependencies.sh
```

## Running test locally
First, start local dependencies

## Unit tests
```
$ dev/unit_tests.sh
```

## Integration tests
```
$ dev/local_integration_tests.sh
```

## Factory tests
```
$ dev/local_factory_tests.sh
```

## All tests
```
$ dev/all_local_tests.sh
```

## Running test without starting local dependencies
Integration and factory tests will start and stop a rabbitmq dockerized

## Unit tests
```
$ dev/unit_tests.sh
```

## Integration tests
```
$ dev/integration_tests.sh
```

## Factory tests
```
$ dev/factory_tests.sh
```

## All tests
```
$ dev/all_tests.sh
```
