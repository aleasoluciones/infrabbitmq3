# infrabbitmq3

[![Build status](https://secure.travis-ci.org/aleasoluciones/infrabbitmq3.svg?branch=master)](https://secure.travis-ci.org/aleasoluciones/infrabbitmq3)

Wrapper for the [pika](https://pika.readthedocs.io/en/stable/) library using Python 3.7.

## Development

### Setup

Create a virtual environment, install dependencies and load environment variables.

```sh
mkvirtualenv infrabbitmq3 -p $(which python3.7)
dev/setup_venv.sh
source dev/env_develop
```

Run dependencies (in this case, a RabbitMQ docker container).

```sh
dev/start_infrabbitmq3_dependencies.sh
```

### Running tests, linter & formatter and configure git hooks

Note that this project uses Alea's [pydevlib](https://github.com/aleasoluciones/pydevlib), so take a look at its README or run the command `pydevlib` from the virtual environment to see a summary of the available commands.
