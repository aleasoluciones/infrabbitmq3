#!/usr/bin/env python3

import argparse
from infcommon import logger
from infrabbitmq import factory as infrabbitmq_factory


def main(queue_name, exchange, routing_key):
    infrabbitmq_factory.configure_pika_logger_to_error()

    rabbitmq_client = infrabbitmq_factory.no_singleton_rabbitmq_client()
    logger.info(f'Unbind queue: {queue_name} exchange: {exchange} routing_key: {routing_key}')
    rabbitmq_client.queue_unbind(queue_name=queue_name, exchange=exchange, routing_key=routing_key)
    logger.info('Unbind complete')


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('queue_name')
        parser.add_argument('exchange')
        parser.add_argument('routing_key')
        args = parser.parse_args()

        main(queue_name=args.queue_name, exchange=args.exchange, routing_key=args.routing_key)
    except Exception as exc:
        logger.critical(f'Queue unbind Fails. exc_type: {type(exc)} exc: {exc}',
                        exc_info=True)
