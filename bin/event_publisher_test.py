#!/usr/bin/env python3

from os import environ
import argparse
import json
from infcommon import logger
from infrabbitmq import factory as infrabbitmq_factory


def main(destination_exchange, broker_uri, event_name, network, data):
    infrabbitmq_factory.configure_pika_logger_to_error()

    event_publisher = infrabbitmq_factory.rabbitmq_event_publisher(exchange=destination_exchange, broker_uri=broker_uri)
    for index in range(10000):
        json_data = json.loads(data)
        json_data['index'] = index
        data = json.dumps(json_data)
        event_publisher.publish(event_name=event_name, network=network, data=data)


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('-d', '--destination_exchange', action='store', default='events', help='Default exchange is events')
        parser.add_argument('-e', '--event_name', action='store', required=True, help='')
        parser.add_argument('-n', '--network', action='store', required=True, help='Network name (ilo, c2k, ...)')
        parser.add_argument('-o', '--operations', action='store_true', default=False, help='Publish to operations broker')
        parser.add_argument('data')
        args = parser.parse_args()

        if args.operations:
            broker_uri = environ['OPERATIONS_BROKER_URI']
        else:
            broker_uri = environ['BROKER_URI']

        main(destination_exchange=args.destination_exchange, broker_uri=broker_uri, event_name=args.event_name, network=args.network, data=args.data)
    except Exception as exc:
        logger.critical(f'EventPublisher Fails. exc_type: {type(exc)} exc: {exc}',
                        exc_info=True)
