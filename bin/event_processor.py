#!/usr/bin/env python3

from os import (
    getenv,
    getpid,
)
from sys import exit
from importlib import import_module
import argparse

from infcommon import (
    logger,
    utils,
)
from infcommon.serializer import factory as serializer_factory
from infrabbitmq import factory as infrabbitmq_factory
from infrabbitmq.rabbitmq import TOPIC_EXCHANGE_TYPE
from infrabbitmq.exceptions import RabbitMQError


class Importer:

    @classmethod
    def import_module(cls, module_name):
        return import_module(module_name)

    @classmethod
    def get_symbol(cls, symbol_name):
        module_name = cls._extract_module_name(symbol_name)
        final_symbol_name = cls._extract_final_symbol_name(symbol_name)
        module = import_module(module_name)
        try:
            return getattr(module, final_symbol_name)
        except AttributeError:
            raise ImportError()

    @classmethod
    def _extract_module_name(cls, symbol_name):
        return '.'.join(symbol_name.split('.')[:-1])

    @classmethod
    def _extract_final_symbol_name(cls, symbol_name):
        return symbol_name.split('.')[-1:][0]


class LogProcessor:
    def __init__(self, processor):
        self._processor = processor

    def process(self, event):
        logger.debug('Processor {} processing {}'.format(self._processor.__class__.__name__,
                                                         event))
        self._processor.process(event)


def _event_processor_name(factory_func_name):
    return factory_func_name.split('.')[-1:]


def _queue_event_processor(queue_name, exchange, list_of_topics, event_processor, message_ttl_milliseconds, serializer, event_builder, exchange_type, logger):
    queue_options = {'message_ttl': message_ttl_milliseconds}
    return infrabbitmq_factory.no_singleton_rabbitmq_queue_event_processor(queue_name=queue_name,
                                                                           exchange=exchange,
                                                                           list_of_topics=list_of_topics,
                                                                           event_processor=event_processor,
                                                                           serializer=serializer,
                                                                           queue_options=queue_options,
                                                                           exchange_options=None,
                                                                           event_builder=event_builder,
                                                                           exchange_type=exchange_type,
                                                                           logger=logger
                                                                           )


def _process_body_events(queue_name, exchange, list_of_topics, event_processor, message_ttl_milliseconds, serializer, event_builder, exchange_type, logger):
    logger.info("Connecting")
    # Be aware. Each time this function is called, we are creating a new no_singleton_rabbitmq_queue_event_processor
    queue_event_processor = _queue_event_processor(queue_name, exchange,
                                                   list_of_topics,
                                                   event_processor,
                                                   message_ttl_milliseconds,
                                                   serializer,
                                                   event_builder,
                                                   exchange_type,
                                                   logger)
    queue_event_processor.process_body()


def main(factory, network,
         exchange, exchange_type,
         queue_name, list_of_topics,
         serialization, event_builder,
         message_ttl_milliseconds):

    infrabbitmq_factory.configure_pika_logger_to_error()

    # Build event_processor
    if factory:
        event_processor_class = Importer.get_symbol(factory)
        event_processor = event_processor_class(network) if network else event_processor_class()
        processor_name = _event_processor_name(factory)
    else:
        event_processor = infrabbitmq_factory.noop_event_processor()
        processor_name = event_processor.__class__.__name__

    # Serializer
    serializer = None
    if 'json' == serialization:
        serializer = serializer_factory.json_serializer()
    elif 'pickle' == serialization:
        serializer = serializer.pickle_serializer()

    # EventBuilder instance
    event_builder_instance = Importer.get_symbol(event_builder)()

    logger.info(f'PID ({getpid()}) Starting event_processor {processor_name}')
    logger.info(f'{processor_name} queue {queue_name} list_of_topics {list_of_topics}')
    logger.info(f'{processor_name} deserializer {serializer}')

    # Execute
    utils.do_stuff_with_exponential_backoff((RabbitMQError,),
                                            _process_body_events,
                                            queue_name,
                                            exchange,
                                            list_of_topics,
                                            LogProcessor(event_processor),
                                            message_ttl_milliseconds,
                                            serializer,
                                            event_builder_instance,
                                            exchange_type,
                                            logger)


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('-f', '--factory', action='store', required=False, help='')
        parser.add_argument('-b', '--event-builder', action='store', required=False, default='infrabbitmq.factory.felix_event_builder', help='Python function to build an event object')
        parser.add_argument('-e', '--exchange', action='store', required=True, help='')
        parser.add_argument('-et', '--exchange-type', action='store', default=TOPIC_EXCHANGE_TYPE, help='Select exchange type: topic, direct, x-delayed-message')
        parser.add_argument('-q', '--queue-name', action='store', required=True, help='')
        parser.add_argument('-t', '--list-of-topics', nargs='+', action='store', required=True, help="List of topics (example): -t '*.foo.bar' '*.bar.foo'")
        parser.add_argument('-n', '--network', action='store', required=False, help='')
        parser.add_argument('-s', '--serialization', action="store", required=False, help="Select serialization Json, Pickle")
        parser.add_argument('-ttl', '--message-ttl-milliseconds', action='store', type=int, default=None, help='In milliseconds!')
        args = parser.parse_args()

        main(factory=args.factory, network=args.network,
             exchange=args.exchange, exchange_type=args.exchange_type,
             queue_name=args.queue_name, list_of_topics=args.list_of_topics,
             serialization=args.serialization,
             event_builder=args.event_builder,
             message_ttl_milliseconds=args.message_ttl_milliseconds)
    except Exception as exc:
        logger.critical(f'EventProcessor Fails. exc_type: {type(exc)} exc: {exc}',
                        exc_info=True)
        exit(-1)
