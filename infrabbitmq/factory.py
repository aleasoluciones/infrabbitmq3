from os import environ
import logging

import pika

from infcommon.factory import Factory as Singleton
from infcommon.clock import Clock
from infcommon import logger
from infcommon.serializer import factory as serializer_factory

from infrabbitmq.rabbitmq import (
    TOPIC_EXCHANGE_TYPE,
    RabbitMQClient,
    RabbitMQEventPublisher,
    RabbitMQQueueEventProcessor,
)
from infrabbitmq.pika_client_wrapper import PikaClientWrapper
from infrabbitmq.events_builders import (
    RawEventBuilder,
    FelixEventBuilder,
)
from infrabbitmq.events_processors import (
    NoopProcessor,
    ConsoleLogEventsProcessor,
)


def configure_pika_logger_to_error():
    # By default is set to DEBUG
    logging.getLogger("pika").setLevel(logging.ERROR)


def no_singleton_rabbitmq_client(broker_uri=None, serializer=None):
    broker_uri = broker_uri or environ['BROKER_URI']
    serializer = serializer or serializer_factory.json_serializer()

    return RabbitMQClient(broker_uri,
                          serializer,
                          _no_singleton_pika_client_wrapper(),
                          logger)


def _no_singleton_pika_client_wrapper(pika_library=pika):
    return PikaClientWrapper(pika_library=pika_library)


def rabbitmq_event_publisher(exchange='events', broker_uri=None):
    return Singleton.instance('event_publisher_{}_{}'.format(exchange, broker_uri),
                              lambda: RabbitMQEventPublisher(rabbitmq_client=no_singleton_rabbitmq_client(broker_uri=broker_uri),
                                                             clock_service=_clock_service(),
                                                             exchange=exchange)
                              )


def rabbitmq_event_publisher_pickle_serializer(exchange='events', broker_uri=None):
    return Singleton.instance('event_publisher_pickle_serializer_{}_{}'.format(exchange, broker_uri),
                              lambda: RabbitMQEventPublisher(rabbitmq_client=no_singleton_rabbitmq_client(broker_uri=broker_uri,
                                                                                                          serializer=serializer_factory.pickle_serializer()),
                                                             clock_service=_clock_service(),
                                                             exchange=exchange)
                              )


def rabbitmq_event_publisher_json_serializer(exchange='events', broker_uri=None):
    return Singleton.instance('event_publisher_json_serializer_{}_{}'.format(exchange, broker_uri),
                              lambda: RabbitMQEventPublisher(rabbitmq_client=no_singleton_rabbitmq_client(broker_uri=broker_uri,
                                                                                                          serializer=serializer_factory.json_serializer()),
                                                             clock_service=_clock_service(),
                                                             exchange=exchange)
                              )


def no_singleton_rabbitmq_queue_event_processor(queue_name, exchange, list_of_topics, event_processor, serializer=None,
                                                queue_options=None, exchange_options=None, event_builder=None,
                                                exchange_type=TOPIC_EXCHANGE_TYPE, logger=logger):
    if event_builder is None:
        event_builder = felix_event_builder()

    if serializer is None:
        serializer = serializer_factory.json_or_pickle_serializer()

    return RabbitMQQueueEventProcessor(queue_name=queue_name,
                                       event_processor=event_processor,
                                       rabbitmq_client=no_singleton_rabbitmq_client(serializer=serializer),
                                       exchange=exchange,
                                       list_of_topics=list_of_topics,
                                       exchange_options=exchange_options or {},
                                       queue_options=queue_options or {},
                                       event_builder=event_builder,
                                       exchange_type=exchange_type,
                                       logger=logger)


def _clock_service():
    return Singleton.instance("_clock_service", lambda: Clock())


def raw_event_builder():
    return Singleton.instance('raw_event_builder',
                              lambda: RawEventBuilder())


def felix_event_builder():
    return Singleton.instance('felix_event_builder',
                              lambda: FelixEventBuilder())


def noop_event_processor():
    return Singleton.instance('noop_event_processor',
                              lambda: NoopProcessor())


def console_log_event_processor():
    return Singleton.instance('console_log_event_processor',
                              lambda: ConsoleLogEventsProcessor())
