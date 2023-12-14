from mamba import description, before, context, it, after
from expects import expect, be_none

from os import (
    environ,
    getpid,
)
import pika

from infcommon import logger
from infcommon.serializer import factory as serializer_factory

from infrabbitmq.rabbitmq import (
    RabbitMQClient,
    DIRECT_EXCHANGE_TYPE,
    TOPIC_EXCHANGE_TYPE,
)
from infrabbitmq.pika_client_wrapper import PikaClientWrapper
from infrabbitmq import factory

MY_DIRECT_EXCHANGE_NAME = 'my_direct_exchange_name'
MY_TOPIC_EXCHANGE_NAME = 'my_topic_exchange_name'
MY_DIRECT_QUEUE_NAME = f'my_direct_queue_name_{getpid()}'
MY_TOPIC_QUEUE_NAME = f'my_topic_queue_name_{getpid()}'

DEFAULT_ROUTING_KEY = ''
MY_ROUTING_KEY = '#'

A_MESSAGE = 'a_message'
ANOTHER_MESSAGE = 'another_message'
SOME_ANOTHER_MESSAGE = 'some_another_message'


with description('RabbitMQClient Integration tests - Purging a queue') as self:
    with before.each:
        self.broker_uri = environ['BROKER_URI']
        self.serializer = serializer_factory.json_serializer()
        self.pika_wrapper_client = PikaClientWrapper(pika_library=pika)
        self.logger = logger
        self.compressor = factory._compressor()
        self.sut = RabbitMQClient(self.broker_uri,
                                  self.serializer,
                                  self.pika_wrapper_client,
                                  self.logger,
                                  self.compressor)

    with context('when purging a queue (direct exchange)'):
        with before.each:
            self.sut.exchange_declare(exchange=MY_DIRECT_EXCHANGE_NAME, exchange_type=DIRECT_EXCHANGE_TYPE)
            self.sut.queue_declare(queue_name=MY_DIRECT_QUEUE_NAME, auto_delete=False)
            self.sut.queue_bind(queue_name=MY_DIRECT_QUEUE_NAME, exchange=MY_DIRECT_EXCHANGE_NAME, routing_key=DEFAULT_ROUTING_KEY)

        with after.each:
            self.sut.queue_unbind(queue_name=MY_DIRECT_QUEUE_NAME, exchange=MY_DIRECT_EXCHANGE_NAME, routing_key=DEFAULT_ROUTING_KEY)
            self.sut.queue_delete(queue_name=MY_DIRECT_QUEUE_NAME)
            self.sut.exchange_delete(exchange=MY_DIRECT_EXCHANGE_NAME)

        with it('does NOT consume any message from that queue_name'):
            self.sut.publish(exchange=MY_DIRECT_EXCHANGE_NAME, routing_key=DEFAULT_ROUTING_KEY, message=A_MESSAGE)
            self.sut.publish(exchange=MY_DIRECT_EXCHANGE_NAME, routing_key=DEFAULT_ROUTING_KEY, message=ANOTHER_MESSAGE)
            self.sut.publish(exchange=MY_DIRECT_EXCHANGE_NAME, routing_key=DEFAULT_ROUTING_KEY, message=SOME_ANOTHER_MESSAGE)

            self.sut.queue_purge(queue_name=MY_DIRECT_QUEUE_NAME)

            first_consumed_message = self.sut.consume(queue_name=MY_DIRECT_QUEUE_NAME)
            second_consumed_message = self.sut.consume(queue_name=MY_DIRECT_QUEUE_NAME)
            third_consumed_message = self.sut.consume(queue_name=MY_DIRECT_QUEUE_NAME)

            expect(first_consumed_message).to(be_none)
            expect(second_consumed_message).to(be_none)
            expect(third_consumed_message).to(be_none)

    with context('when purging a queue (topic exchange)'):
        with before.each:
            self.sut.exchange_declare(exchange=MY_TOPIC_EXCHANGE_NAME, exchange_type=TOPIC_EXCHANGE_TYPE)
            self.sut.queue_declare(queue_name=MY_TOPIC_QUEUE_NAME, auto_delete=False)
            self.sut.queue_bind(queue_name=MY_TOPIC_QUEUE_NAME, exchange=MY_TOPIC_EXCHANGE_NAME, routing_key=MY_ROUTING_KEY)

        with after.each:
            self.sut.queue_unbind(queue_name=MY_TOPIC_QUEUE_NAME, exchange=MY_TOPIC_EXCHANGE_NAME, routing_key=MY_ROUTING_KEY)
            self.sut.queue_delete(queue_name=MY_TOPIC_QUEUE_NAME)
            self.sut.exchange_delete(exchange=MY_DIRECT_EXCHANGE_NAME)

        with it('does NOT consume any message from that queue'):
            self.sut.publish(exchange=MY_TOPIC_EXCHANGE_NAME, routing_key='routing.key.1', message=A_MESSAGE)
            self.sut.publish(exchange=MY_TOPIC_EXCHANGE_NAME, routing_key='routing.key.1', message=ANOTHER_MESSAGE)
            self.sut.publish(exchange=MY_TOPIC_EXCHANGE_NAME, routing_key='routing.key.2', message=SOME_ANOTHER_MESSAGE)

            self.sut.queue_purge(queue_name=MY_TOPIC_QUEUE_NAME)

            first_consumed_message = self.sut.consume(queue_name=MY_TOPIC_QUEUE_NAME)
            second_consumed_message = self.sut.consume(queue_name=MY_TOPIC_QUEUE_NAME)
            third_consumed_message = self.sut.consume(queue_name=MY_TOPIC_QUEUE_NAME)

            expect(first_consumed_message).to(be_none)
            expect(second_consumed_message).to(be_none)
            expect(third_consumed_message).to(be_none)
