from mamba import description, before, context, it
from expects import expect, raise_error

from os import environ
import pika

from infcommon import logger
from infcommon.serializer import factory as serializer_factory

from infrabbitmq.exceptions import RabbitMQError
from infrabbitmq.rabbitmq import (
    RabbitMQClient,
    TOPIC_EXCHANGE_TYPE,
)
from infrabbitmq.pika_client_wrapper import PikaClientWrapper

MY_DIRECT_EXCHANGE_NAME = 'my_direct_exchange_name'
A_MESSAGE = 'a_message'


with description('RabbitMQClient Integration tests - Handling errors') as self:
    with before.each:
        self.broker_uri = environ['BROKER_URI']
        self.serializer = serializer_factory.json_serializer()
        self.pika_wrapper_client = PikaClientWrapper(pika_library=pika)
        self.logger = logger
        self.sut = RabbitMQClient(self.broker_uri,
                                  self.serializer,
                                  self.pika_wrapper_client,
                                  self.logger)

    with context('when the exchange is not declared'):
        with it('raises RabbitMQError'):
            def _callback():
                self.sut.publish(exchange='NON_EXISTING_EXCHANGE', routing_key='irrelevant_routing_key', message=A_MESSAGE)

            expect(_callback).to(raise_error(RabbitMQError))

    with context('when operating in a not declared queue'):
        with it('raises RabbitMQError'):
            def _callback():
                self.sut.consume(queue_name='NON_EXISTING_QUEUE')

            expect(_callback).to(raise_error(RabbitMQError))

    with context('when trying to do an operation and the connection is not allowed '):
        with it('raises RabbitMQError'):
            def _callback():
                broker_uri = 'rabbitmq://WRONGUSER:WRONGPASSWD@localhost:5672/'
                sut = RabbitMQClient(broker_uri,
                                              self.serializer,
                                              self.pika_wrapper_client,
                                              self.logger)
                sut.exchange_declare(exchange=MY_DIRECT_EXCHANGE_NAME, exchange_type=TOPIC_EXCHANGE_TYPE)

            expect(_callback).to(raise_error(RabbitMQError))
