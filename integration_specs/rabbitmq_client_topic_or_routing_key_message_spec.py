from mamba import description, before, context, it, after
from expects import equal, expect, be_none

from os import (
    environ,
    getpid,
)
import pika

from infcommon import logger
from infcommon.serializer import factory as serializer_factory

from infrabbitmq.rabbitmq import (
    RabbitMQClient,
    TOPIC_EXCHANGE_TYPE,
)
from infrabbitmq.pika_client_wrapper import PikaClientWrapper

MY_TOPIC_EXCHANGE_NAME = 'my_topic_exchange_name'
MY_QUEUE_NAME = 'my_queue_name_{}'.format(getpid())

A_MESSAGE = 'a_message'
ANOTHER_MESSAGE = 'another_message'


with description('RabbitMQClient Integration tests - Consuming and publishing Topic Exchange (routing key)') as self:
    with before.each:
        self.broker_uri = environ['BROKER_URI']
        self.serializer = serializer_factory.json_serializer()
        self.pika_wrapper_client = PikaClientWrapper(pika_library=pika)
        self.logger = logger
        self.sut = RabbitMQClient(self.broker_uri,
                                  self.serializer,
                                  self.pika_wrapper_client,
                                  self.logger)

        self.sut.exchange_declare(exchange=MY_TOPIC_EXCHANGE_NAME, exchange_type=TOPIC_EXCHANGE_TYPE)
        self.sut.queue_declare(queue_name=MY_QUEUE_NAME, auto_delete=False)

    with after.each:
        self.sut.queue_delete(queue_name=MY_QUEUE_NAME)
        self.sut.exchange_delete(exchange=MY_TOPIC_EXCHANGE_NAME)

    with context('when topic is #'):
        with before.each:
            self.topic_routing_key = '#'
            self.sut.queue_bind(queue_name=MY_QUEUE_NAME, exchange=MY_TOPIC_EXCHANGE_NAME, routing_key=self.topic_routing_key)

        with after.each:
            self.sut.queue_unbind(queue_name=MY_QUEUE_NAME, exchange=MY_TOPIC_EXCHANGE_NAME, routing_key=self.topic_routing_key)

        with it('consumes all messages'):
            self.sut.publish(exchange=MY_TOPIC_EXCHANGE_NAME, routing_key="kernel.critical", message=A_MESSAGE)
            self.sut.publish(exchange=MY_TOPIC_EXCHANGE_NAME, routing_key="mail.critical.info", message=ANOTHER_MESSAGE)

            msg_1 = self.sut.consume(queue_name=MY_QUEUE_NAME)
            msg_2 = self.sut.consume(queue_name=MY_QUEUE_NAME)

            expect(msg_1.body).to(equal(A_MESSAGE))
            expect(msg_2.body).to(equal(ANOTHER_MESSAGE))

    with context('when topic is kernel.#'):
        with before.each:
            self.topic_routing_key = 'kernel.#'
            self.sut.queue_bind(queue_name=MY_QUEUE_NAME, exchange=MY_TOPIC_EXCHANGE_NAME, routing_key=self.topic_routing_key)

        with after.each:
            self.sut.queue_unbind(queue_name=MY_QUEUE_NAME, exchange=MY_TOPIC_EXCHANGE_NAME, routing_key=self.topic_routing_key)

        with it('consumes kernel messages only'):
            self.sut.publish(exchange=MY_TOPIC_EXCHANGE_NAME, routing_key="kernel.critical", message=A_MESSAGE)
            self.sut.publish(exchange=MY_TOPIC_EXCHANGE_NAME, routing_key="mail.critical.info", message=ANOTHER_MESSAGE)

            msg_1 = self.sut.consume(queue_name=MY_QUEUE_NAME)
            msg_2 = self.sut.consume(queue_name=MY_QUEUE_NAME)

            expect(msg_1.body).to(equal(A_MESSAGE))
            expect(msg_2).to(be_none)

    with context('queue with multiple binding (when receiving from 2 routing keys)'):
        with before.each:
            self.topic_routing_key_1 = 'topic_routing_key_1'
            self.topic_routing_key_2 = 'topic_routing_key_2'
            self.sut.queue_bind(queue_name=MY_QUEUE_NAME, exchange=MY_TOPIC_EXCHANGE_NAME, routing_key=self.topic_routing_key_1)
            self.sut.queue_bind(queue_name=MY_QUEUE_NAME, exchange=MY_TOPIC_EXCHANGE_NAME, routing_key=self.topic_routing_key_2)

        with after.each:
            self.sut.queue_unbind(queue_name=MY_QUEUE_NAME, exchange=MY_TOPIC_EXCHANGE_NAME, routing_key=self.topic_routing_key_1)
            self.sut.queue_unbind(queue_name=MY_QUEUE_NAME, exchange=MY_TOPIC_EXCHANGE_NAME, routing_key=self.topic_routing_key_2)

        with it('consumes all messages'):
            self.sut.publish(exchange=MY_TOPIC_EXCHANGE_NAME, routing_key=self.topic_routing_key_1, message=A_MESSAGE)
            self.sut.publish(exchange=MY_TOPIC_EXCHANGE_NAME, routing_key=self.topic_routing_key_2, message=ANOTHER_MESSAGE)

            msg_1 = self.sut.consume(queue_name=MY_QUEUE_NAME)
            msg_2 = self.sut.consume(queue_name=MY_QUEUE_NAME)
            msg_3 = self.sut.consume(queue_name=MY_QUEUE_NAME)

            expect(msg_1.body).to(equal(A_MESSAGE))
            expect(msg_2.body).to(equal(ANOTHER_MESSAGE))
            expect(msg_3).to(be_none)
