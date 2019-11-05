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
    DIRECT_EXCHANGE_TYPE,
)
from infrabbitmq.pika_client_wrapper import PikaClientWrapper

MY_DIRECT_EXCHANGE_NAME = 'my_direct_exchange_name'
A_QUEUE_NAME = 'a_queue_name_{}'.format(getpid())

DEFAULT_ROUTING_KEY = ''

A_MESSAGE = 'a_message'
ANOTHER_MESSAGE = 'another_message'
SOME_ANOTHER_MESSAGE = 'some_another_message'


with description('RabbitMQClient Integration tests - Consuming and publishing Direct Exchange (direct message)') as self:
    with before.each:
        self.broker_uri = environ['BROKER_URI']
        self.serializer = serializer_factory.json_serializer()
        self.pika_wrapper_client = PikaClientWrapper(pika_library=pika)
        self.logger = logger
        self.sut = RabbitMQClient(self.broker_uri,
                                  self.serializer,
                                  self.pika_wrapper_client,
                                  self.logger)

        self.sut.exchange_declare(exchange=MY_DIRECT_EXCHANGE_NAME, exchange_type=DIRECT_EXCHANGE_TYPE)
        self.sut.queue_declare(queue_name=A_QUEUE_NAME, auto_delete=False)
        self.sut.queue_bind(queue_name=A_QUEUE_NAME, exchange=MY_DIRECT_EXCHANGE_NAME, routing_key=DEFAULT_ROUTING_KEY)

    with after.each:
        self.sut.queue_unbind(queue_name=A_QUEUE_NAME, exchange=MY_DIRECT_EXCHANGE_NAME, routing_key=DEFAULT_ROUTING_KEY)
        self.sut.queue_delete(queue_name=A_QUEUE_NAME)
        self.sut.exchange_delete(exchange=MY_DIRECT_EXCHANGE_NAME)

    with context('when publishing and consuming a direct message'):
        with it('consumes the message'):
            self.sut.publish(exchange=MY_DIRECT_EXCHANGE_NAME, routing_key=DEFAULT_ROUTING_KEY, message=A_MESSAGE)

            msg = self.sut.consume(queue_name=A_QUEUE_NAME)

            expect(msg.body).to(equal(A_MESSAGE))

        with it('consumes only one message'):
            self.sut.publish(exchange=MY_DIRECT_EXCHANGE_NAME, routing_key=DEFAULT_ROUTING_KEY, message=A_MESSAGE)

            msg = self.sut.consume(queue_name=A_QUEUE_NAME)
            msg = self.sut.consume(queue_name=A_QUEUE_NAME)

            expect(msg).to(be_none)

    with context('when publishing and consuming more than one direct message'):
        with it('consumes all pending messages (manually)'):
            self.sut.publish(exchange=MY_DIRECT_EXCHANGE_NAME, routing_key=DEFAULT_ROUTING_KEY, message=A_MESSAGE)
            self.sut.publish(exchange=MY_DIRECT_EXCHANGE_NAME, routing_key=DEFAULT_ROUTING_KEY, message=ANOTHER_MESSAGE)

            first_consumed_message = self.sut.consume(queue_name=A_QUEUE_NAME)
            second_consumed_message = self.sut.consume(queue_name=A_QUEUE_NAME)
            third_consumed_message = self.sut.consume(queue_name=A_QUEUE_NAME)

            expect(first_consumed_message.body).to(equal(A_MESSAGE))
            expect(second_consumed_message.body).to(equal(ANOTHER_MESSAGE))
            expect(third_consumed_message).to(be_none)

        with it('consumes all pending messages (consuming next)'):
            self.sut.publish(exchange=MY_DIRECT_EXCHANGE_NAME, routing_key=DEFAULT_ROUTING_KEY, message=A_MESSAGE)
            self.sut.publish(exchange=MY_DIRECT_EXCHANGE_NAME, routing_key=DEFAULT_ROUTING_KEY, message=ANOTHER_MESSAGE)
            self.sut.publish(exchange=MY_DIRECT_EXCHANGE_NAME, routing_key=DEFAULT_ROUTING_KEY, message=SOME_ANOTHER_MESSAGE)

            expected_results = [A_MESSAGE, ANOTHER_MESSAGE, SOME_ANOTHER_MESSAGE]
            for counter, msg in enumerate(self.sut.consume_next(queue_name=A_QUEUE_NAME)):
                expect(msg.body).to(equal(expected_results[counter]))
                if counter == (len(expected_results) - 1):
                    break

        with it('consumes all pending messages iterating over them (consuming pending)'):
            self.sut.publish(exchange=MY_DIRECT_EXCHANGE_NAME, routing_key=DEFAULT_ROUTING_KEY, message=A_MESSAGE)
            self.sut.publish(exchange=MY_DIRECT_EXCHANGE_NAME, routing_key=DEFAULT_ROUTING_KEY, message=ANOTHER_MESSAGE)
            self.sut.publish(exchange=MY_DIRECT_EXCHANGE_NAME, routing_key=DEFAULT_ROUTING_KEY, message=SOME_ANOTHER_MESSAGE)

            expected_results = [A_MESSAGE, ANOTHER_MESSAGE, SOME_ANOTHER_MESSAGE]
            for index, msg in enumerate(self.sut.consume_pending(queue_name=A_QUEUE_NAME)):
                expect(msg.body).to(equal(expected_results[index]))
