from mamba import description, before, context, it, after
from expects import expect, raise_error

from os import (
    environ,
    getpid,
)
import pika

from infcommon import logger
from infcommon.serializer import factory as serializer_factory

from infrabbitmq.rabbitmq import RabbitMQClient
from infrabbitmq.pika_client_wrapper import PikaClientWrapper
from infrabbitmq import factory

A_QUEUE_NAME = f'a_queue_recreate_{getpid()}'


with description('RabbitMQClient Integration tests - Queue declare recreates queue when arguments change') as self:
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

    with after.each:
        try:
            self.sut.queue_delete(queue_name=A_QUEUE_NAME)
        except Exception:
            pass

    with context('when declaring a queue that already exists with different arguments'):
        with it('recreates the queue with the new arguments'):
            self.sut.queue_declare(queue_name=A_QUEUE_NAME, auto_delete=False, durable=True)

            def _redeclare_with_different_args():
                self.sut.queue_declare(queue_name=A_QUEUE_NAME, auto_delete=False, durable=True, max_length=100)

            expect(_redeclare_with_different_args).not_to(raise_error)

    with context('when declaring a queue that already exists with the same arguments'):
        with it('does not fail'):
            self.sut.queue_declare(queue_name=A_QUEUE_NAME, auto_delete=False, durable=True, max_length=50)

            def _redeclare_with_same_args():
                self.sut.queue_declare(queue_name=A_QUEUE_NAME, auto_delete=False, durable=True, max_length=50)

            expect(_redeclare_with_same_args).not_to(raise_error)
