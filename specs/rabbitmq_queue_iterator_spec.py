from mamba import description, before, context, it
from doublex import Spy, when, ANY_ARG
from expects import expect, equal, raise_error
from doublex_expects import have_been_called_with, have_been_called

from infcommon.serializer.serializers import JsonSerializer
from infcommon.logger import Logger

from infrabbitmq.rabbitmq import (
    RabbitMQQueueIterator,
    RabbitMQMessage,
)
from infrabbitmq.pika_client_wrapper import PikaClientWrapper
from infrabbitmq.exceptions import ClientWrapperError, RabbitMQError

A_QUEUE_NAME = 'a_queue_name'
TIMEOUT_IN_SECONDS = 42


with description('RabbitMQQueueIterator tests') as self:
    with before.each:
        self.pika_wrapper_client = Spy(PikaClientWrapper)
        self.serializer = Spy(JsonSerializer)
        self.logger = Spy(Logger)
        self.sut = RabbitMQQueueIterator(A_QUEUE_NAME,
                                         self.pika_wrapper_client,
                                         TIMEOUT_IN_SECONDS,
                                         self.serializer,
                                         self.logger)

    with context('getting next item'):
        # No useful test
        # with it('calls pika_wrapper_client consume_one_message'):
        #     iterator = iter(self.sut)
        #     message = next(iterator)
        #
        #     expect(self.pika_wrapper_client.consume_one_message).to(have_been_called)

        with context('when there is a message'):
            with before.each:
                self.a_serialized_body = 'a_serialized_body'
                self.serialized_message = {'body': self.a_serialized_body}
                when(self.pika_wrapper_client).consume_one_message(queue_name=A_QUEUE_NAME,
                                                                   timeout_in_seconds=TIMEOUT_IN_SECONDS).returns(self.serialized_message)

            with it('deserialize the message'):
                iterator = iter(self.sut)
                message = next(iterator)

                expect(self.serializer.loads).to(have_been_called_with(self.a_serialized_body))

            with it('returns a RabbitMQMessage with the deserialized message body'):
                a_deserialized_body = 'a_deserialized_body'
                a_deserialized_message = {'body': a_deserialized_body}
                when(self.serializer).loads(self.a_serialized_body).returns(a_deserialized_body)

                iterator = iter(self.sut)
                message = next(iterator)

                expected_rabbitmq_message = RabbitMQMessage(a_deserialized_message)
                expect(message.body).to(equal(expected_rabbitmq_message.body))

        with context('when there is NOT message'):
            with it('raises StopIteration exception'):
                def _next_when_there_is_no_message():
                    empty_serialized_message = {}
                    when(self.pika_wrapper_client).consume_one_message(queue_name=A_QUEUE_NAME,
                                                                       timeout_in_seconds=TIMEOUT_IN_SECONDS).returns(empty_serialized_message)

                    iterator = iter(self.sut)
                    _ = next(iterator)

                expect(_next_when_there_is_no_message).to(raise_error(StopIteration))

        with context('when an error arise (unhappy path)'):
            with context('when arise a pika_wrapper_client error'):
                with before.each:
                    self.a_pika_wrapper_client_error = ClientWrapperError('a_pika_wrapper_client_error_description')

                def _when_pika_wrapper_client_error_arise(self):
                    when(self.pika_wrapper_client).consume_one_message(ANY_ARG).raises(self.a_pika_wrapper_client_error)

                    iterator = iter(self.sut)
                    _ = next(iterator)

                with it('raises a RabbitMQError'):
                    expect(self._when_pika_wrapper_client_error_arise).to(raise_error(RabbitMQError))

                with it('logs the error'):
                    expect(self._when_pika_wrapper_client_error_arise).to(raise_error(RabbitMQError))

                    expected_info_message = 'Reconnecting, Error ClientWrapper {}'.format(self.a_pika_wrapper_client_error)
                    expect(self.logger.info).to(have_been_called_with(expected_info_message,
                                                                      exc_info=True))

                with it('disconnect'):
                    expect(self._when_pika_wrapper_client_error_arise).to(raise_error(RabbitMQError))

                    expect(self.pika_wrapper_client.disconnect).to(have_been_called)

            with context('when any other error arise'):
                with before.each:
                    a_serialized_message = {'body': 'a_serialized_body'}
                    when(self.pika_wrapper_client).consume_one_message(queue_name=A_QUEUE_NAME,
                                                                       timeout_in_seconds=TIMEOUT_IN_SECONDS).returns(a_serialized_message)
                    self.any_other_error = RuntimeError()
                    when(self.serializer).loads(ANY_ARG).raises(self.any_other_error)

                def _when_any_other_error_arise(self):
                    iterator = iter(self.sut)
                    _ = next(iterator)

                with it('raises the Error'):
                    expect(self._when_any_other_error_arise).to(raise_error(RuntimeError))

                with it('logs the error'):
                    expect(self._when_any_other_error_arise).to(raise_error(RuntimeError))

                    expected_critical_message = 'Error consuming from queue {} exc_type {} exc {}'.format(A_QUEUE_NAME,
                                                                                                          type(self.any_other_error),
                                                                                                          self.any_other_error)
                    expect(self.logger.critical).to(have_been_called_with(expected_critical_message,
                                                                          exc_info=True))

                with it('DOES NOT CALL ITSELF next (old code does)'):
                    expect(self._when_any_other_error_arise).to(raise_error(RuntimeError))

                    expect(self.pika_wrapper_client.consume_one_message).to(have_been_called.once)

