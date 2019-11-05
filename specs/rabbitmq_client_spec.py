from mamba import description, before, context, it
from doublex import Spy, when, ANY_ARG
from expects import expect, equal, be_none, raise_error, contain
from doublex_expects import have_been_called_with, anything, have_been_called

from infcommon.logger import Logger
from infcommon.serializer.serializers import JsonSerializer
from infrabbitmq.pika_client_wrapper import PikaClientWrapper
from infrabbitmq.rabbitmq import (
    RabbitMQClient,
    X_DELAYED,
    RabbitMQMessage)
from infrabbitmq.exceptions import (
    ClientWrapperError,
    RabbitMQError,
)


with description('RabbitMQClient Collaboration tests') as self:
    with before.each:
        self.broker_uri = "a_broker_uri"
        self.serializer = Spy(JsonSerializer)
        self.pika_wrapper_client = Spy(PikaClientWrapper)
        self.logger = Spy(Logger)
        self.sut = RabbitMQClient(self.broker_uri, self.serializer, self.pika_wrapper_client, self.logger)

    with context('Connecting'):
        with context('when there is NOT a connected client'):
            with it('connects (calls pika_client_wrapper connect)'):
                self.sut.connected_client

                expect(self.pika_wrapper_client.connect).to(have_been_called_with(self.broker_uri))

        with context('when there is a connected client'):
            with it('does NOT connect (do not call pika_client_wrapper connect again)'):
                self.sut.connected_client
                self.sut.connected_client

                expect(self.pika_wrapper_client.connect).to(have_been_called_with(self.broker_uri).once)

    with context('Disconnecting'):
        with it('calls pika_client_wrapper disconnect'):
            an_exchange_name = 'an_exchange_name'
            an_exchange_type = 'an_exchange_type'

            self.sut.disconnect()

            expect(self.pika_wrapper_client.disconnect).to(have_been_called)

    with context('Exchanges functionalities'):
        with context('Declaring an exchange'):
            with it('calls pika_client_wrapper exchange_declare'):
                an_exchange_name = 'an_exchange_name'
                an_exchange_type = 'an_exchange_type'

                self.sut.exchange_declare(an_exchange_name, an_exchange_type)

                expect(self.pika_wrapper_client.exchange_declare).to(have_been_called_with(exchange=an_exchange_name,
                                                                                           exchange_type=an_exchange_type)
                                                                     )

            with context('Declaring an exchange with x-delayed-message type option'):
                with it('calls pika_client_wrapper exchange_declare with x-delayed-message options (exchange type and arguments)'):
                    an_exchange_name = 'an_exchange_name'
                    x_delayed_exchange_type = X_DELAYED

                    self.sut.exchange_declare(an_exchange_name, x_delayed_exchange_type)

                    expected_arguments = {'x-delayed-type': 'topic'}
                    expect(self.pika_wrapper_client.exchange_declare).to(have_been_called_with(exchange=an_exchange_name,
                                                                                               exchange_type=x_delayed_exchange_type,
                                                                                               arguments=expected_arguments)
                                                                     )

        with context('Deleting an exchange'):
            with it('calls pika_client_wrapper exchange_delete'):
                an_exchange_name = 'an_exchange_name'

                self.sut.exchange_delete(an_exchange_name)

                expect(self.pika_wrapper_client.exchange_delete).to(have_been_called_with(exchange=an_exchange_name))

    with context('Queues functionalities'):
        with context('Declaring a queue'):
            with it('calls pika_client_wrapper queue_declare'):
                a_queue_name = 'a_queue_name'
                a_queue_autodelete = False
                a_queue_exclusive = True
                a_queue_durable = True
                self.sut.queue_declare(a_queue_name, auto_delete=a_queue_autodelete, exclusive=a_queue_exclusive, durable=a_queue_durable)

                expect(self.pika_wrapper_client.queue_declare).to(have_been_called_with(queue_name=a_queue_name,
                                                                                        auto_delete=a_queue_autodelete,
                                                                                        exclusive=a_queue_exclusive,
                                                                                        durable=a_queue_durable
                                                                                        )
                                                                  )

            with context('Declaring a queue with ttl_message option'):
                with it('calls pika_client_wrapper queue_declare with x-message-ttl option'):
                    a_queue_name = 'a_queue_name'
                    a_queue_message_ttl_milliseconds = 60000

                    self.sut.queue_declare(a_queue_name, message_ttl=a_queue_message_ttl_milliseconds)

                    expected_arguments = {'x-message-ttl': a_queue_message_ttl_milliseconds}
                    expect(self.pika_wrapper_client.queue_declare).to(have_been_called_with(queue_name=a_queue_name,
                                                                                            arguments=expected_arguments)
                                                                      )

        with context('Binding a queue'):
            with it('calls pika_client_wrapper queue_bind'):
                an_exchange_name = 'an_exchange_name'
                a_queue_name = 'a_queue_name'
                a_routing_key = 'a_routing_key'

                self.sut.queue_bind(a_queue_name, an_exchange_name, a_routing_key)

                expect(self.pika_wrapper_client.queue_bind).to(have_been_called_with(queue_name=a_queue_name,
                                                                                     exchange=an_exchange_name,
                                                                                     routing_key=a_routing_key)
                                                               )

        with context('Unbinding a queue'):
            with it('calls pika_client_wrapper queue_unbind'):
                an_exchange_name = 'an_exchange_name'
                a_queue_name = 'a_queue_name'
                a_routing_key = 'a_routing_key'

                self.sut.queue_unbind(a_queue_name, an_exchange_name, a_routing_key)

                expect(self.pika_wrapper_client.queue_unbind).to(have_been_called_with(queue_name=a_queue_name,
                                                                                       exchange=an_exchange_name,
                                                                                       routing_key=a_routing_key)
                                                                 )

        with context('Purging a queue'):
            with it('calls pika_client_wrapper queue_purge'):
                a_queue_name = 'a_queue_name'

                self.sut.queue_purge(a_queue_name)

                expect(self.pika_wrapper_client.queue_purge).to(have_been_called_with(queue_name=a_queue_name))

        with context('Deleting a queue'):
            with it('calls pika_client_wrapper queue_delete'):
                a_queue_name = 'a_queue_name'

                self.sut.queue_delete(a_queue_name)

                expect(self.pika_wrapper_client.queue_delete).to(have_been_called_with(queue_name=a_queue_name))

    with context('Publishing a message'):
        with before.each:
            self.an_exchange_name = 'an_exchange_name'
            self.a_routing_key = 'a_rounting_key'
            self.a_message = 'a_message'

        with it('serializes the message to publish'):
            a_serialized_message = 'a_serialized_message'
            when(self.serializer).dumps(self.a_message).returns(a_serialized_message)

            self.sut.publish(self.an_exchange_name, self.a_routing_key, self.a_message)

            expect(self.pika_wrapper_client.basic_publish).to(have_been_called_with(exchange=self.an_exchange_name,
                                                                                    routing_key=self.a_routing_key,
                                                                                    body=a_serialized_message)
                                                              )

        with it('calls pika_client_wrapper basic_publish'):
            self.sut.publish(self.an_exchange_name, self.a_routing_key, self.a_message)

            expect(self.pika_wrapper_client.basic_publish).to(have_been_called_with(exchange=self.an_exchange_name,
                                                                                    routing_key=self.a_routing_key,
                                                                                    body=anything)
                                                              )

    with context('Consuming one message'):
        with before.each:
            self.a_queue_name = 'a_queue_name'
            self.a_timeout_in_seconds = 1

        with it('calls pika_client_wrapper consume_one_message'):
            self.sut.consume(self.a_queue_name, self.a_timeout_in_seconds)

            expect(self.pika_wrapper_client.consume_one_message).to(have_been_called_with(queue_name=self.a_queue_name,
                                                                                          timeout_in_seconds=self.a_timeout_in_seconds))

        with context('when there is a message from rabbitmq'):
            with it('deserialize the message'):
                a_serialized_message = {'body': 'a_serialized_message'}
                when(self.pika_wrapper_client).consume_one_message(queue_name=self.a_queue_name,
                                                                   timeout_in_seconds=self.a_timeout_in_seconds).returns(a_serialized_message)

                self.sut.consume(self.a_queue_name, self.a_timeout_in_seconds)

                expect(self.serializer.loads).to(have_been_called)

            with it('returns a RabbitMQMessage with the message body'):
                a_real_body = 'a_real_body'
                a_serialized_body = 'a_serialized_body'
                a_serialized_message = {'body': a_serialized_body}
                a_deserialized_message = {'body': a_real_body}
                when(self.pika_wrapper_client).consume_one_message(queue_name=self.a_queue_name,
                                                                   timeout_in_seconds=self.a_timeout_in_seconds).returns(a_serialized_message)
                when(self.serializer).loads(a_serialized_body).returns(a_real_body)

                result = self.sut.consume(self.a_queue_name, self.a_timeout_in_seconds)

                expected_rabbitmq_message = RabbitMQMessage(a_deserialized_message)
                expect('{}'.format(result)).to(equal('{}'.format(expected_rabbitmq_message)))

        with context('when there is NOT a message from rabbitmq'):
            with it('returns None'):
                empty_serialized_message = {}
                when(self.pika_wrapper_client).consume_one_message(queue_name=self.a_queue_name,
                                                                   timeout_in_seconds=self.a_timeout_in_seconds).returns(empty_serialized_message)

                result = self.sut.consume(self.a_queue_name, self.a_timeout_in_seconds)

                expect(result).to(be_none)

        with it('calls pika_client_wrapper disconnect'):
            empty_serialized_message = {}
            when(self.pika_wrapper_client).consume_one_message(queue_name=self.a_queue_name,
                                                               timeout_in_seconds=self.a_timeout_in_seconds).returns(empty_serialized_message)

            self.sut.consume(self.a_queue_name, self.a_timeout_in_seconds)

            expect(self.pika_wrapper_client.disconnect).to(have_been_called)

    with context('Consuming messages (consume_next)'):
        with before.each:
            self.a_queue_name = 'a_queue_name'
            self.a_timeout_in_seconds = 1

        with it('calls pika_client_wrapper consume_one_message'):
            iterator_with_results = self.sut.consume_next(self.a_queue_name, self.a_timeout_in_seconds)

            total_message_to_check = 1
            for counter, message in enumerate(iterator_with_results, start=1):
                if counter == total_message_to_check:
                    break

            expect(self.pika_wrapper_client.consume_one_message).to(have_been_called_with(queue_name=self.a_queue_name,
                                                                                          timeout_in_seconds=self.a_timeout_in_seconds).once)

        with context('when there is NOT any message from rabbitmq'):
            with it('returns None'):
                empty_serialized_message = {}
                when(self.pika_wrapper_client).consume_one_message(queue_name=self.a_queue_name,
                                                                   timeout_in_seconds=self.a_timeout_in_seconds).returns(empty_serialized_message)

                iterator_with_results = self.sut.consume_next(self.a_queue_name, self.a_timeout_in_seconds)

                total_message_to_check = 1
                for counter, message in enumerate(iterator_with_results, start=1):
                    expect(message).to(be_none)
                    if counter == total_message_to_check:
                        break

        with context('when there are messages from rabbitmq'):
            with before.each:
                self.a_real_body_1 = 'a_real_body_1'
                self.a_serialized_body_1 = 'a_serialized_body_1'
                self.a_serialized_message_1 = {'body': self.a_serialized_body_1}
                self.a_deserialized_message_1 = {'body': self.a_real_body_1}
                when(self.pika_wrapper_client).consume_one_message(queue_name=self.a_queue_name,
                                                                   timeout_in_seconds=self.a_timeout_in_seconds).returns(self.a_serialized_message_1)

            with it('deserializes the message'):
                iterator_with_results = self.sut.consume_next(self.a_queue_name, self.a_timeout_in_seconds)

                total_message_to_check = 1
                for counter, message in enumerate(iterator_with_results, start=1):
                    if counter == total_message_to_check:
                        break

                expect(self.serializer.loads).to(have_been_called.once)

            with it('returns a RabbitMQMessage with the message body'):
                when(self.serializer).loads(self.a_serialized_body_1).returns(self.a_real_body_1)

                iterator_with_results = self.sut.consume_next(self.a_queue_name, self.a_timeout_in_seconds)

                total_message_to_check = 1
                for counter, message in enumerate(iterator_with_results, start=1):
                    expected_rabbitmq_message = RabbitMQMessage(self.a_deserialized_message_1)
                    expect('{}'.format(message)).to(equal('{}'.format(expected_rabbitmq_message)))
                    if counter == total_message_to_check:
                        break

    with context('Consuming messages (consume_pending)'):
        with it('See integration test->consumes all pending messages iterating over them (consuming pending)'):
            pass

    with context('Handling error (unhappy path)'):
        with context('when ClientWrapperError arise'):
            def _when_client_wrapper_error_arise(self):
                self.sut.queue_purge(queue_name='irrelevant_queue_name')

            with before.each:
                self.a_client_wrapper_error = ClientWrapperError('blabla')
                when(self.pika_wrapper_client).queue_purge(ANY_ARG).raises(self.a_client_wrapper_error)

            with it('raises a RabbitMQError'):
                expect(self._when_client_wrapper_error_arise).to(raise_error(RabbitMQError))

            with it('logs the error'):
                expect(self._when_client_wrapper_error_arise).to(raise_error(RabbitMQError))

                expected_info_message = 'Reconnecting, Error ClientWrapper {}'.format(self.a_client_wrapper_error)
                expect(self.logger.info).to(have_been_called_with(expected_info_message,
                                                                  exc_info=True))

            with it('disconnect'):
                expect(self._when_client_wrapper_error_arise).to(raise_error(RabbitMQError))

                expect(self.pika_wrapper_client.disconnect).to(have_been_called)

            with context('Exchanges functionalities'):
                with context('with exchange_declare'):
                    with it('does the same (raises a RabbitMQError, logs the error, disconnect)'):
                        def _raise_an_error():
                            when(self.pika_wrapper_client).exchange_declare(ANY_ARG).raises(self.a_client_wrapper_error)
                            self.sut.exchange_declare(exchange='irrelevant_exchange', exchange_type='irrelevant_exchange_type')

                        expect(_raise_an_error).to(raise_error(RabbitMQError))
                        expect(self.pika_wrapper_client.disconnect).to(have_been_called)
                        expected_info_message = 'Reconnecting, Error ClientWrapper {}'.format(self.a_client_wrapper_error)
                        expect(self.logger.info).to(have_been_called_with(expected_info_message, exc_info=True))

                with context('with exchange_delete'):
                    with it('does the same (raises a RabbitMQError, logs the error, disconnect)'):
                        def _raise_an_error():
                            when(self.pika_wrapper_client).exchange_delete(ANY_ARG).raises(self.a_client_wrapper_error)
                            self.sut.exchange_delete(exchange='irrelevant_exchange')

                        expect(_raise_an_error).to(raise_error(RabbitMQError))
                        expect(self.pika_wrapper_client.disconnect).to(have_been_called)
                        expected_info_message = 'Reconnecting, Error ClientWrapper {}'.format(self.a_client_wrapper_error)
                        expect(self.logger.info).to(have_been_called_with(expected_info_message, exc_info=True))

            with context('Queues functionalities'):
                with context('with queue_declare'):
                    with it('does the same (raises a RabbitMQError, logs the error, disconnect)'):
                        def _raise_an_error():
                            when(self.pika_wrapper_client).queue_declare(ANY_ARG).raises(self.a_client_wrapper_error)
                            self.sut.queue_declare(queue_name='irrelevant_queue_name')

                        expect(_raise_an_error).to(raise_error(RabbitMQError))
                        expect(self.pika_wrapper_client.disconnect).to(have_been_called)
                        expected_info_message = 'Reconnecting, Error ClientWrapper {}'.format(self.a_client_wrapper_error)
                        expect(self.logger.info).to(have_been_called_with(expected_info_message, exc_info=True))

                with context('with queue_delete'):
                    with it('does the same (raises a RabbitMQError, logs the error, disconnect)'):
                        def _raise_an_error():
                            when(self.pika_wrapper_client).queue_delete(ANY_ARG).raises(self.a_client_wrapper_error)
                            self.sut.queue_delete(queue_name='irrelevant_queue_name')

                        expect(_raise_an_error).to(raise_error(RabbitMQError))
                        expect(self.pika_wrapper_client.disconnect).to(have_been_called)
                        expected_info_message = 'Reconnecting, Error ClientWrapper {}'.format(self.a_client_wrapper_error)
                        expect(self.logger.info).to(have_been_called_with(expected_info_message, exc_info=True))

                with context('with queue_bind'):
                    with it('does the same (raises a RabbitMQError, logs the error, disconnect)'):
                        def _raise_an_error():
                            when(self.pika_wrapper_client).queue_bind(ANY_ARG).raises(self.a_client_wrapper_error)
                            self.sut.queue_bind(queue_name='irrelevant_queue_name', exchange='irrelevant_exchange')

                        expect(_raise_an_error).to(raise_error(RabbitMQError))
                        expect(self.pika_wrapper_client.disconnect).to(have_been_called)
                        expected_info_message = 'Reconnecting, Error ClientWrapper {}'.format(self.a_client_wrapper_error)
                        expect(self.logger.info).to(have_been_called_with(expected_info_message, exc_info=True))

                with context('with queue_unbind'):
                    with it('does the same (raises a RabbitMQError, logs the error, disconnect)'):
                        def _raise_an_error():
                            when(self.pika_wrapper_client).queue_unbind(ANY_ARG).raises(self.a_client_wrapper_error)
                            self.sut.queue_unbind(queue_name='irrelevant_queue_name', exchange='irrelevant_exchange')

                        expect(_raise_an_error).to(raise_error(RabbitMQError))
                        expect(self.pika_wrapper_client.disconnect).to(have_been_called)
                        expected_info_message = 'Reconnecting, Error ClientWrapper {}'.format(self.a_client_wrapper_error)
                        expect(self.logger.info).to(have_been_called_with(expected_info_message, exc_info=True))

                with context('with queue_purge'):
                    with it('does the same (raises a RabbitMQError, logs the error, disconnect)'):
                        def _raise_an_error():
                            when(self.pika_wrapper_client).queue_purge(ANY_ARG).raises(self.a_client_wrapper_error)
                            self.sut.queue_purge(queue_name='irrelevant_queue_name')

                        expect(_raise_an_error).to(raise_error(RabbitMQError))
                        expect(self.pika_wrapper_client.disconnect).to(have_been_called)
                        expected_info_message = 'Reconnecting, Error ClientWrapper {}'.format(self.a_client_wrapper_error)
                        expect(self.logger.info).to(have_been_called_with(expected_info_message, exc_info=True))

        with context('Disconnecting functionalities'):
            with context('when ClientWrapperError arise'):
                def _when_disconnecting_and_client_wrapper_arises_an_error(self):
                    self.sut.disconnect()

                with before.each:
                    self.a_client_wrapper_error = ClientWrapperError('blabla')
                    when(self.pika_wrapper_client).disconnect().raises(self.a_client_wrapper_error)

                with it('does NOT raise an error'):
                    expect(self._when_disconnecting_and_client_wrapper_arises_an_error).not_to(raise_error(RabbitMQError))
                    expect(self._when_disconnecting_and_client_wrapper_arises_an_error).not_to(raise_error(ClientWrapperError))

                with it('logs the error'):
                    self.sut.disconnect()

                    expect(self.logger.info).to(have_been_called_with(contain('disconnect fails:',
                                                                              str(self.a_client_wrapper_error),
                                                                              '{}'.format(type(self.a_client_wrapper_error))
                                                                              ),
                                                                      exc_info=True)
                                                )

                with it('sets connected client to None (crappy test)'):
                    self.sut.disconnect()

                    expect(self.sut._connected_client).to(be_none)

            with context('when any other error arise'):
                def _when_disconnecting_and_any_other_error_arise(self):
                    self.sut.disconnect()

                with before.each:
                    when(self.pika_wrapper_client).disconnect().raises(NotImplementedError)

                with it('does NOT raise an error'):
                    expect(self._when_disconnecting_and_any_other_error_arise).not_to(raise_error(RabbitMQError))
                    expect(self._when_disconnecting_and_any_other_error_arise).not_to(raise_error(ClientWrapperError))
                    expect(self._when_disconnecting_and_any_other_error_arise).not_to(raise_error(NotImplementedError))

                with it('logs the error'):
                    self.sut.disconnect()

                    expect(self.logger.info).to(have_been_called_with(contain('disconnect fails:'),
                                                                      exc_info=True)
                                                )

                with it('sets connected client to None (crappy test)'):
                    self.sut.disconnect()

                    expect(self.sut._connected_client).to(be_none)

        with context('Publishing a message functionalities'):
            with context('when ClientWrapperError arise'):
                with it('does the same (raises a RabbitMQError, logs the error, disconnect)'):
                    def _raise_a_client_wrapper_error():
                        self.a_client_wrapper_error = ClientWrapperError('blabla')
                        when(self.pika_wrapper_client).basic_publish(ANY_ARG).raises(self.a_client_wrapper_error)

                        self.sut.publish(exchange='irrelevant_exchange',
                                         routing_key='irrelevant_routing_key',
                                         message='irrelevant_message')

                    expect(_raise_a_client_wrapper_error).to(raise_error(RabbitMQError))
                    expect(self.pika_wrapper_client.disconnect).to(have_been_called)
                    expected_info_message = 'Reconnecting, Error ClientWrapper {}'.format(self.a_client_wrapper_error)
                    expect(self.logger.info).to(have_been_called_with(expected_info_message, exc_info=True))

        with context('Consuming one message functionalities'):
            with context('when ClientWrapperError arise'):
                with it('does the same (raises a RabbitMQError, logs the error, disconnect)'):
                    def _raise_a_client_wrapper_error():
                        self.a_client_wrapper_error = ClientWrapperError('blabla')
                        when(self.pika_wrapper_client).consume_one_message(ANY_ARG).raises(self.a_client_wrapper_error)

                        self.sut.consume(queue_name='irrelevant_queue_name')

                    expect(_raise_a_client_wrapper_error).to(raise_error(RabbitMQError))
                    expect(self.pika_wrapper_client.disconnect).to(have_been_called)
                    expected_info_message = 'Reconnecting, Error ClientWrapper {}'.format(self.a_client_wrapper_error)
                    expect(self.logger.info).to(have_been_called_with(expected_info_message, exc_info=True))

        with context('Consuming messages (consume_next)'):
            with context('when ClientWrapperError arise'):
                with it('does the same (raises a RabbitMQError, logs the error, disconnect)'):
                    def _raise_a_client_wrapper_error():
                        self.a_client_wrapper_error = ClientWrapperError('blabla')
                        when(self.pika_wrapper_client).consume_one_message(ANY_ARG).raises(self.a_client_wrapper_error)

                        iterator_with_results = self.sut.consume_next(queue_name='irrelevant_queue_name')

                        total_message_to_check = 1
                        for counter, message in enumerate(iterator_with_results, start=1):
                            if counter == total_message_to_check:
                                break

                    expect(_raise_a_client_wrapper_error).to(raise_error(RabbitMQError))
                    expect(self.pika_wrapper_client.disconnect).to(have_been_called)
                    expected_info_message = 'Reconnecting, Error ClientWrapper {}'.format(self.a_client_wrapper_error)
                    expect(self.logger.info).to(have_been_called_with(expected_info_message, exc_info=True))

        with context('Consuming messages (consume_pending)'):
            with context('when ClientWrapperError arise'):
                with it('does the same (raises a RabbitMQError, logs the error, disconnect)'):
                    def _raise_a_client_wrapper_error():
                        self.a_client_wrapper_error = ClientWrapperError('blabla')
                        when(self.pika_wrapper_client).consume_one_message(ANY_ARG).raises(self.a_client_wrapper_error)

                        iterator = iter(self.sut.consume_pending(queue_name='irrelevant_queue_name'))
                        _ = next(iterator)

                    expect(_raise_a_client_wrapper_error).to(raise_error(RabbitMQError))
                    expect(self.pika_wrapper_client.disconnect).to(have_been_called)
                    expected_info_message = 'Reconnecting, Error ClientWrapper {}'.format(self.a_client_wrapper_error)
                    expect(self.logger.info).to(have_been_called_with(expected_info_message, exc_info=True))
