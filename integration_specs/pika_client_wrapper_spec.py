from mamba import description, before, context, it
from doublex import Spy, when, ANY_ARG
from doublex_expects import have_been_called, have_been_called_with
from expects import expect, equal, raise_error, be_empty, be

from os import environ
from pika.adapters.blocking_connection import (
    BlockingConnection as pika_BlockingConnection,
    BlockingChannel as pika_BlockingChannel,
)
from pika.connection import URLParameters as pika_URLParameters
from pika.spec import (
    BasicProperties as pika_BasicProperties,
    Basic as pika_Basic
)
from pika import exceptions as pika_exceptions

from infrabbitmq.pika_client_wrapper import PikaClientWrapper
from infrabbitmq.exceptions import ClientWrapperError

IRRELEVANT_EXCEPTION_REPLY_CODE = 42
IRRELEVANT_EXCEPTION_REPLY_TEXT = 'irrelevant_exception_reply_text'
A_BROKER_URI_WITH_NO_QUERY_PARAMETERS = environ['BROKER_URI']
A_BROKER_URI_WITH_QUERY_PARAMETERS_AND_NO_HEARTBEAT = f"{A_BROKER_URI_WITH_NO_QUERY_PARAMETERS}?connection_attempts=3"
A_BROKER_URI_WITH_HEARTBEAT = f"{A_BROKER_URI_WITH_NO_QUERY_PARAMETERS}?heartbeat=60"


with description('PikaClientWrapper contract tests') as self:
    with before.each:
        self.broker_uri = A_BROKER_URI_WITH_NO_QUERY_PARAMETERS
        self.pika_library_spy = Spy()
        self.sut = PikaClientWrapper(self.pika_library_spy)

        self.pika_blocking_connection_spy = Spy(pika_BlockingConnection)
        self.pika_blocking_channel_spy = Spy(pika_BlockingChannel)

    with context('Connect'):
        with it('calls pika_library to create a BlockingConnection'):
            when(self.pika_library_spy).BlockingConnection(pika_URLParameters(self.broker_uri)).returns(self.pika_blocking_connection_spy)
            when(self.pika_blocking_connection_spy).channel().returns(self.pika_blocking_channel_spy)

            self.sut.connect(self.broker_uri)

            expect(self.pika_blocking_connection_spy.channel).to(have_been_called)

        with it('stores pika_blocking_channel at its attribute'):
            when(self.pika_library_spy).BlockingConnection(pika_URLParameters(self.broker_uri)).returns(self.pika_blocking_connection_spy)
            when(self.pika_blocking_connection_spy).channel().returns(self.pika_blocking_channel_spy)

            self.sut.connect(self.broker_uri)

            expect(self.sut._channel).to(be(self.pika_blocking_channel_spy))

        with it('calls pika_library channel to confirm delivery'):
            when(self.pika_library_spy).BlockingConnection(pika_URLParameters(self.broker_uri)).returns(self.pika_blocking_connection_spy)
            when(self.pika_blocking_connection_spy).channel().returns(self.pika_blocking_channel_spy)

            self.sut.connect(self.broker_uri)

            expect(self.pika_blocking_channel_spy.confirm_delivery).to(have_been_called)

        with context('handling heartbeat in broker uri'):
            with context('when broker uri has query parameters'):
                with context('and heartbeat is NOT one of them'):
                    with it('adds heartbeat to 0 on broker uri'):
                        pika_url_parameters_with_default_heartbeat = pika_URLParameters(f"{A_BROKER_URI_WITH_QUERY_PARAMETERS_AND_NO_HEARTBEAT}&heartbeat={self.sut.DEFAULT_HEARTBEAT}")
                        when(self.pika_library_spy).BlockingConnection(pika_url_parameters_with_default_heartbeat).returns(self.pika_blocking_connection_spy)
                        when(self.pika_blocking_connection_spy).channel().returns(self.pika_blocking_channel_spy)

                        self.sut.connect(A_BROKER_URI_WITH_QUERY_PARAMETERS_AND_NO_HEARTBEAT)

                        expect(self.pika_library_spy.BlockingConnection).to(have_been_called_with(pika_url_parameters_with_default_heartbeat))

                with context('and heartbeat is one of them'):
                    with it('does NOT change the broker uri'):
                        when(self.pika_library_spy).BlockingConnection(pika_URLParameters(A_BROKER_URI_WITH_HEARTBEAT)).returns(self.pika_blocking_connection_spy)
                        when(self.pika_blocking_connection_spy).channel().returns(self.pika_blocking_channel_spy)

                        self.sut.connect(A_BROKER_URI_WITH_HEARTBEAT)

                        expect(self.pika_library_spy.BlockingConnection).to(have_been_called_with(pika_URLParameters(A_BROKER_URI_WITH_HEARTBEAT)))

            with context('when broker uri has NOT query parameters'):
                with it('adds heartbeat to 0 on broker uri'):
                    pika_url_parameters_with_no_query_parameters = pika_URLParameters(f"{A_BROKER_URI_WITH_NO_QUERY_PARAMETERS}?heartbeat={self.sut.DEFAULT_HEARTBEAT}")
                    when(self.pika_library_spy).BlockingConnection(pika_url_parameters_with_no_query_parameters).returns(self.pika_blocking_connection_spy)
                    when(self.pika_blocking_connection_spy).channel().returns(self.pika_blocking_channel_spy)

                    self.sut.connect(A_BROKER_URI_WITH_NO_QUERY_PARAMETERS)

                    expect(self.pika_library_spy.BlockingConnection).to(have_been_called_with(pika_url_parameters_with_no_query_parameters))

        with context('when an error arise (unhappy path)'):
            with it('raises a ClientWrapperError'):
                def _when_blocking_connection_raises_an_error():
                    when(self.pika_library_spy).BlockingConnection(pika_URLParameters(self.broker_uri)).raises(pika_exceptions.ConnectionWrongStateError)

                    self.sut.connect(self.broker_uri)
                expect(_when_blocking_connection_raises_an_error).to(raise_error(ClientWrapperError))

    with context('Disconnect'):
        with before.each:
            when(self.pika_library_spy).BlockingConnection(pika_URLParameters(self.broker_uri)).returns(self.pika_blocking_connection_spy)
            when(self.pika_blocking_connection_spy).channel().returns(self.pika_blocking_channel_spy)
            self.sut.connect(self.broker_uri)

        with it('calls pika_library connection close'):
            self.sut.disconnect()

            expect(self.pika_blocking_connection_spy.close).to(have_been_called)

        with it('calls pika_library channel close'):
            self.sut.disconnect()

            expect(self.pika_blocking_channel_spy.close).to(have_been_called)

        with context('when an error arise (unhappy path)'):
            with it('raises a ClientWrapperError'):
                def _when_closing_a_channel_raises_an_error():
                    when(self.pika_blocking_channel_spy).close().raises(ValueError)

                    self.sut.disconnect()

                expect(_when_closing_a_channel_raises_an_error).to(raise_error(ClientWrapperError))

    with context('Exchanges functionalities'):
        with before.each:
            when(self.pika_library_spy).BlockingConnection(pika_URLParameters(self.broker_uri)).returns(self.pika_blocking_connection_spy)
            when(self.pika_blocking_connection_spy).channel().returns(self.pika_blocking_channel_spy)
            self.sut.connect(self.broker_uri)

        with context('exchange declare'):
            with it('calls pika_library channel exchange_declare'):
                an_exchange = 'an_exchange'
                an_exchange_type = 'an_exchange_type'
                an_exchange_passive_param = 'an_exchange_passive'
                an_exchange_durable_param = 'an_exchange_durable_param'
                an_exchange_auto_delete_param = 'an_exchange_auto_delete_param'
                an_exchange_internal_param = 'an_exchange_internal_param'
                an_exchange_arguments_param = {'k': 'v'}
                parameters = {'passive': an_exchange_passive_param,
                              'durable': an_exchange_durable_param,
                              'auto_delete': an_exchange_auto_delete_param,
                              'internal': an_exchange_internal_param,
                              'arguments': an_exchange_arguments_param}

                self.sut.exchange_declare(an_exchange, an_exchange_type, **parameters)

                expect(self.pika_blocking_channel_spy.exchange_declare).to(have_been_called_with(exchange=an_exchange,
                                                                                                 exchange_type=an_exchange_type,
                                                                                                 passive=an_exchange_passive_param,
                                                                                                 durable=an_exchange_durable_param,
                                                                                                 auto_delete=an_exchange_auto_delete_param,
                                                                                                 internal=an_exchange_internal_param,
                                                                                                 arguments=an_exchange_arguments_param))

            with context('when an error arise (unhappy path)'):
                with it('raises a ClientWrapperError'):
                    def _when_declare_exchange_raises_an_error():
                        a_channel_closed_error = pika_exceptions.ChannelClosed(reply_code=IRRELEVANT_EXCEPTION_REPLY_CODE,
                                                                               reply_text=IRRELEVANT_EXCEPTION_REPLY_TEXT)
                        when(self.pika_blocking_channel_spy).exchange_declare(ANY_ARG).raises(a_channel_closed_error)

                        self.sut.exchange_declare('irrelevant_exchange', 'irrelevant_exchange_type')

                    expect(_when_declare_exchange_raises_an_error).to(raise_error(ClientWrapperError))

        with context('exchange delete'):
            with it('calls pika_library channel exchange_delete'):
                an_exchange = 'an_exchange'

                self.sut.exchange_delete(an_exchange)

                expect(self.pika_blocking_channel_spy.exchange_delete).to(have_been_called_with(exchange=an_exchange))

            with context('when an error arise (unhappy path)'):
                with it('raises a ClientWrapperError'):
                    def _when_exchange_delete_raises_an_error():
                        a_channel_closed_error = pika_exceptions.ChannelClosed(reply_code=IRRELEVANT_EXCEPTION_REPLY_CODE,
                                                                               reply_text=IRRELEVANT_EXCEPTION_REPLY_TEXT)
                        when(self.pika_blocking_channel_spy).exchange_delete(ANY_ARG).raises(a_channel_closed_error)

                        self.sut.exchange_delete('irrelevant_exchange')

                    expect(_when_exchange_delete_raises_an_error).to(raise_error(ClientWrapperError))

    with context('Queues functionalities'):
        with before.each:
            when(self.pika_library_spy).BlockingConnection(pika_URLParameters(self.broker_uri)).returns(self.pika_blocking_connection_spy)
            when(self.pika_blocking_connection_spy).channel().returns(self.pika_blocking_channel_spy)
            self.sut.connect(self.broker_uri)

        with context('Declaring a queue'):
            with it('calls pika_library channel queue_declare'):
                a_queue_name = 'a_queue_name'
                a_queue_durable_param = 'a_queue_durable_param'
                a_queue_exclusive_param = 'a_queue_exclusive_param'
                a_queue_auto_delete_param = 'a_queue_auto_delete_param'
                a_queue_arguments_param = {'k': 'v'}

                self.sut.queue_declare(queue_name=a_queue_name,
                                       durable=a_queue_durable_param,
                                       exclusive=a_queue_exclusive_param,
                                       auto_delete=a_queue_auto_delete_param,
                                       arguments=a_queue_arguments_param)

                expect(self.pika_blocking_channel_spy.queue_declare).to(have_been_called_with(a_queue_name,
                                                                                              durable=a_queue_durable_param,
                                                                                              exclusive=a_queue_exclusive_param,
                                                                                              auto_delete=a_queue_auto_delete_param,
                                                                                              arguments=a_queue_arguments_param))
            with context('when an error arise (unhappy path)'):
                with it('raises a ClientWrapperError'):
                    def _when_queue_declare_raises_an_error():
                        a_channel_closed_error = pika_exceptions.ChannelClosed(reply_code=IRRELEVANT_EXCEPTION_REPLY_CODE,
                                                                               reply_text=IRRELEVANT_EXCEPTION_REPLY_TEXT)
                        when(self.pika_blocking_channel_spy).queue_declare(ANY_ARG).raises(a_channel_closed_error)

                        self.sut.queue_declare(queue_name='irrelevant_queue_name')

                    expect(_when_queue_declare_raises_an_error).to(raise_error(ClientWrapperError))

        with context('Binding a queue'):
            with it('calls pika_library channel queue_bind'):
                an_exchange = 'an_exchange'
                a_queue_name = 'a_queue_name'
                a_routing_key = 'a_routing_key'

                self.sut.queue_bind(queue_name=a_queue_name, exchange=an_exchange, routing_key=a_routing_key)

                expect(self.pika_blocking_channel_spy.queue_bind).to(have_been_called_with(queue=a_queue_name,
                                                                                           exchange=an_exchange,
                                                                                           routing_key=a_routing_key))

            with context('when an error arise (unhappy path)'):
                with it('raises a ClientWrapperError'):
                    def _when_queue_bind_raises_an_error():
                        a_channel_closed_error = pika_exceptions.ChannelClosed(reply_code=IRRELEVANT_EXCEPTION_REPLY_CODE,
                                                                               reply_text=IRRELEVANT_EXCEPTION_REPLY_TEXT)
                        when(self.pika_blocking_channel_spy).queue_bind(ANY_ARG).raises(a_channel_closed_error)

                        self.sut.queue_bind(queue_name='irrelevant_queue_name', exchange='irrelevant_exchange')

                    expect(_when_queue_bind_raises_an_error).to(raise_error(ClientWrapperError))

        with context('Unbinding a queue'):
            with it('calls pika_library channel queue_unbind'):
                an_exchange = 'an_exchange'
                a_queue_name = 'a_queue_name'
                a_routing_key = 'a_routing_key'

                self.sut.queue_unbind(queue_name=a_queue_name, exchange=an_exchange, routing_key=a_routing_key)

                expect(self.pika_blocking_channel_spy.queue_unbind).to(have_been_called_with(queue=a_queue_name,
                                                                                             exchange=an_exchange,
                                                                                             routing_key=a_routing_key))
            with context('when an error arise (unhappy path)'):
                with it('raises a ClientWrapperError'):
                    def _when_queue_unbind_raises_an_error():
                        a_channel_closed_error = pika_exceptions.ChannelClosed(reply_code=IRRELEVANT_EXCEPTION_REPLY_CODE,
                                                                               reply_text=IRRELEVANT_EXCEPTION_REPLY_TEXT)
                        when(self.pika_blocking_channel_spy).queue_unbind(ANY_ARG).raises(a_channel_closed_error)

                        self.sut.queue_unbind(queue_name='irrelevant_queue_name', exchange='irrelevant_exchange')

                    expect(_when_queue_unbind_raises_an_error).to(raise_error(ClientWrapperError))

        with context('Purging a queue'):
            with it('calls pika_library channel queue_purge'):
                a_queue_name = 'a_queue_name'

                self.sut.queue_purge(queue_name=a_queue_name)

                expect(self.pika_blocking_channel_spy.queue_purge).to(have_been_called_with(queue=a_queue_name))

            with context('when an error arise (unhappy path)'):
                with it('raises a ClientWrapperError'):
                    def _when_queue_purge_raises_an_error():
                        a_channel_closed_error = pika_exceptions.ChannelClosed(reply_code=IRRELEVANT_EXCEPTION_REPLY_CODE,
                                                                               reply_text=IRRELEVANT_EXCEPTION_REPLY_TEXT)
                        when(self.pika_blocking_channel_spy).queue_purge(ANY_ARG).raises(a_channel_closed_error)

                        self.sut.queue_purge(queue_name='irrelevant_queue_name')

                    expect(_when_queue_purge_raises_an_error).to(raise_error(ClientWrapperError))

        with context('Deleting a queue'):
            with it('calls pika_library channel queue_delete'):
                a_queue_name = 'a_queue_name'

                self.sut.queue_delete(queue_name=a_queue_name)

                expect(self.pika_blocking_channel_spy.queue_delete).to(have_been_called_with(queue=a_queue_name))

            with context('when an error arise (unhappy path)'):
                with it('raises a ClientWrapperError'):
                    def _when_queue_delete_raises_an_error():
                        a_channel_closed_error = pika_exceptions.ChannelClosed(reply_code=IRRELEVANT_EXCEPTION_REPLY_CODE,
                                                                               reply_text=IRRELEVANT_EXCEPTION_REPLY_TEXT)
                        when(self.pika_blocking_channel_spy).queue_delete(ANY_ARG).raises(a_channel_closed_error)

                        self.sut.queue_delete(queue_name='irrelevant_queue_name')

                    expect(_when_queue_delete_raises_an_error).to(raise_error(ClientWrapperError))

    with context('Publishing a message'):
        with before.each:
            when(self.pika_library_spy).BlockingConnection(pika_URLParameters(self.broker_uri)).returns(self.pika_blocking_connection_spy)
            when(self.pika_blocking_connection_spy).channel().returns(self.pika_blocking_channel_spy)
            self.sut.connect(self.broker_uri)

        with context('happy path'):
            with before.each:
                self.an_exchange = 'an_exchange'
                self.a_routing_key = 'a_routing_key'
                self.a_body = 'a_body'

            with it('calls pika_library channel basic_publish and "mandatory=False"'):
                self.sut.basic_publish(exchange=self.an_exchange,
                                       routing_key=self.a_routing_key,
                                       body=self.a_body)

                expected_properties = pika_BasicProperties()
                expect(self.pika_blocking_channel_spy.basic_publish).to(have_been_called_with(exchange=self.an_exchange,
                                                                                              routing_key=self.a_routing_key,
                                                                                              body=self.a_body,
                                                                                              mandatory=False,
                                                                                              properties=expected_properties)
                                                                        )

            with context('when publish has additional options (i.e. headers)'):
                with context('when ttl (i.e. expiration) is present'):
                    with it('calls pika_library channel basic_publish with the correct properties (as parameter)'):
                        ttl_milliseconds_string_value = '42'
                        publish_other_parameters = {'headers': {'expiration': ttl_milliseconds_string_value}}

                        self.sut.basic_publish(exchange=self.an_exchange,
                                               routing_key=self.a_routing_key,
                                               body=self.a_body,
                                               **publish_other_parameters)

                        expected_properties = pika_BasicProperties(expiration=ttl_milliseconds_string_value)
                        expect(self.pika_blocking_channel_spy.basic_publish).to(have_been_called_with(exchange=self.an_exchange,
                                                                                                      routing_key=self.a_routing_key,
                                                                                                      body=self.a_body,
                                                                                                      mandatory=False,
                                                                                                      properties=expected_properties)
                                                                                )

                with context('when delay (i.e. x-delay) is present'):
                    with it('calls pika_library channel basic_publish with the correct properties (as header)'):
                        delayed_milliseconds_string_value = '4242'
                        publish_other_parameters = {'headers': {'x-delay': delayed_milliseconds_string_value}}

                        self.sut.basic_publish(exchange=self.an_exchange,
                                               routing_key=self.a_routing_key,
                                               body=self.a_body,
                                               **publish_other_parameters)

                        expected_properties = pika_BasicProperties(headers=publish_other_parameters['headers'])
                        expect(self.pika_blocking_channel_spy.basic_publish).to(have_been_called_with(exchange=self.an_exchange,
                                                                                                      routing_key=self.a_routing_key,
                                                                                                      body=self.a_body,
                                                                                                      mandatory=False,
                                                                                                      properties=expected_properties)
                                                                                )

        with context('when an error arise (unhappy path)'):
            with it('raises a ClientWrapperError'):
                def _when_basic_publish_raises_an_error():
                    an_unroutable_error = pika_exceptions.UnroutableError('unroutable message')
                    when(self.pika_blocking_channel_spy).basic_publish(ANY_ARG).raises(an_unroutable_error)

                    self.sut.basic_publish(exchange='irrelevant_exchange', routing_key='irrelevant_routing_key', body='irrelevant_body')

                expect(_when_basic_publish_raises_an_error).to(raise_error(ClientWrapperError))

    with context('Consuming one message'):
        with before.each:
            when(self.pika_library_spy).BlockingConnection(pika_URLParameters(self.broker_uri)).returns(self.pika_blocking_connection_spy)
            when(self.pika_blocking_connection_spy).channel().returns(self.pika_blocking_channel_spy)
            self.sut.connect(self.broker_uri)

        with context('happy path'):
            with it('calls pika_library channel basic_get (irrelevant test, only for documentation)'):
                a_queue_name = 'a_queue_name'
                timeout_in_seconds = 1
                consume_result_from_pika_library = (None, None, None)
                when(self.pika_blocking_channel_spy).basic_get(a_queue_name).returns(consume_result_from_pika_library)

                self.sut.consume_one_message(queue_name=a_queue_name, timeout_in_seconds=timeout_in_seconds)

                expect(self.pika_blocking_channel_spy.basic_get).to(have_been_called_with(a_queue_name))

            with context('when there is a message'):
                with before.each:
                    self.a_queue_name = 'a_queue_name'
                    self.timeout_in_seconds = 12
                    self.a_delivery_tag = 'a_delivery_tag'
                    self.a_message_body = 'a_body_message'
                    consume_result_from_pika_library = (pika_Basic.Deliver(delivery_tag=self.a_delivery_tag),
                                                        pika_BasicProperties(),
                                                        self.a_message_body)

                    when(self.pika_blocking_channel_spy).basic_get(self.a_queue_name).returns(consume_result_from_pika_library)

                with it('calls pika_library channel basic_ack'):
                    self.sut.consume_one_message(queue_name=self.a_queue_name, timeout_in_seconds=self.timeout_in_seconds)

                    expect(self.pika_blocking_channel_spy.basic_ack).to(have_been_called_with(self.a_delivery_tag))

                with it('returns a dict with key "body" and the message body as value'):
                    result = self.sut.consume_one_message(queue_name=self.a_queue_name, timeout_in_seconds=self.timeout_in_seconds)

                    expected_result = {'body': self.a_message_body}
                    expect(result).to(equal(expected_result))

            with context('when there is NOT a message'):
                with before.each:
                    self.a_queue_name = 'a_queue_name'
                    self.timeout_in_seconds = 12
                    consume_result_from_pika_library = (None, None, None)
                    when(self.pika_blocking_channel_spy).basic_get(self.a_queue_name).returns(consume_result_from_pika_library)

                with it('returns an empty dict'):
                    result = self.sut.consume_one_message(queue_name=self.a_queue_name, timeout_in_seconds=self.timeout_in_seconds)

                    expect(result).to(be_empty)

        with context('when an error arise (unhappy path)'):
            with context('when pika_library channel closed error arise'):
                with it('raises a ClientWrapperError'):
                    def _when_consume_arise_a_channel_closed_error():
                        a_channel_closed_error = pika_exceptions.ChannelClosed(reply_code=IRRELEVANT_EXCEPTION_REPLY_CODE,
                                                                               reply_text=IRRELEVANT_EXCEPTION_REPLY_TEXT)

                        when(self.pika_blocking_channel_spy).basic_get(ANY_ARG).raises(a_channel_closed_error)

                        self.sut.consume_one_message(queue_name='irrelevant_queue_name', timeout_in_seconds=42)

                    expect(_when_consume_arise_a_channel_closed_error).to(raise_error(ClientWrapperError))

            with context('when value error arise'):
                with it('raises a ClientWrapperError'):
                    def _when_consume_arise_a_value_error():
                        when(self.pika_blocking_channel_spy).basic_get(ANY_ARG).raises(ValueError)

                        self.sut.consume_one_message(queue_name='irrelevant_queue_name', timeout_in_seconds=42)

                    expect(_when_consume_arise_a_value_error).to(raise_error(ClientWrapperError))
