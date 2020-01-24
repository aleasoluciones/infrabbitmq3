from mamba import description, before, context, it
from doublex import Spy, Mock, ANY_ARG, when
from expects import expect, contain
from doublex_expects import have_been_called_with, have_been_called, have_been_satisfied

from infcommon.logger import Logger
from infrabbitmq.rabbitmq import (
    RabbitMQClient,
    RabbitMQQueueEventProcessor,
    RabbitMQMessage,
    TOPIC_EXCHANGE_TYPE,
    X_DELAYED,
)

AN_EXCHANGE = 'an_exchange'
A_QUEUE_NAME = 'a_queue_name'

A_SINGLE_TOPIC = 'a_single_topic'
A_TOPIC_LIST_WITH_A_SINGLE_TOPIC = [A_SINGLE_TOPIC]
AN_EXCHANGE_OPTIONS = {}
A_QUEUE_OPTIONS = {}
A_LIST_OF_ONE_NONE_RAW_MESSAGE = [None]


with description('RabbitMQQueueEventProcessor specs') as self:
    with before.each:
        self.event_processor = Spy()
        self.event_builder = Spy()
        self.logger = Spy(Logger)

    with context('FEATURE: connection setup'):
        with it('does every connection action in a precise order (using default options)'):
            rabbitmq_client_mock = Mock(RabbitMQClient)
            with rabbitmq_client_mock:
                rabbitmq_client_mock.disconnect()
                rabbitmq_client_mock.exchange_declare(exchange=AN_EXCHANGE,
                                                      exchange_type=TOPIC_EXCHANGE_TYPE,
                                                      durable=True,
                                                      auto_delete=False)
                rabbitmq_client_mock.queue_declare(queue_name=A_QUEUE_NAME,
                                                   durable=True,
                                                   auto_delete=False,
                                                   message_ttl=None)
                rabbitmq_client_mock.queue_bind(queue_name=A_QUEUE_NAME,
                                                exchange=AN_EXCHANGE,
                                                routing_key=A_SINGLE_TOPIC)


            sut = RabbitMQQueueEventProcessor(queue_name=A_QUEUE_NAME,
                                              event_processor=self.event_processor,
                                              rabbitmq_client=rabbitmq_client_mock,
                                              exchange=AN_EXCHANGE,
                                              list_of_topics=A_TOPIC_LIST_WITH_A_SINGLE_TOPIC,
                                              exchange_options=AN_EXCHANGE_OPTIONS,
                                              queue_options=A_QUEUE_OPTIONS,
                                              event_builder=self.event_builder,
                                              logger=self.logger,
                                              exchange_type=TOPIC_EXCHANGE_TYPE)

            sut.connection_setup()

            expect(rabbitmq_client_mock).to(have_been_satisfied)

        with context('with exchange options'):
            with it('uses options received'):
                set_durable_false = False
                set_auto_delete_true = True
                exchange_options = {'durable': set_durable_false,
                                    'auto_delete': set_auto_delete_true}
                rabbitmq_client_spy = Spy(RabbitMQClient)
                sut = RabbitMQQueueEventProcessor(queue_name=A_QUEUE_NAME,
                                                  event_processor=self.event_processor,
                                                  rabbitmq_client=rabbitmq_client_spy,
                                                  exchange=AN_EXCHANGE,
                                                  list_of_topics=A_TOPIC_LIST_WITH_A_SINGLE_TOPIC,
                                                  exchange_options=exchange_options,
                                                  queue_options=A_QUEUE_OPTIONS,
                                                  event_builder=self.event_builder,
                                                  logger=self.logger,
                                                  exchange_type=X_DELAYED)

                sut.connection_setup()

                expect(rabbitmq_client_spy.exchange_declare).to(have_been_called_with(exchange=AN_EXCHANGE,
                                                                                      exchange_type=X_DELAYED,
                                                                                      durable=set_durable_false,
                                                                                      auto_delete=set_auto_delete_true)
                                                                )

        with context('with queue options'):
            with it('uses options received'):
                set_durable_false = False
                set_auto_delete_true = True
                set_ttl_milliseconds_message = 42
                queue_options = {'durable': set_durable_false,
                                 'auto_delete': set_auto_delete_true,
                                 'message_ttl': set_ttl_milliseconds_message}
                rabbitmq_client_spy = Spy(RabbitMQClient)
                sut = RabbitMQQueueEventProcessor(queue_name=A_QUEUE_NAME,
                                                  event_processor=self.event_processor,
                                                  rabbitmq_client=rabbitmq_client_spy,
                                                  exchange=AN_EXCHANGE,
                                                  list_of_topics=A_TOPIC_LIST_WITH_A_SINGLE_TOPIC,
                                                  exchange_options=AN_EXCHANGE_OPTIONS,
                                                  queue_options=queue_options,
                                                  event_builder=self.event_builder,
                                                  logger=self.logger,
                                                  exchange_type=TOPIC_EXCHANGE_TYPE)

                sut.connection_setup()

                expect(rabbitmq_client_spy.queue_declare).to(have_been_called_with(queue_name=A_QUEUE_NAME,
                                                                                   durable=set_durable_false,
                                                                                   auto_delete=set_auto_delete_true,
                                                                                   message_ttl=set_ttl_milliseconds_message)
                                                             )

        with context('when there are several topics'):
            with it('binds the queue for each topic'):
                a_topic = 'a_topic'
                another_topic = 'another_topic'
                topics = [a_topic, another_topic]
                rabbitmq_client_spy = Spy(RabbitMQClient)
                sut = RabbitMQQueueEventProcessor(queue_name=A_QUEUE_NAME,
                                                  event_processor=self.event_processor,
                                                  rabbitmq_client=rabbitmq_client_spy,
                                                  exchange=AN_EXCHANGE,
                                                  list_of_topics=topics,
                                                  exchange_options=AN_EXCHANGE_OPTIONS,
                                                  queue_options=A_QUEUE_OPTIONS,
                                                  event_builder=self.event_builder,
                                                  logger=self.logger,
                                                  exchange_type=TOPIC_EXCHANGE_TYPE)

                sut.connection_setup()

                expect(rabbitmq_client_spy.queue_bind).to(have_been_called_with(routing_key=a_topic))
                expect(rabbitmq_client_spy.queue_bind).to(have_been_called_with(routing_key=another_topic))

    with context('FEATURE: proccess body'):
        with before.each:
            self.rabbitmq_client_spy = Spy(RabbitMQClient)
            self.sut = RabbitMQQueueEventProcessor(queue_name=A_QUEUE_NAME,
                                                   event_processor=self.event_processor,
                                                   rabbitmq_client=self.rabbitmq_client_spy,
                                                   exchange=AN_EXCHANGE,
                                                   list_of_topics=A_TOPIC_LIST_WITH_A_SINGLE_TOPIC,
                                                   exchange_options=AN_EXCHANGE_OPTIONS,
                                                   queue_options=A_QUEUE_OPTIONS,
                                                   event_builder=self.event_builder,
                                                   logger=self.logger,
                                                   exchange_type=TOPIC_EXCHANGE_TYPE)

        with context('when "max_iteration" is None'):
            with context('when there is a message'):
                with context('processing a message'):
                    with before.each:
                        a_message_body = 'a_message_body'
                        self.a_rabbitmq_message = self._build_a_rabbitmq_message(a_message_body)
                        a_list_of_one_raw_message = [self.a_rabbitmq_message]
                        when(self.rabbitmq_client_spy).consume_next(queue_name=A_QUEUE_NAME).returns(a_list_of_one_raw_message)

                    with it('calls rabbitmq_client consume next'):
                        self.sut.process_body(max_iterations=None)

                        expect(self.rabbitmq_client_spy.consume_next).to(have_been_called_with(queue_name=A_QUEUE_NAME))

                    with it('calls event_builder to build the message body to process'):
                        self.sut.process_body(max_iterations=None)

                        expect(self.event_builder.build).to(have_been_called_with(self.a_rabbitmq_message.body))

                    with it('calls event_processor.process to process the built message'):
                        when(self.event_builder).build(self.a_rabbitmq_message.body).returns('built_message')

                        self.sut.process_body(max_iterations=None)

                        expect(self.event_processor.process).to(have_been_called_with('built_message'))

                with context('when processing a message fail (unhappy path)'):
                    with it('logs the error with "critical"'):
                        a_list_of_one_raw_message = ['an_irrelevant_raw_message']
                        when(self.rabbitmq_client_spy).consume_next(queue_name=A_QUEUE_NAME).returns(a_list_of_one_raw_message)
                        when(self.event_processor).process(ANY_ARG).raises(RuntimeError)

                        self.sut.process_body(max_iterations=None)

                        expect(self.logger.critical).to(have_been_called_with(contain('Error processing from queue',
                                                                                      'raw_message',
                                                                                      'with exc_type',
                                                                                      'exc'),
                                                                              exc_info=True)
                                                        )

                    with it('continues processing messages'):
                        a_message_body = 'a_message_body'
                        self.a_rabbitmq_message = self._build_a_rabbitmq_message(a_message_body)
                        a_list_of_two_raw_message = [self.a_rabbitmq_message, self.a_rabbitmq_message]
                        when(self.rabbitmq_client_spy).consume_next(queue_name=A_QUEUE_NAME).returns(a_list_of_two_raw_message)
                        when(self.event_processor).process(ANY_ARG).raises(RuntimeError)

                        self.sut.process_body(max_iterations=2)

                        expect(self.event_processor.process).to(have_been_called.twice)

            with context('when there is NOT a message (message is None)'):
                with before.each:
                    when(self.rabbitmq_client_spy).consume_next(queue_name=A_QUEUE_NAME).returns(A_LIST_OF_ONE_NONE_RAW_MESSAGE)

                with it('calls rabbitmq_client consume next'):
                    self.sut.process_body(max_iterations=None)

                    expect(self.rabbitmq_client_spy.consume_next).to(have_been_called_with(queue_name=A_QUEUE_NAME))

                with it('does NOT call event_builder to build the message body to process'):
                    self.sut.process_body(max_iterations=None)

                    expect(self.event_builder.build).not_to(have_been_called)

                with it('does NOT call event_processor.process to process the built message'):
                    self.sut.process_body(max_iterations=None)

                    expect(self.event_processor.process).not_to(have_been_called)

        with context('when "max_iteration" is NOT None'):
            with context('when there are messages'):
                with it('processes as many messages as configure at max_iterations'):
                    a_rabbitmq_message = self._build_a_rabbitmq_message('a_message_body')
                    a_list_of_two_raw_messages = [a_rabbitmq_message, a_rabbitmq_message]
                    when(self.rabbitmq_client_spy).consume_next(queue_name=A_QUEUE_NAME).returns(a_list_of_two_raw_messages)

                    self.sut.process_body(max_iterations=1)

                    expect(self.event_builder.build).to(have_been_called.once)
                    expect(self.event_processor.process).to(have_been_called.once)

            with context('when there are NOT messages (last message is None and max_iteration reached)'):
                with it('finishes'):
                    a_list_of_two_raw_messages = [None, None]
                    when(self.rabbitmq_client_spy).consume_next(queue_name=A_QUEUE_NAME).returns(a_list_of_two_raw_messages)

                    self.sut.process_body(max_iterations=1)

                    expect(self.event_builder.build).not_to(have_been_called.once)
                    expect(self.event_processor.process).not_to(have_been_called.once)


    def _build_a_rabbitmq_message(self, message_body):
        data = {'body': message_body}
        return RabbitMQMessage(data)
