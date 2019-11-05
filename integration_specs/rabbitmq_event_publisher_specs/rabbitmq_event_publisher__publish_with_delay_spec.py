from mamba import description, before, context, it, after
from doublex import Spy
from doublex_expects import have_been_called_with, have_been_called
from expects import expect, have_keys, be_a, have_len, be_above_or_equal

from os import getpid
from time import sleep

from infcommon import logger
from infrabbitmq import factory
from infrabbitmq.rabbitmq import (
    RabbitMQQueueEventProcessor,
    X_DELAYED,
)

# --------------------------------------------------
# Avoid pika logging
factory.configure_pika_logger_to_error()
# --------------------------------------------------


A_TOPIC_EXCHANGE_NAME = 'a_topic_exchange_name'
A_QUEUE_NAME = 'a_queue_name_{}'.format(getpid())
A_LIST_OF_TOPICS = '#'
A_NETWORK = 'a_network'
AN_EVENT_NAME = 'an_event_name'
AN_EVENT_DATA = 'an_event_data'


with description('RabbitMQEventPublisher integration test: Feature publish_with_delay') as self:
    with before.each:
        self.sut_event_publisher = factory.rabbitmq_event_publisher(exchange=A_TOPIC_EXCHANGE_NAME)
        self.rabbitmq_client = factory.no_singleton_rabbitmq_client()
        self.event_processor = Spy()
        self.event_builder = factory.raw_event_builder()
        self.logger = logger
        self.sut_event_processor = RabbitMQQueueEventProcessor(queue_name=A_QUEUE_NAME,
                                                               event_processor=self.event_processor,
                                                               rabbitmq_client=self.rabbitmq_client,
                                                               exchange=A_TOPIC_EXCHANGE_NAME,
                                                               list_of_topics=A_LIST_OF_TOPICS,
                                                               event_builder=self.event_builder,
                                                               logger=self.logger,
                                                               exchange_type=X_DELAYED,
                                                               queue_options={},
                                                               exchange_options={}
                                                               )

    with after.each:
        self.rabbitmq_client.queue_unbind(queue_name=A_QUEUE_NAME,
                                          exchange=A_TOPIC_EXCHANGE_NAME,
                                          routing_key=A_LIST_OF_TOPICS[0])
        self.rabbitmq_client.queue_delete(queue_name=A_QUEUE_NAME)
        self.rabbitmq_client.exchange_delete(exchange=A_TOPIC_EXCHANGE_NAME)

    with context('processing a delayed event'):
        with context('when delayed time is NOT zero'):
            with it('calls the processor with event object data after delayed time'):
                delay_milliseconds = 2000

                self.sut_event_publisher.publish_with_delay(AN_EVENT_NAME, A_NETWORK, delay_milliseconds, data=AN_EVENT_DATA)
                self.sut_event_processor.process_body(max_iterations=1)

                expect(self.event_processor.process).not_to(have_been_called)

                sleep(2)
                self.sut_event_processor.process_body(max_iterations=1)

                expect(self.event_processor.process).to(have_been_called_with(have_keys(name=AN_EVENT_NAME,
                                                                                        network=A_NETWORK,
                                                                                        data=AN_EVENT_DATA,
                                                                                        timestamp=be_a(float),
                                                                                        timestamp_str=have_len(be_above_or_equal(1))
                                                                                        )
                                                                              ).once
                                                        )

        with context('when delayed time is zero'):
            with it('calls the processor with event object immediately'):
                delay_milliseconds = 0

                self.sut_event_publisher.publish_with_delay(AN_EVENT_NAME, A_NETWORK, delay_milliseconds, data=AN_EVENT_DATA)
                self.sut_event_processor.process_body(max_iterations=1)

                expect(self.event_processor.process).to(have_been_called_with(have_keys(name=AN_EVENT_NAME,
                                                                                        network=A_NETWORK,
                                                                                        data=AN_EVENT_DATA,
                                                                                        timestamp=be_a(float),
                                                                                        timestamp_str=have_len(be_above_or_equal(1))
                                                                                        )
                                                                              ).once
                                                        )
