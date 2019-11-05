from mamba import description, before, context, it, after
from doublex import Spy
from doublex_expects import have_been_called_with
from expects import expect, have_keys, be_a, have_len, be_above_or_equal

from os import getpid

from infcommon import logger
from infcommon import clock
from infrabbitmq import factory
from infrabbitmq.rabbitmq import (
    RabbitMQQueueEventProcessor,
    TOPIC_EXCHANGE_TYPE,
)

# --------------------------------------------------
# Avoid pika logging
factory.configure_pika_logger_to_error()
# --------------------------------------------------


A_TOPIC_EXCHANGE_NAME = 'a_topic_exchange_name'
A_QUEUE_NAME = 'my_queue_name_{}'.format(getpid())
A_LIST_OF_TOPICS = ['#']
A_NETWORK = 'a_network'
A_TOPIC = 'a_topic'
A_DATA = {'foo': 'bar'}


class FakeEvent:
    def __init__(self, topic, network, data, timestamp=None, timestamp_str=None):
        self._data = data
        self._network = network
        self._topic = topic
        self.timestamp = timestamp
        self.timestamp_str = timestamp_str

    @property
    def data(self):
        return self._data

    @property
    def network(self):
        return self._network

    @property
    def topic(self):
        return self._topic


with description('RabbitMQEventPublisher integration test: Feature publish_event_object') as self:
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
                                                               exchange_type=TOPIC_EXCHANGE_TYPE,
                                                               queue_options={},
                                                               exchange_options={}
                                                               )

    with after.each:
        self.rabbitmq_client.queue_unbind(queue_name=A_QUEUE_NAME,
                                          exchange=A_TOPIC_EXCHANGE_NAME,
                                          routing_key=A_LIST_OF_TOPICS[0])
        self.rabbitmq_client.queue_delete(queue_name=A_QUEUE_NAME)
        self.rabbitmq_client.exchange_delete(exchange=A_TOPIC_EXCHANGE_NAME)

    with context('publishing and processing an event'):
        with it('calls the processor with event object data'):
            event = FakeEvent(A_TOPIC, A_NETWORK, A_DATA, timestamp=None, timestamp_str=None)

            self.sut_event_publisher.publish_event_object(event)
            self.sut_event_publisher.publish_event_object(event)
            self.sut_event_processor.process_body(max_iterations=1)

            expect(self.event_processor.process).to(have_been_called_with(have_keys(_network=A_NETWORK,
                                                                                    _data=A_DATA,
                                                                                    _topic=A_TOPIC,
                                                                                    )
                                                                          ).once
                                                    )

        with context('when timestamp is NOT set in the event'):
            with it('sets timestamp and timestamp_str'):
                event = FakeEvent(A_TOPIC, A_NETWORK, A_DATA, timestamp=None, timestamp_str=None)

                self.sut_event_publisher.publish_event_object(event)
                self.sut_event_publisher.publish_event_object(event)
                self.sut_event_processor.process_body(max_iterations=1)

                expect(self.event_processor.process).to(have_been_called_with(have_keys(_network=A_NETWORK,
                                                                                        _data=A_DATA,
                                                                                        _topic=A_TOPIC,
                                                                                        timestamp=be_a(float),
                                                                                        timestamp_str=have_len(be_above_or_equal(1))
                                                                                        )
                                                                              ).once
                                                        )

        with context('when timestamp is set in the event'):
            with it('does not set timestamp and timestamp_str'):
                clock_service = clock.Clock()
                now = clock_service.utcnow()
                now_timestamp = clock_service.timestamp(now)
                now_timestamp_str = str(now_timestamp)
                event = FakeEvent(A_TOPIC, A_NETWORK, A_DATA, timestamp=now_timestamp, timestamp_str=now_timestamp_str)

                self.sut_event_publisher.publish_event_object(event)
                self.sut_event_publisher.publish_event_object(event)
                self.sut_event_processor.process_body(max_iterations=1)

                expect(self.event_processor.process).to(have_been_called_with(have_keys(_network=A_NETWORK,
                                                                                        _data=A_DATA,
                                                                                        _topic=A_TOPIC,
                                                                                        timestamp=now_timestamp,
                                                                                        timestamp_str=now_timestamp_str
                                                                                        )
                                                                              ).once
                                                        )
