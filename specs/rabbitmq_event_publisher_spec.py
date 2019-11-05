from mamba import description, before, context, it
from doublex import Spy
from expects import expect, have_properties, have_len, be_above_or_equal, be_a, have_key
from doublex_expects import have_been_called_with

from infcommon.clock import Clock
from infrabbitmq.events import Event
from infrabbitmq.rabbitmq import (
    RabbitMQClient,
    RabbitMQEventPublisher,
    TOPIC_EXCHANGE_TYPE,
    X_DELAYED,
)

AN_EXCHANGE = 'an_exchange'
AN_EVENT_NAME = 'an_event_name'
A_NETWORK = 'a_network'
A_TOPIC_PREFIX = 'a_topic_prefix'
A_TTL_MILLISECONDS_IN_NUMBER = 42
A_DELAY_MILLISECONDS_IN_NUMBER = 42


with description('RabbitMQEventPublisher tests') as self:
    with before.each:
        self.rabbitmq_client = Spy(RabbitMQClient)
        self.clock_service = Clock()
        self.sut = RabbitMQEventPublisher(rabbitmq_client=self.rabbitmq_client, clock_service=self.clock_service, exchange=AN_EXCHANGE)

    with context('Publishing with: publish, publish_event_object and publish_with_ttl'):
        with it('calls rabbitmq_client exchange_declare with exchange, exchange_type and durable attributes'):
            an_event = Event(AN_EVENT_NAME, network=A_NETWORK, topic_prefix=A_TOPIC_PREFIX, data=None)

            self.sut.publish(AN_EVENT_NAME, A_NETWORK)
            self.sut.publish_event_object(an_event)
            self.sut.publish_with_ttl(AN_EVENT_NAME, A_NETWORK, A_TTL_MILLISECONDS_IN_NUMBER)

            expect(self.rabbitmq_client.exchange_declare).to(have_been_called_with(exchange=AN_EXCHANGE,
                                                                                   exchange_type=TOPIC_EXCHANGE_TYPE,
                                                                                   durable=True).exactly(3))

    with context('Publishing with: publish, publish_event_object, publish_with_ttl and publish_with_delay'):
        with it('calls rabbitmq_client publish with the specific routing_key'):
            an_event = Event(AN_EVENT_NAME, network=A_NETWORK, topic_prefix=A_TOPIC_PREFIX, data=None)

            self.sut.publish(AN_EVENT_NAME, A_NETWORK, topic_prefix=A_TOPIC_PREFIX)
            self.sut.publish_event_object(an_event)
            self.sut.publish_with_ttl(AN_EVENT_NAME, A_NETWORK, A_TTL_MILLISECONDS_IN_NUMBER, topic_prefix=A_TOPIC_PREFIX)
            self.sut.publish_with_delay(AN_EVENT_NAME, A_NETWORK, delay_milliseconds=0, topic_prefix=A_TOPIC_PREFIX)

            expect(self.rabbitmq_client.publish).to(have_been_called_with(exchange=AN_EXCHANGE,
                                                                          routing_key=an_event.topic).exactly(4))

    with context('Feature: publish'):
        with it('calls rabbitmq_client publish to publish an Event entity as message (with the timestamp and timestamp_str)'):
            a_data = 'a_data'
            an_id = 'an_id'

            self.sut.publish(AN_EVENT_NAME, A_NETWORK, data=a_data, id=an_id, topic_prefix=A_TOPIC_PREFIX)

            expect(self.rabbitmq_client.publish).to(have_been_called_with(message=have_properties(name=AN_EVENT_NAME,
                                                                                                  network=A_NETWORK,
                                                                                                  data=a_data,
                                                                                                  topic_prefix=A_TOPIC_PREFIX,
                                                                                                  id=an_id,
                                                                                                  timestamp=be_a(float),
                                                                                                  timestamp_str=have_len(be_above_or_equal(1))
                                                                                                  )
                                                                          )
                                                    )

    with context('Feature: publish_event_object'):
        with context('when the event has NOT timestamp and/or timestamp_str'):
            with it('publishes the event with the actual timestamp and timestamp_str'):
                a_topic_prefix = 'a_topic_prefix'
                a_data = 'a_data'
                an_id = 'an_id'
                an_event_without_timestamp = Event(AN_EVENT_NAME,
                                                   network=A_NETWORK,
                                                   data=a_data,
                                                   topic_prefix=A_TOPIC_PREFIX,
                                                   id=an_id,
                                                   timestamp=None,
                                                   timestamp_str=None)

                self.sut.publish_event_object(an_event_without_timestamp)

                expect(self.rabbitmq_client.publish).to(have_been_called_with(message=have_properties(name=AN_EVENT_NAME,
                                                                                                      network=A_NETWORK,
                                                                                                      data=a_data,
                                                                                                      topic_prefix=A_TOPIC_PREFIX,
                                                                                                      id=an_id,
                                                                                                      timestamp=be_a(float),
                                                                                                      timestamp_str=have_len(be_above_or_equal(1))
                                                                                                      )
                                                                              )
                                                        )

        with context('when the event has timestamp and/or timestamp_str'):
            with it('publishes the event with the received timestamp and timestamp_str'):
                a_data = 'a_data'
                an_id = 'an_id'
                a_timestamp = 'a_timestamp'
                a_timestamp_str = 'a_timestamp_str'
                an_event_with_timestamp = Event(AN_EVENT_NAME,
                                                network=A_NETWORK,
                                                data=a_data,
                                                topic_prefix=A_TOPIC_PREFIX,
                                                id=an_id,
                                                timestamp=a_timestamp,
                                                timestamp_str=a_timestamp_str)

                self.sut.publish_event_object(an_event_with_timestamp)

                expect(self.rabbitmq_client.publish).to(have_been_called_with(message=have_properties(name=AN_EVENT_NAME,
                                                                                                      network=A_NETWORK,
                                                                                                      data=a_data,
                                                                                                      topic_prefix=A_TOPIC_PREFIX,
                                                                                                      id=an_id,
                                                                                                      timestamp=a_timestamp,
                                                                                                      timestamp_str=a_timestamp_str)
                                                                              )
                                                        )

    with context('Feature: publish_with_ttl'):
        with it('calls rabbitmq_client publish to publish an Event entity as message (with the timestamp and timestamp_str)'):
            a_data = 'a_data'
            an_id = 'an_id'

            self.sut.publish_with_ttl(AN_EVENT_NAME, A_NETWORK, A_TTL_MILLISECONDS_IN_NUMBER, data=a_data, id=an_id, topic_prefix=A_TOPIC_PREFIX)

            expect(self.rabbitmq_client.publish).to(have_been_called_with(message=have_properties(name=AN_EVENT_NAME,
                                                                                                  network=A_NETWORK,
                                                                                                  data=a_data,
                                                                                                  topic_prefix=A_TOPIC_PREFIX,
                                                                                                  id=an_id,
                                                                                                  timestamp=be_a(float),
                                                                                                  timestamp_str=have_len(be_above_or_equal(1))
                                                                                                  )
                                                                          )
                                                    )
        with it('calls rabbitmq_client publish with headers attribute set with ttl value'):
            self.sut.publish_with_ttl(AN_EVENT_NAME, A_NETWORK, A_TTL_MILLISECONDS_IN_NUMBER)

            expect(self.rabbitmq_client.publish).to(have_been_called_with(headers=have_key('expiration', str(A_TTL_MILLISECONDS_IN_NUMBER))))

    with context('Feature: publish_with_delay'):
        with context('Declaring exchange'):
            with it('calls rabbitmq_client exchange_declare with exchange name and durable attributes'):
                self.sut.publish_with_delay(AN_EVENT_NAME, A_NETWORK, delay_milliseconds=0)

                expect(self.rabbitmq_client.exchange_declare).to(have_been_called_with(exchange=AN_EXCHANGE, durable=True))

            with it('calls rabbitmq_client exchange_declare with exchange_type as X_DELAYED'):
                self.sut.publish_with_delay(AN_EVENT_NAME, A_NETWORK, delay_milliseconds=0)

                expect(self.rabbitmq_client.exchange_declare).to(have_been_called_with(exchange_type=X_DELAYED))

            with it('calls rabbitmq_client exchange_declare with arguments...'):
                self.sut.publish_with_delay(AN_EVENT_NAME, A_NETWORK, delay_milliseconds=0)

                expect(self.rabbitmq_client.exchange_declare).to(have_been_called_with(arguments=have_key('x-delayed-type', 'topic')))

        with context('Publishing'):
            with it('calls rabbitmq_client publish to publish an Event entity as message (with the timestamp and timestamp_str)'):
                a_data = 'a_data'
                an_id = 'an_id'

                self.sut.publish_with_delay(AN_EVENT_NAME, A_NETWORK, delay_milliseconds=0, data=a_data, id=an_id, topic_prefix=A_TOPIC_PREFIX)

                expect(self.rabbitmq_client.publish).to(have_been_called_with(message=have_properties(name=AN_EVENT_NAME,
                                                                                                      network=A_NETWORK,
                                                                                                      data=a_data,
                                                                                                      topic_prefix=A_TOPIC_PREFIX,
                                                                                                      id=an_id,
                                                                                                      timestamp=be_a(float),
                                                                                                      timestamp_str=have_len(be_above_or_equal(1))
                                                                                                      )
                                                                              )
                                                        )

            with it('calls rabbitmq_client publish with headers attribute set with x-delay value'):
                self.sut.publish_with_delay(AN_EVENT_NAME, A_NETWORK, delay_milliseconds=A_DELAY_MILLISECONDS_IN_NUMBER)

                expect(self.rabbitmq_client.publish).to(have_been_called_with(headers=have_key('x-delay', str(A_DELAY_MILLISECONDS_IN_NUMBER))))

