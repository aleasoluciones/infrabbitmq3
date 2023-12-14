from mamba import description, before, context, it
from doublex import Spy, when, ANY_ARG
from expects import expect, have_properties, have_len, be_above_or_equal, be_a, have_key, have_keys, raise_error
from doublex_expects import have_been_called_with, have_been_called

from infcommon.clock import Clock
from infrabbitmq.events import Event
from infrabbitmq.rabbitmq import (
    RabbitMQClient,
    RabbitMQEventPublisher,
    TOPIC_EXCHANGE_TYPE,
    X_DELAYED,
)
from infrabbitmq.exceptions import RabbitMQError


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
                                                                                                  ),
                                                                          headers=have_keys(persistent=False, compress=False)
                                                                          )
                                                    )

    with context('Feature: publish_event_object'):
        with context('when the event has NOT timestamp and/or timestamp_str'):
            with before.each:
                self.a_data = 'a_data'
                self.an_id = 'an_id'
                self.an_event_without_timestamp = Event(AN_EVENT_NAME,
                                                   network=A_NETWORK,
                                                   data=self.a_data,
                                                   topic_prefix=A_TOPIC_PREFIX,
                                                   id=self.an_id,
                                                   timestamp=None,
                                                   timestamp_str=None)

            with it('publishes the event with the actual timestamp and timestamp_str'):
                self.sut.publish_event_object(self.an_event_without_timestamp)

                expect(self.rabbitmq_client.publish).to(have_been_called_with(message=have_properties(name=AN_EVENT_NAME,
                                                                                                      network=A_NETWORK,
                                                                                                      data=self.a_data,
                                                                                                      topic_prefix=A_TOPIC_PREFIX,
                                                                                                      id=self.an_id,
                                                                                                      timestamp=be_a(float),
                                                                                                      timestamp_str=have_len(be_above_or_equal(1))
                                                                                                      )
                                                                              )
                                                        )

            with it('calls rabbitmq_client publish with persistent header as False by default'):
                self.sut.publish_event_object(self.an_event_without_timestamp)

                expect(self.rabbitmq_client.publish).to(have_been_called_with(headers=have_key('persistent', False)))

            with context('compression'):
                with it('calls rabbitmq_client publish with compress header as False by default'):
                    self.sut.publish_event_object(self.an_event_without_timestamp)

                    expect(self.rabbitmq_client.publish).to(have_been_called_with(headers=have_key('compress', False)))

                with it('calls rabbitmq_client publish with compress header as True if compression is select'):
                    self.sut.publish_event_object(self.an_event_without_timestamp, compress=True)

                    expect(self.rabbitmq_client.publish).to(have_been_called_with(headers=have_key('compress', True)))

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

        with it('calls rabbitmq_client publish with persistent header as False by default'):
            self.sut.publish_with_ttl(AN_EVENT_NAME, A_NETWORK, A_TTL_MILLISECONDS_IN_NUMBER)

            expect(self.rabbitmq_client.publish).to(have_been_called_with(headers=have_key('persistent', False)))

        with context('compression'):
            with it('calls rabbitmq_client publish with compress header as False by default'):
                self.sut.publish_with_ttl(AN_EVENT_NAME, A_NETWORK, A_TTL_MILLISECONDS_IN_NUMBER)

                expect(self.rabbitmq_client.publish).to(have_been_called_with(headers=have_key('compress', False)))

            with it('calls rabbitmq_client publish with compress header as True if compression is select'):
                self.sut.publish_with_ttl(AN_EVENT_NAME, A_NETWORK, A_TTL_MILLISECONDS_IN_NUMBER, compress=True)

                expect(self.rabbitmq_client.publish).to(have_been_called_with(headers=have_key('compress', True)))

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
                expect(self.rabbitmq_client.publish).to(have_been_called_with(headers=have_key('compress', False)))

            with it('calls rabbitmq_client publish with persistent header as False by default'):
                self.sut.publish_with_delay(AN_EVENT_NAME, A_NETWORK, delay_milliseconds=A_DELAY_MILLISECONDS_IN_NUMBER)

                expect(self.rabbitmq_client.publish).to(have_been_called_with(headers=have_key('persistent', False)))

            with context('compression'):
                with it('calls rabbitmq_client publish with compress header as False by default'):
                    self.sut.publish_with_delay(AN_EVENT_NAME, A_NETWORK, delay_milliseconds=A_DELAY_MILLISECONDS_IN_NUMBER)

                    expect(self.rabbitmq_client.publish).to(have_been_called_with(headers=have_key('compress', False)))

                with it('calls rabbitmq_client publish with compress header as True if compression is select'):
                    self.sut.publish_with_delay(AN_EVENT_NAME, A_NETWORK, delay_milliseconds=A_DELAY_MILLISECONDS_IN_NUMBER, compress=True)

                    expect(self.rabbitmq_client.publish).to(have_been_called_with(headers=have_key('compress', True)))

    with context('When any calls to rabbitmq_client fails (UNHAPPY PATH)'):
        with context('exchange declare fails (used by publish, publish_event_object, publish_with_delay or publish_with_ttl)'):
            with it('retries three times'):
                self.sut.WAIT_EXPONENTIAL_MULTIPLIER_IN_MILLISECONDS=1
                when(self.rabbitmq_client).exchange_declare(ANY_ARG).raises(RabbitMQError)

                def _declaring_exchange_raises_rabbitmq_error():
                    self.sut.publish(AN_EVENT_NAME, A_NETWORK, data='a_data', id='an_id', topic_prefix=A_TOPIC_PREFIX)

                expect(_declaring_exchange_raises_rabbitmq_error).to(raise_error(RabbitMQError))
                expect(self.rabbitmq_client.exchange_declare).to(have_been_called.exactly(3))

        with context('publish fails (used by publish, publish_event_object, publish_with_delay or publish_with_ttl)'):
            with it('retries three times'):
                self.sut.WAIT_EXPONENTIAL_MULTIPLIER_IN_MILLISECONDS=1
                when(self.rabbitmq_client).publish(ANY_ARG).raises(RabbitMQError)

                def _publishing_raises_rabbitmq_error():
                    self.sut.publish(AN_EVENT_NAME, A_NETWORK, data='a_data', id='an_id', topic_prefix=A_TOPIC_PREFIX)

                expect(_publishing_raises_rabbitmq_error).to(raise_error(RabbitMQError))
                expect(self.rabbitmq_client.publish).to(have_been_called.exactly(3))

