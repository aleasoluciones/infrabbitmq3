from mamba import description, before, context, it, after
from doublex import Spy
from doublex_expects import have_been_called_with, have_been_called
from expects import expect,  be_a

from os import getpid
from time import sleep

from infrabbitmq import factory
from infrabbitmq.events import Event
from infrabbitmq.rabbitmq import (
    TOPIC_EXCHANGE_TYPE,
    X_DELAYED,
)

# --------------------------------------------------
# Avoid pika logging
factory.configure_pika_logger_to_error()
# --------------------------------------------------


A_TOPIC_EXCHANGE_NAME = 'a_topic_exchange_name'
A_QUEUE_NAME = f'a_queue_name_{getpid()}'
WILDCARD_TOPIC = '#'
KERN_CRITICAL_TOPIC = '*.kern.critical'
HDD_INFO_TOPIC = '*.hdd.info'
A_LIST_OF_TOPICS = [WILDCARD_TOPIC, KERN_CRITICAL_TOPIC, HDD_INFO_TOPIC]
A_NETWORK = 'a_network'
AN_EVENT_NAME = 'an_event_name'
AN_EVENT_DATA = 'an_event_data'
ANOTHER_EVENT_DATA = 'another_event_data'
SOME_ANOTHER_EVENT_DATA = 'some_another_event_data'


with description('RabbitMQQueueEventProcessor integration test') as self:
    with before.each:
        self.sut_event_publisher = factory.rabbitmq_event_publisher(exchange=A_TOPIC_EXCHANGE_NAME)
        self.event_processor = Spy()
        self.felix_event_builder = factory.felix_event_builder()

    with after.each:
        rabbitmq_client = factory.no_singleton_rabbitmq_client()
        for topic in A_LIST_OF_TOPICS:
            rabbitmq_client.queue_unbind(queue_name=A_QUEUE_NAME,
                                         exchange=A_TOPIC_EXCHANGE_NAME,
                                         routing_key=topic)
        rabbitmq_client.queue_delete(queue_name=A_QUEUE_NAME)
        rabbitmq_client.exchange_delete(exchange=A_TOPIC_EXCHANGE_NAME)

    with context('FEATURE: process body'):
        with context('when wildcard topic and publish two events'):
            with it('calls the processor twice with event object data'):
                sut_event_processor = factory.no_singleton_rabbitmq_queue_event_processor(queue_name=A_QUEUE_NAME,
                                                                                          event_processor=self.event_processor,
                                                                                          exchange=A_TOPIC_EXCHANGE_NAME,
                                                                                          list_of_topics=[WILDCARD_TOPIC],
                                                                                          event_builder=self.felix_event_builder,
                                                                                          exchange_type=TOPIC_EXCHANGE_TYPE,
                                                                                          queue_options={},
                                                                                          exchange_options={}
                                                                                          )
                self.sut_event_publisher.publish('kern.critical', A_NETWORK, data=AN_EVENT_DATA)
                self.sut_event_publisher.publish('kern.critical.info', A_NETWORK, data=ANOTHER_EVENT_DATA)


                sut_event_processor.process_body(max_iterations=2)


                expect(self.event_processor.process).to(have_been_called_with(be_a(Event)).twice)

        with context('when processing from multiple topics and publish three events with different topics'):
            with it('calls the processor twice with event object data (for the binding topics)'):
                sut_event_processor = factory.no_singleton_rabbitmq_queue_event_processor(queue_name=A_QUEUE_NAME,
                                                                                          event_processor=self.event_processor,
                                                                                          exchange=A_TOPIC_EXCHANGE_NAME,
                                                                                          list_of_topics=[KERN_CRITICAL_TOPIC, HDD_INFO_TOPIC],
                                                                                          event_builder=self.felix_event_builder,
                                                                                          exchange_type=TOPIC_EXCHANGE_TYPE,
                                                                                          queue_options={},
                                                                                          exchange_options={}
                                                                                          )
                self.sut_event_publisher.publish('kern.critical', A_NETWORK, data=AN_EVENT_DATA)
                self.sut_event_publisher.publish('hdd.info', A_NETWORK, data=ANOTHER_EVENT_DATA)
                self.sut_event_publisher.publish('another.topic', A_NETWORK, data=SOME_ANOTHER_EVENT_DATA)


                sut_event_processor.process_body(max_iterations=3)


                expect(self.event_processor.process).to(have_been_called_with(be_a(Event)).twice)

        with context('when processing events from a queue with ttl and time expired'):
            with it('does not process any event'):
                a_ttl_milliseconds = 800
                a_queue_with_ttl_option = {'message_ttl': a_ttl_milliseconds}
                sut_event_processor = factory.no_singleton_rabbitmq_queue_event_processor(queue_name=A_QUEUE_NAME,
                                                                                          event_processor=self.event_processor,
                                                                                          exchange=A_TOPIC_EXCHANGE_NAME,
                                                                                          list_of_topics=[WILDCARD_TOPIC],
                                                                                          event_builder=self.felix_event_builder,
                                                                                          exchange_type=TOPIC_EXCHANGE_TYPE,
                                                                                          queue_options=a_queue_with_ttl_option,
                                                                                          exchange_options={}
                                                                                          )
                self.sut_event_publisher.publish('kern.critical', A_NETWORK, data=AN_EVENT_DATA)

                sleep(1)
                sut_event_processor.process_body(max_iterations=1)

                expect(self.event_processor.process).not_to(have_been_called)

        with context('when processing events from a delayed exchange'):
            with context('when publishing a delayed event'):
                with it('calls (after delayed) the processor once with event object data'):
                    delay_milliseconds = 2000
                    sut_event_processor = factory.no_singleton_rabbitmq_queue_event_processor(queue_name=A_QUEUE_NAME,
                                                                                              event_processor=self.event_processor,
                                                                                              exchange=A_TOPIC_EXCHANGE_NAME,
                                                                                              list_of_topics=[WILDCARD_TOPIC],
                                                                                              event_builder=self.felix_event_builder,
                                                                                              exchange_type=X_DELAYED,
                                                                                              queue_options={},
                                                                                              exchange_options={}
                                                                                              )

                    self.sut_event_publisher.publish_with_delay('kern.critical', A_NETWORK, delay_milliseconds, data=AN_EVENT_DATA)
                    sut_event_processor.process_body(max_iterations=1)
                    expect(self.event_processor.process).not_to(have_been_called)

                    sleep(2)
                    sut_event_processor.process_body(max_iterations=1)
                    expect(self.event_processor.process).to(have_been_called_with(be_a(Event)).once)

        with context('when raw event builder is using as event builder'):
            with it('calls the process with instance of dict'):
                raw_event_builder = factory.raw_event_builder()
                sut_event_processor = factory.no_singleton_rabbitmq_queue_event_processor(queue_name=A_QUEUE_NAME,
                                                                                          event_processor=self.event_processor,
                                                                                          exchange=A_TOPIC_EXCHANGE_NAME,
                                                                                          list_of_topics=[WILDCARD_TOPIC],
                                                                                          event_builder=raw_event_builder,
                                                                                          exchange_type=TOPIC_EXCHANGE_TYPE,
                                                                                          queue_options={},
                                                                                          exchange_options={}
                                                                                          )
                self.sut_event_publisher.publish('kern.critical', A_NETWORK, data={})


                sut_event_processor.process_body(max_iterations=1)


                expect(self.event_processor.process).to(have_been_called_with(be_a(dict)).once)
