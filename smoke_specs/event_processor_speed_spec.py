from mamba import description, context, it
from expects import expect, be_below

from os import environ
from time import perf_counter

from infrabbitmq import factory as infrabbitmq_factory
from infrabbitmq.rabbitmq import TOPIC_EXCHANGE_TYPE


EXCHANGE_NAME='infrabbitmq3_acceptance_specs'
QUEUE_NAME='speed_spec'
EVENT_NAME = 'speed_test'
DUMMY_NETWORK = 'dummy_network'
DUMMY_DATA = {'foo': 'bar'}
NUMBER_OF_EVENTS=2000


with description('Event processor speed test'):
    with context('when consuming 2.000 messages'):
        with it('does it in less than 3 seconds'):
            broker_uri = environ['BROKER_URI']
            rabbitmq_client = infrabbitmq_factory.no_singleton_rabbitmq_client(broker_uri=broker_uri)
            rabbitmq_client.exchange_declare(EXCHANGE_NAME, TOPIC_EXCHANGE_TYPE, durable=True)
            rabbitmq_client.queue_declare(QUEUE_NAME)
            rabbitmq_client.queue_bind(QUEUE_NAME, EXCHANGE_NAME)
            infrabbitmq_factory.configure_pika_logger_to_error()
            event_publisher = infrabbitmq_factory.rabbitmq_event_publisher(exchange=EXCHANGE_NAME, broker_uri=broker_uri)
            processor = infrabbitmq_factory.noop_event_processor()
            event_processor = infrabbitmq_factory.no_singleton_rabbitmq_queue_event_processor(queue_name=QUEUE_NAME,
                                                                                              exchange=EXCHANGE_NAME,
                                                                                              queue_options={'durable': False, 'auto_delete': True},
                                                                                              list_of_topics=['#.speed_test.#'],
                                                                                              event_processor=processor)
            for _ in range(NUMBER_OF_EVENTS):
                event_publisher.publish(event_name=EVENT_NAME, network=DUMMY_NETWORK, data=DUMMY_DATA)

            t1_start = perf_counter()
            event_processor.process_body(max_iterations=NUMBER_OF_EVENTS)
            t2_end = perf_counter()
            total_time = t2_end - t1_start
            expect(total_time).to(be_below(3))

            rabbitmq_client.queue_unbind(QUEUE_NAME, EXCHANGE_NAME)
            rabbitmq_client.queue_purge(QUEUE_NAME)
            rabbitmq_client.queue_delete(QUEUE_NAME)
            rabbitmq_client.exchange_delete(EXCHANGE_NAME)
