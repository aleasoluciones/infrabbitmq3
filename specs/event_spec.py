from mamba import description, context, it
from expects import expect, equal
from infrabbitmq import events

A_TOPIC_PREFIX = 'a_topic_prefix'
A_NETWORK = 'a_network'
AN_EVENT_NAME = 'an_event_name'

with description('Event tests') as self:
    with context('when creating an event'):
        with context('when specifying a prefix'):
            with it('returns the topic with the specified prefix'):
                event = events.Event(AN_EVENT_NAME, A_NETWORK, 'an_event_data', topic_prefix=A_TOPIC_PREFIX)

                expected_topic = f'{A_NETWORK}.{A_TOPIC_PREFIX}.{AN_EVENT_NAME}'
                expect(event.topic).to(equal(expected_topic))

        with context('when NOT specifying a prefix'):
            with it('returns the topic without prefix'):
                event = events.Event(AN_EVENT_NAME, A_NETWORK, 'an_event_data')

                expected_topic = f'{A_NETWORK}.{AN_EVENT_NAME}'
                expect(event.topic).to(equal(expected_topic))
