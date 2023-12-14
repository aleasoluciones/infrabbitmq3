from mamba import description, before, context, it
from expects import expect, equal, be_none, raise_error, contain_only

from infrabbitmq.rabbitmq import RabbitMQMessage

with description('RabbitMQMessage tests') as self:
    with before.each:
        pass
    with context('getting correlation_id'):
        with context('when correlaction_id exists'):
            with it('returns correlation_id from raw_message headers'):
                a_correlaction_id = 'my_correlaction_id'
                a_raw_message = {'headers': {'correlation_id': a_correlaction_id}}

                rabbimq_message = RabbitMQMessage(a_raw_message)

                expect(rabbimq_message.correlation_id).to(equal(a_correlaction_id))

        with context('when correlaction_id DOES NOT exist'):
            with it('returns None'):
                a_raw_message = {'headers': {}}

                rabbimq_message = RabbitMQMessage(a_raw_message)

                expect(rabbimq_message.correlation_id).to(be_none)

    with context('getting reply_to'):
        with context('when reply_to exists'):
            with it('returns reply_to from raw_message headers'):
                a_reply_to = 'a_reply_to'
                a_raw_message = {'headers': {'reply_to': a_reply_to}}

                rabbimq_message = RabbitMQMessage(a_raw_message)

                expect(rabbimq_message.reply_to).to(equal(a_reply_to))

        with context('when reply_to DOES NOT exist'):
            with it('returns None'):
                a_raw_message = {'headers': {}}

                rabbimq_message = RabbitMQMessage(a_raw_message)

                expect(rabbimq_message.reply_to).to(be_none)

    with context('getting host'):
        with context('when host exists'):
            with it('returns host from raw_message headers'):
                a_host = 'a_host'
                a_raw_message = {'headers': {'HOST': a_host}}

                rabbimq_message = RabbitMQMessage(a_raw_message)

                expect(rabbimq_message.host).to(equal(a_host))

        with context('when host DOES NOT exist'):
            with it('returns None'):
                a_raw_message = {'headers': {}}

                rabbimq_message = RabbitMQMessage(a_raw_message)

                expect(rabbimq_message.host).to(be_none)

    with context('getting body'):
        with context('when body exists'):
            with it('returns body from raw_message'):
                a_body = 'a_body'
                a_raw_message = {'body': a_body}

                rabbimq_message = RabbitMQMessage(a_raw_message)

                expect(rabbimq_message.body).to(equal(a_body))

        with context('when body DOES NOT exist'):
            with it('raises KeyError exception'):
                def _a_raw_message_without_body():
                    a_raw_message = {}
                    rabbitmq_message = RabbitMQMessage(a_raw_message)
                    rabbitmq_message.body

                expect(_a_raw_message_without_body).to(raise_error(KeyError))

    with context('getting routing_key'):
        with context('when routing_key exists'):
            with it('returns routing_key from raw_message'):
                a_routing_key = 'a_routing_key'
                a_raw_message = {'routing_key': a_routing_key}

                rabbimq_message = RabbitMQMessage(a_raw_message)

                expect(rabbimq_message.routing_key).to(equal(a_routing_key))

        with context('when routing_key DOES NOT exist'):
            with it('returns None'):
                a_raw_message = {}

                rabbimq_message = RabbitMQMessage(a_raw_message)

                expect(rabbimq_message.routing_key).to(be_none)

    with context('printing a message'):
        with it('returns an string with body from raw_message'):
            a_body = 'a_body'
            a_raw_message = {'body': a_body}

            rabbimq_message = RabbitMQMessage(a_raw_message)

            expected_string = f'{a_body}'
            expect(f'{rabbimq_message}').to(contain_only(expected_string))
