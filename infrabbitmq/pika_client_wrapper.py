from functools import wraps
import time

from pika import (
    URLParameters,
)
from pika.spec import (
    BasicProperties,
)

from pika import exceptions as pika_exceptions

from infrabbitmq.exceptions import ClientWrapperError


class PikaClientWrapper:
    DEFAULT_HEARTBEAT = 0
    SLEEP_TIME_IN_SECONDS = 1

    def __init__(self, pika_library):
        self._connection = None
        self._channel = None
        self._pika_library = pika_library

    def raise_client_wrapper_error(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except (pika_exceptions.AMQPError, pika_exceptions.ChannelError, pika_exceptions.ReentrancyError) as exc:
                raise ClientWrapperError(exc)
            except ValueError as exc:
                # if consumer-creation parameters don’t match those of the existing queue consumer generator, if any. NEW in pika 0.10.0
                raise ClientWrapperError(exc)
        return wrapper

    @raise_client_wrapper_error
    def connect(self, broker_uri):
        broker_uri_with_heartbeat = self._build_broker_uri_with_heartbeat(broker_uri)
        self._connection = self._pika_library.BlockingConnection(URLParameters(broker_uri_with_heartbeat))
        self._channel = self._connection.channel()
        self._channel.confirm_delivery()

    def _build_broker_uri_with_heartbeat(self, broker_uri):
        heartbeat_param = 'heartbeat'
        existing_query_params = '?'
        if heartbeat_param not in broker_uri:
            if existing_query_params in broker_uri:
                return f"{broker_uri}&{heartbeat_param}={self.DEFAULT_HEARTBEAT}"
            return f"{broker_uri}?{heartbeat_param}={self.DEFAULT_HEARTBEAT}"
        return broker_uri



    @raise_client_wrapper_error
    def disconnect(self):
        self._channel.close()
        self._connection.close()

    @raise_client_wrapper_error
    def exchange_declare(self, exchange, exchange_type, **kwargs):
        self._channel.exchange_declare(exchange=exchange,
                                       exchange_type=exchange_type,
                                       passive=kwargs.get('passive', False),
                                       durable=kwargs.get('durable', False),
                                       auto_delete=kwargs.get('auto_delete', False),
                                       internal=kwargs.get('internal', False),
                                       arguments=kwargs.get('arguments', {}))
    @raise_client_wrapper_error
    def exchange_delete(self, exchange):
        self._channel.exchange_delete(exchange=exchange)

    @raise_client_wrapper_error
    def queue_declare(self, queue_name, auto_delete=True, exclusive=False, durable=False, arguments=None):
        self._channel.queue_declare(queue_name,
                                    durable=durable,
                                    exclusive=exclusive,
                                    auto_delete=auto_delete,
                                    arguments=arguments)

    @raise_client_wrapper_error
    def queue_bind(self, queue_name, exchange, routing_key=''):
        self._channel.queue_bind(queue=queue_name, exchange=exchange, routing_key=routing_key)

    @raise_client_wrapper_error
    def queue_unbind(self, queue_name, exchange, routing_key=''):
        self._channel.queue_unbind(queue=queue_name, exchange=exchange, routing_key=routing_key)

    @raise_client_wrapper_error
    def queue_purge(self, queue_name):
        self._channel.queue_purge(queue=queue_name)

    @raise_client_wrapper_error
    def queue_delete(self, queue_name):
        self._channel.queue_delete(queue=queue_name)

    @raise_client_wrapper_error
    def basic_publish(self, exchange, routing_key, body, **kwargs):
        headers = kwargs.get('headers', {})
        properties = self._build_properties_for_basic_publish(headers)

        self._channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body, properties=properties, mandatory=False)

    def _build_properties_for_basic_publish(self, headers):
        if 'expiration' in headers.keys():
            return BasicProperties(expiration=headers['expiration'])
        elif 'x-delay' in headers.keys():
            return BasicProperties(headers=headers)

        return BasicProperties()

    @raise_client_wrapper_error
    def consume_one_message(self, queue_name, timeout_in_seconds=1):
        message_body = {}
        method_frame, properties, body = self._channel.basic_get(queue_name)
        if body and method_frame:
            self._channel.basic_ack(method_frame.delivery_tag)
            message_body['body'] = body
        else:
            # Workaround to avoid eat CPU
            time.sleep(self.SLEEP_TIME_IN_SECONDS)

        return message_body
