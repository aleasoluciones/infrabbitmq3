from functools import wraps
from retrying import retry

from infrabbitmq.exceptions import (
    ClientWrapperError,
    RabbitMQError,
)
from infrabbitmq.events import Event


DIRECT_EXCHANGE_TYPE = 'direct'
TOPIC_EXCHANGE_TYPE = 'topic'
X_DELAYED = 'x-delayed-message'
COMPRESS_KEY = 'compress'

# AMQP list_of_topics
# * (star) can substitute for exactly one word.
# # (hash) can substitute for zero or more words.


# pylint: disable=E0213
# pylint: disable=E1102
class RabbitMQClient:
    def __init__(self, broker_uri, serializer, pika_client_wrapper, logger, compressor):
        self._broker_uri = broker_uri.replace('rabbitmq', 'amqp')
        self._connected_client = None
        self._serializer = serializer
        self._pika_client_wrapper = pika_client_wrapper
        self._logger = logger
        self._compressor = compressor

    @property
    def connected_client(self):
        if self._connected_client is None:
            self._connect()
        return self._connected_client

    def raise_rabbitmq_error(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except ClientWrapperError as exc:
                self._logger.info(f'Reconnecting, Error ClientWrapper {exc}',
                                  exc_info=True)
                self.disconnect()
                raise RabbitMQError(exc)
        return wrapper

    def _connect(self):
        self._connected_client = self._pika_client_wrapper # Be careful has to be an object
        self._connected_client.connect(self._broker_uri)

    def disconnect(self):
        try:
            self.connected_client.disconnect()
        except Exception as exc:
            self._logger.info(
                f'{self.__class__.__name__} disconnect fails: {type(exc)} {exc}',
                exc_info=True,
            )
        finally:
            self._connected_client = None

    @raise_rabbitmq_error
    def exchange_declare(self, exchange, exchange_type, **kwargs):
        if exchange_type == X_DELAYED:
            kwargs['arguments'] = {'x-delayed-type': 'topic'}
        self.connected_client.exchange_declare(exchange=exchange,
                                               exchange_type=exchange_type,
                                               passive=kwargs.get('passive', False),
                                               durable=kwargs.get('durable', False),
                                               auto_delete=kwargs.get('auto_delete', False),
                                               internal=kwargs.get('internal', False),
                                               arguments=kwargs.get('arguments', {}))

    @raise_rabbitmq_error
    def exchange_delete(self, exchange):
        self.connected_client.exchange_delete(exchange=exchange)

    @raise_rabbitmq_error
    def queue_declare(self, queue_name, auto_delete=True, exclusive=False, durable=False, message_ttl=None):
        arguments = {}
        if message_ttl is not None:
            arguments['x-message-ttl'] = message_ttl
        self.connected_client.queue_declare(queue_name=queue_name,
                                            auto_delete=auto_delete,
                                            exclusive=exclusive,
                                            durable=durable,
                                            arguments=arguments)

    @raise_rabbitmq_error
    def queue_bind(self, queue_name, exchange, routing_key=''):
        self.connected_client.queue_bind(queue_name=queue_name, exchange=exchange, routing_key=routing_key)

    @raise_rabbitmq_error
    def queue_unbind(self, queue_name, exchange, routing_key=''):
        self.connected_client.queue_unbind(queue_name=queue_name, exchange=exchange, routing_key=routing_key)

    @raise_rabbitmq_error
    def queue_purge(self, queue_name):
        self.connected_client.queue_purge(queue_name=queue_name)

    @raise_rabbitmq_error
    def queue_delete(self, queue_name):
        self.connected_client.queue_delete(queue_name=queue_name)

    @raise_rabbitmq_error
    def publish(self, exchange, routing_key, message, **kwargs):
        compression = self._is_compression_enable(kwargs)
        self.connected_client.basic_publish(exchange=exchange,
                                            routing_key=routing_key,
                                            body=self._compressor.compress(self._serialize(message), compression),
                                            **kwargs)

    @raise_rabbitmq_error
    def consume(self, queue_name, timeout=1):
        message = self.connected_client.consume_one_message(queue_name=queue_name, timeout_in_seconds=timeout)
        if message:
            message['body'] = self._deserialize(self._compressor.decompress(message['body'], self._is_decompression_enable(message['properties'])))
            message = RabbitMQMessage(message)
        else:
            message = None

        self.disconnect()
        return message

    def consume_next(self, queue_name, timeout=1):
        try:
            while True:
                if next_message := self.connected_client.consume_one_message(
                    queue_name=queue_name, timeout_in_seconds=timeout
                ):
                    next_message['body'] = self._deserialize(self._compressor.decompress(next_message['body'], self._is_decompression_enable(next_message['properties'])))
                    yield RabbitMQMessage(next_message)
                else:
                    yield None
        except ClientWrapperError as exc:
            self._logger.info(f'Reconnecting, Error ClientWrapper {exc}', exc_info=True)
            self.disconnect()
            raise RabbitMQError(exc) from exc

    @raise_rabbitmq_error
    def consume_pending(self, queue_name, timeout=1):
        return RabbitMQQueueIterator(queue_name=queue_name,
                                     pika_client_wrapper=self._pika_client_wrapper,
                                     timeout_in_seconds=timeout,
                                     serializer=self._serializer,
                                     logger=self._logger,
                                     compressor=self._compressor)

    def _serialize(self, value):
        return self._serializer.dumps(value)

    def _deserialize(self, value):
        return self._serializer.loads(value)

    def _is_compression_enable(self, kwargs):
        if kwargs is None:
            return False
        return bool(kwargs.get('headers', {}).get(COMPRESS_KEY, False))

    def _is_decompression_enable(self, properties):
        if not properties.get('headers', False):
            return False
        return bool(properties.get('headers').get(COMPRESS_KEY, False))


class RabbitMQMessage:
    def __init__(self, message):
        self.message = message

    @property
    def correlation_id(self):
        return self.message['headers'].get('correlation_id')

    @property
    def reply_to(self):
        return self.message['headers'].get('reply_to')

    @property
    def host(self):
        return self.message['headers'].get('HOST')

    @property
    def body(self):
        return self.message['body']

    @property
    def routing_key(self):
        return self.message.get('routing_key')

    def __str__(self):
        return str(self.body)


# pylint: disable=E0213
# pylint: disable=E1102
class RabbitMQQueueIterator:
    def __init__(self, queue_name, pika_client_wrapper, timeout_in_seconds, serializer, logger, compressor):
        self._queue_name = queue_name
        self._pika_wrapper_client = pika_client_wrapper
        self._timeout_in_seconds = timeout_in_seconds
        self._serializer = serializer
        self._logger = logger
        self._compressor = compressor

    def iterator_raise_rabbitmq_error(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except ClientWrapperError as exc:
                self._logger.info(f'Reconnecting, Error ClientWrapper {exc}',
                                  exc_info=True)
                self._pika_wrapper_client.disconnect()
                raise RabbitMQError(exc)
            except Exception as exc:
                self._logger.critical('Error consuming from queue {} exc_type {} exc {}'.format(self._queue_name,
                                                                                                type(exc),
                                                                                                exc),
                                      exc_info=True)
                new_exc = exc
                raise new_exc
        return wrapper

    def __iter__(self):
        return self

    @iterator_raise_rabbitmq_error
    def __next__(self):
        message = self._pika_wrapper_client.consume_one_message(queue_name=self._queue_name,
                                                                timeout_in_seconds=self._timeout_in_seconds)
        if not message:
            raise StopIteration

        message['body'] = self._serializer.loads(self._compressor.decompress(message['body'], self._is_decompression_enable(message['properties'])))
        return RabbitMQMessage(message)

    def _is_decompression_enable(self, properties):
        if not properties.get('headers', False):
            return False
        return bool(properties.get('headers').get(COMPRESS_KEY, False))


class RabbitMQEventPublisher:
    WAIT_EXPONENTIAL_MULTIPLIER_IN_MILLISECONDS = 1000
    MAX_PUBLISHING_RETRIES = 3

    def __init__(self, rabbitmq_client, clock_service, exchange):
        self._rabbitmq_client = rabbitmq_client
        self._clock_service = clock_service
        self._exchange = exchange

    def publish(self, event_name, network, data=None, id=None, topic_prefix=None, persistent=False, compress=False):
        event = self._build_an_event_with_timestamp(event_name, network=network, data=data, id=id, topic_prefix=topic_prefix)
        self.publish_event_object(event=event, persistent=persistent, compress=compress)

    def publish_event_object(self, event, persistent=False, compress=False):
        if not event.timestamp or not event.timestamp_str:
            now = self._clock_service.now()
            event.timestamp = self._clock_service.timestamp(now)
            event.timestamp_str = str(now)
        message_header = {'persistent': persistent, COMPRESS_KEY: compress}
        self._exchange_declare(exchange_type=TOPIC_EXCHANGE_TYPE, durable=True)
        self._publish_an_event(event=event, message_header=message_header)

    def publish_with_ttl(self, event_name, network, ttl_milliseconds, data=None, id=None, topic_prefix=None, persistent=False, compress=False):
        self._exchange_declare(exchange_type=TOPIC_EXCHANGE_TYPE, durable=True)

        event = self._build_an_event_with_timestamp(event_name, network=network, data=data, id=id, topic_prefix=topic_prefix)
        message_header = {
            'expiration': str(ttl_milliseconds),
            'persistent': persistent,
            COMPRESS_KEY: compress,
        }
        self._publish_an_event(event=event, message_header=message_header)

    def publish_with_delay(self, event_name, network, delay_milliseconds=0, data=None, id=None, topic_prefix=None, persistent=False, compress=False):
        exchange_arguments = {'x-delayed-type': 'topic'}
        self._exchange_declare(exchange_type=X_DELAYED, durable=True, arguments=exchange_arguments)

        event = self._build_an_event_with_timestamp(event_name, network=network, data=data, id=id, topic_prefix=topic_prefix)
        message_header = {'x-delay': str(delay_milliseconds), 'persistent': persistent, COMPRESS_KEY: compress}
        self._publish_an_event(event=event, message_header=message_header)

    def _build_an_event_with_timestamp(self, event_name, network, data, id, topic_prefix):
        now = self._clock_service.now()
        timestamp = self._clock_service.timestamp(now)
        return Event(
            event_name,
            network=network,
            data=data,
            id=id,
            topic_prefix=topic_prefix,
            timestamp=timestamp,
            timestamp_str=str(now),
        )

    def _publish_an_event(self, event, message_header=None):
        @retry(wait_exponential_multiplier=self.WAIT_EXPONENTIAL_MULTIPLIER_IN_MILLISECONDS, stop_max_attempt_number=self.MAX_PUBLISHING_RETRIES)
        def _call_publish_an_event(event, message_header=None):
            if message_header:
                self._rabbitmq_client.publish(exchange=self._exchange, routing_key=event.topic, message=event, headers=message_header)
            else:
                self._rabbitmq_client.publish(exchange=self._exchange, routing_key=event.topic, message=event)

        _call_publish_an_event(event, message_header)

    def _exchange_declare(self, exchange_type, durable, arguments=None):
        @retry(wait_exponential_multiplier=self.WAIT_EXPONENTIAL_MULTIPLIER_IN_MILLISECONDS, stop_max_attempt_number=self.MAX_PUBLISHING_RETRIES)
        def _call_exchange_declare(exchange, exchange_type, durable, arguments):
            if arguments:
                self._rabbitmq_client.exchange_declare(exchange=self._exchange, exchange_type=exchange_type, durable=durable, arguments=arguments)
            else:
                self._rabbitmq_client.exchange_declare(exchange=self._exchange, exchange_type=exchange_type, durable=durable)

        _call_exchange_declare(exchange=self._exchange, exchange_type=exchange_type, durable=durable, arguments=arguments)


class RabbitMQQueueEventProcessor:
    def __init__(self, queue_name, event_processor, rabbitmq_client, exchange, list_of_topics, exchange_options, queue_options, event_builder, logger, exchange_type=TOPIC_EXCHANGE_TYPE):
        self._queue_name = queue_name
        self._event_processor = event_processor
        self._rabbitmq_client = rabbitmq_client
        self._exchange = exchange
        self._list_of_topics = list_of_topics
        self._exchange_options = exchange_options
        self._queue_options = queue_options
        self._event_builder = event_builder
        self._exchange_type = exchange_type
        self._logger = logger

        self._connection_setup()

    def process_body(self, max_iterations=None):
        for index, raw_message in enumerate(self._rabbitmq_client.consume_next(queue_name=self._queue_name), start=1):
            if raw_message is not None:
                try:
                    message = self._event_builder.build(raw_message.body)
                    self._event_processor.process(message)
                except Exception as exc:
                    self._logger.critical(
                        f'Error processing from queue: {self._queue_name} raw_message:{raw_message} with exc_type:{type(exc)} exc:{exc}',
                        exc_info=True,
                    )
            if max_iterations is not None and index >= max_iterations:
                return

    def _connection_setup(self):
        self._rabbitmq_client.disconnect()
        self._declare_recurses()

    def _declare_recurses(self):
        self._declare_exchange()
        self._declare_queue()
        self._bind_queue_to_topics()

    def _declare_exchange(self):
        self._rabbitmq_client.exchange_declare(exchange=self._exchange,
                                               exchange_type=self._exchange_type,
                                               durable=self._exchange_options.get('durable', True),
                                               auto_delete=self._exchange_options.get('auto_delete', False)
                                               )

    def _declare_queue(self):
        self._rabbitmq_client.queue_declare(queue_name=self._queue_name,
                                            durable=self._queue_options.get('durable', True),
                                            auto_delete=self._queue_options.get('auto_delete', False),
                                            message_ttl=self._queue_options.get('message_ttl')
                                            )

    def _bind_queue_to_topics(self):
        for topic in self._list_of_topics:
            self._rabbitmq_client.queue_bind(queue_name=self._queue_name,
                                             exchange=self._exchange,
                                             routing_key=topic)
