## Callback approach

https://pika.readthedocs.io/en/stable/modules/adapters/blocking.html#pika.adapters.blocking_connection.BlockingChannel.basic_consume

### Manual testing this approach

```
dev/start_local_dependencies.sh
TEST_MODE=0 event_processor.py -f infrabbitmq.events.ConsoleLogEventsProcessor -e events -q test_queue -t ‘*.test.#’
python bin/event_publisher_test.py -n lab -d events -e test ‘{“event_data”: “some data”}’
```
