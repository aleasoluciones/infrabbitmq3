## New approach basic get without cancel

It has a workaroun (sleep 1 second if no messages to consume) to avoid eat CPU.

### Manual testing this approach

```
dev/start_local_dependencies.sh
TEST_MODE=0 event_processor.py -f infrabbitmq.events.ConsoleLogEventsProcessor -e events -q test_queue -t ‘*.test.#’
python bin/event_publisher_test.py -n lab -d events -e test ‘{“event_data”: “some data”}’
```
