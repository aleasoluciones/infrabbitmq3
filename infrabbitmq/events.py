import infcommon


class Event(infcommon.AttributesComparison):
    def __init__(self, name, network=None, data=None, timestamp=None, id=None, topic_prefix=None, timestamp_str=None, **kwargs):
        self.name = name
        self.network = network
        self.data = data
        self.timestamp = timestamp
        self.timestamp_str = timestamp_str
        self.topic_prefix = topic_prefix
        self.id = id

    @property
    def topic(self):
        return '.'.join(map(str,
                        filter(None, (self.network, self.topic_prefix, self.name))))


class ConsoleLogEventsProcessor:
    def process(self, event):
        format_str = '{event.timestamp} {event.network} {event.name} {event.id} {data}'
        print(format_str.format(event=event, data=event.data))
