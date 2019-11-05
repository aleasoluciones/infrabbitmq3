class NoopProcessor:
    def process(self, event):
        pass


class ConsoleLogEventsProcessor:
    def process(self, event):
        print(f'{event.timestamp} {event.network} {event.name} {event.id} {event.data}')
