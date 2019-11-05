from infrabbitmq.events import Event


class RawEventBuilder:
    def build(self, raw_event):
        return raw_event


class FelixEventBuilder:
    def build(self, raw_event):
        return Event(**raw_event)