from watch import WatchMe
from watch.builtins import InstanceOf, Container, Predicate


from .proto import LinkMeta, DATA, NAMESPACE


SpanChecker = (
    Container(items=InstanceOf(int), container=tuple) &
    Predicate(lambda container: len(container) == 2) &
    Predicate(lambda container: container[1] >= container[0])
)


class LinkEndpoint(WatchMe):
    id = InstanceOf(bytes)

    def __init__(self, id):
        self.id = id


class ChunkEndpoint(LinkEndpoint):
    span = SpanChecker()

    def __init__(self, id, span):
        super().__init__(id=id)
        self.span = span


class NamespaceEndpoint(LinkEndpoint):

    def __init__(self, id):
        super().__init__(id)


class Link(WatchMe):

    def __init__(self, endpoint):
        self.endpoint = endpoint

    def dump(self):
        return LinkMeta(
            type=(
                DATA if isinstance(self.endpoint, ChunkEndpoint) else
                NAMESPACE
            )
        )


class NamedLink(Link):
    name = InstanceOf(str)
    endpoint = InstanceOf(ChunkEndpoint, NamespaceEndpoint)

    def dump(self):
        base_result = super().dump()
        base_result.name = self.name

        if isinstance(self.endpoint, ChunkEndpoint):
            base_result.span_from, base_result.span_to = self.endpoint.span

        return base_result

    def __init__(self, endpoint, name):
        super().__init__(endpoint)
        self.name = name


class UnnamedLink(Link):
    endpoint = InstanceOf(ChunkEndpoint)

    def dump(self):
        base_result = super().dump()
        base_result.span_from, base_result.span_to = self.endpoint.span
        return base_result

    def __init__(self, endpoint):
        super().__init__(endpoint)

