import abc


from watch import WatchMe
from watch.core import AttributeControllerMeta


class WatchABCMetaType(abc.ABCMeta, AttributeControllerMeta):
    pass


class WatchABCType(abc.ABC, WatchMe, metaclass=WatchABCMetaType):
    pass

