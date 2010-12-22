import zope.interface

class IClientStorage(zope.interface.Interface):
    """ Interface provided by storage backends
    """

class IClientCache(zope.interface.Interface):
    """ Interface provided by cache backends
    """