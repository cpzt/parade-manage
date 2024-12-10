from parade_manage.constants import FLAG_NODE_PRIORITY


def order(priority):
    def h(cls: type) -> type:
        setattr(cls, FLAG_NODE_PRIORITY, priority)
        return cls
    return h

