import asyncio

class LogEntry:
    """ base class of all logs """
    repr_attrs = {'raft_index', 'commited', 'action'}
    def __init__(self, node, raft_index, commited=False):
        self.node = node
        self.commited = commited
        self.raft_index = raft_index

    @asyncio.coroutine
    def replicate(self):
        repr_ = dict()
        for attr in self.repr_attrs:
            repr_[attr] = getattr(self, attr)
        done, pending = yield from self.node.broadcast('replicate', repr_,
            wait_majority=True)
        if done is not None and not self.commited:
            self.commited = True
            data = self.commit()
            yield from self.replicate()
            return data

class SetLogEntry(LogEntry):
    repr_attrs = {'key', 'value'}
    repr_attrs.update(LogEntry.repr_attrs)
    action = 'set'
    def __init__(self, node, raft_index, key, value, commited=False):
        self.key = key
        self.value = value
        super(SetLogEntry, self).__init__(node, raft_index, commited)

    def commit(self):
        self.node.raft_index = self.raft_index
        entry = self.node.data.set_value(self.key, self.value)
        return entry

action_map = {
    'set': SetLogEntry,
}
