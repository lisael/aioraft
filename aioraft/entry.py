class Entry:
    def __init__(self, name, node=None):
        self.name = name
        self.node = node
        self.prev_value = None
        self.index = node.raft_index if node is not None else None


class FileEntry(Entry):
    def __init__(self, name, value=None, node=None):
        super(FileEntry, self).__init__(name, node)
        self.value = value


class DirEntry(dict, Entry):
    def __init__(self, name, node=None):
        Entry.__init__(self, name, node=node)

    def __setitem__(self, key, value):
        self.set_value(key, value)

    def __getitem__(self, key):
        return self.get_entry(key, create=False).value

    def set_value(self, key, value=None):
        isdir = False
        if key.endswith('/'):
            if value is not None:
                raise ValueError('Directories have no values')
            isdir = True
        entry = self.get_entry(key, create=True)
        if not isdir:
            entry.prev_value = entry.value
            entry.value = value
        return entry

    def get_entry(self, key, create=False):
        if not '/' in key:
            try:
                entry = super(DirEntry, self).__getitem__(key)
                if isinstance(entry, DirEntry):
                    raise ValueError('`%s` is a directory' % key)
                return entry
            except KeyError:
                if create:
                    entry = FileEntry(key, node=self.node)
                    dict.__setitem__(self, key, entry)
                    return entry
                else:
                    raise
        key, rest = key.split('/', 1)
        try:
            entry = super(DirEntry, self).__getitem__(key)
            if isinstance(entry, FileEntry):
                raise ValueError('`%s` is a file' % key)
        except KeyError:
            if create:
                entry = DirEntry(key, node=self.node)
                dict.__setitem__(self, key, entry)
            else:
                raise
        if not rest:
            return entry
        return entry.get_entry(rest, create=create)



