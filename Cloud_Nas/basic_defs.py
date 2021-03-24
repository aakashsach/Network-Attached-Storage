class cloud_storage:
    block_size = 4096       # System configuration -- DO NOT CHANGE!

    def list_blocks(self):
        raise NotImplementedError

    def read_block(self, offset):
        raise NotImplementedError

    def write_block(self, block, offset):
        raise NotImplementedError

    def delete_block(self, offset):
        raise NotImplementedError

class NAS:
    def open(self, filename):
        raise NotImplementedError

    def read(self, fd, len, offset):
        raise NotImplementedError

    def write(self, fd, data, offset):
        raise NotImplementedError

    def close(self, fd):
        raise NotImplementedError

    def delete(self, filename):
        raise NotImplementedError

    def get_storage_sizes(self):
        return [len(b.list_blocks()) for b in self.backends]


