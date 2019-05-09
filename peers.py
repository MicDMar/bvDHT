from hash_functions import get_hash, hash_size

class Peer:
    def __init__(self, address, port):
        self.address = "{}:{}".format(address, port)
        self.hash = get_hash(self.address)

    def __repr__(self):
        return "<Peer: {} : {}>".format(self.address, self.hash)

class FingerTable:
    def __init__(self, our_address, size=5):
        self.our_address = our_address
        self.size = size
        self.table = []

    def get(self, hsh):
        options = [x for x in self.table if x.hash < hsh]
        if len(options) == 0:
            return self.our_address
        else:
            # Find the 'closest' owner of this hash
            return max(options)

    def add(self, peer):
        # FIXME: Enforce size limit, remove items if needed
        self.table.append(peer)

    def add_address(self, peer_addr):
        self.add(Peer(*get_addr_tupple(peer_addr)))

    def set_successors(self, peer1, peer2):
        self.successors = [ peer1, peer2 ]
        self.add(peer1)
        self.add(peer2)

    def get_successors(self):
        return self.successors

    def set_predecessor(self, peer):
        self.predecessor = peer
        self.add(predecessor)

    def get_predecessor(self):
        return self.predecessor

    def remove(self, peer_addr):
        self.table = [x for x in self.table if x.address != peer_addr]

    def our_hash(self):
        return self.hash

    """
    Get the hash directly before ours
    (The last hash belonging to somebody else before us)
    """
    def prev_hash(self):
        return (self.our_hash() - 1) % hash_size()

    def empty(self):
        return len(self.table) is 0

    def __repr__(self):
        s = ""
        for peer in self.table:
            s += "  {},\n".format(peer)
        return "<FingerTable({}): [\n{}]>".format(self.our_address, s[:])

