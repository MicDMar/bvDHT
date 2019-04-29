import hashlib 

class Peer:
    def __init__(self, address, port):
        self.address = "{}:{}".format(address, port)
        self.hash = int.from_bytes(hashlib.sha1(self.address.encode()).digest(), byteorder="big")

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
        self.table.append(peer)

    def set_successor(self, peer):
        self.successor = peer
        self.add(peer)

    def get_successor(self):
        return self.successor

    def set_predecessor(self, peer):
        self.predecessor = peer
        self.add(predecessor)

    def get_predecessor(self):
        return self.predecessor

    def remove(peer_addr):
        self.table = [x for x in self.table if x.address != peer_addr]

    def __repr__(self):
        s = ""
        for peer in self.table:
            s += str(peer)
        return "<FingerTable: [{}]>".format(s)

