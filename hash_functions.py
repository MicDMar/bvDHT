import hashlib

def get_hash(item):
    return int.from_bytes(hashlib.sha1(item.encode()).digest(), byteorder="big")

def hash_size():
    return (2 ** 160) - 1
    
