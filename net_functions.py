
from socket import *

############################
############################
##                        ##
##  Network-Related Code  ##
##                        ##
############################
############################

# Note: In all of the following functions, conn is a socket object

##############################
# recvAll equivalent to sendall

def recvAll(conn, msgLength):
    msg = b''
    while len(msg) < msgLength:
        retVal = conn.recv(msgLength - len(msg))
        msg += retVal
        if len(retVal) == 0:
            break    
    return msg


######################
# Send/Recv 8-byte int

def sendInt(conn, i):
    conn.sendall(i.to_bytes(8, byteorder="big"))

def recvInt(conn):
    return int.from_bytes(recvAll(conn, 8), byteorder="big")

####################
# Send/Recv DHT Keys

# Note: key is an integer representation of the key. That is, it is derived
#       from hashlib.sha1(bStr).digest()

def sendKey(conn, key):
    conn.sendall(key.to_bytes(20, byteorder="big"))

def recvKey(conn):
    return int.from_bytes(recvAll(conn, 20), byteorder="big")


####################
# Send/Recv DHT Vals

def sendVal(conn, val):
    valSz = len(val)
    sendInt(conn, valSz)
    conn.sendall(val)

def recvVal(conn):
    valLen = recvInt(conn)
    return recvAll(conn, valLen)


#############################
# Send/Recv Network Addresses

def sendAddress(conn, addr):
    msg = "%s:%d" % (addr)
    sendInt(conn, len(msg))
    conn.sendall(msg.encode())

def recvAddress(conn):
    msgLen = recvInt(conn)
    strAddr = recvAll(conn, msgLen).decode().split(":")
    return (strAddr[0], int(strAddr[1]))

def getLocalIPAddress():
    s = socket(AF_INET, SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    return s.getsockname()[0]
