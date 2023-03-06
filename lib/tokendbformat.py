import binascii
import hashlib


def encode_tokendb_v1(data):
    """
    Encodes a dict of "uid: name"
    into the tokendb v1 format.
    """
    output = bytes([1])  # version 1
    for uid in sorted(data.keys()):
        uid = binascii.unhexlify(uid)
        uidlen = len(uid)
        if uidlen == 4 or uidlen == 7:
            output += bytes([uidlen]) + uid
    return output


def encode_tokendb_v2(data, hash_length=4, salt=b""):
    """
    Encodes a dict of "uid: name"
    into the tokendb v2 format.
    """
    output = bytes([2, hash_length, len(salt)])
    output += salt
    for hexuid in sorted(data.keys()):
        uid = binascii.unhexlify(hexuid)
        uidlen = len(uid)
        if uidlen == 4 or uidlen == 7:
            output += hashlib.md5(salt + uid).digest()[0:hash_length]
            output += bytes([1])  # access level = 0x01
            try:
                user = data[hexuid].encode("us-ascii")
                output += bytes([len(user)])
                output += user
            except UnicodeEncodeError:
                output += bytes([0])
    return output
