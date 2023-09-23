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


def decode_tokendb_v2(data):
    output = {}

    pos = 0
    version = data[pos]
    hash_length = data[pos+1]
    salt_length = data[pos+2]
    salt = data[pos+3:pos+3+salt_length]
    pos = pos + 3 + salt_length

    output = {
        "hash_length": hash_length,
        "salt": binascii.hexlify(salt).decode(),
        "tokens": {}
    }

    while pos < len(data):
        hashed_uid = data[pos:pos+hash_length]
        access = data[pos+hash_length]
        username_length = data[pos+hash_length+1]
        username = data[pos+hash_length+2:pos+hash_length+2+username_length]
        pos = pos+hash_length+2+username_length
        output["tokens"][binascii.hexlify(hashed_uid).decode()] = {
            "access": access,
            "username": username.decode()
        }

    return output


if __name__ == "__main__":
    import sys
    data = sys.stdin.buffer.read()
    print(decode_tokendb_v2(data))
