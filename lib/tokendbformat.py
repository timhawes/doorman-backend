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
        if uidlen == 4 or uidlen == 7 or uidlen == 10:
            output += bytes([uidlen]) + uid
    return output


def decode_tokendb_v1(data):
    version = data[0]
    if version != 1:
        raise ValueError("Incorrect version byte")

    tokens = {}
    pos = 1

    while pos < len(data):
        uid_len = data[pos]
        uid = data[pos + 1 : pos + 1 + uid_len]
        tokens[binascii.hexlify(uid).decode()] = ""
        pos = pos + 1 + uid_len

    return tokens


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
        if uidlen == 4 or uidlen == 7 or uidlen == 10:
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
    version = data[0]
    if version != 3:
        raise ValueError("Incorrect version byte")

    output = {}
    pos = 1

    hash_length = data[pos + 1]
    salt_length = data[pos + 2]
    salt = data[pos + 3 : pos + 3 + salt_length]
    pos = pos + 3 + salt_length

    output = {
        "hash_length": hash_length,
        "salt": binascii.hexlify(salt).decode(),
        "tokens": {},
    }

    while pos < len(data):
        hashed_uid = data[pos : pos + hash_length]
        access = data[pos + hash_length]
        username_length = data[pos + hash_length + 1]
        username = data[pos + hash_length + 2 : pos + hash_length + 2 + username_length]
        pos = pos + hash_length + 2 + username_length
        output["tokens"][binascii.hexlify(hashed_uid).decode()] = {
            "access": access,
            "username": username.decode(),
        }

    return output


def encode_tokendb_v3(data):
    """
    Encodes a dict of "uid: name"
    into the tokendb v3 format.
    """
    output = bytes([3])
    for hexuid in sorted(data.keys()):
        uid = binascii.unhexlify(hexuid)
        uidlen = len(uid)
        if uidlen == 4 or uidlen == 7 or uidlen == 10:
            output += bytes([uidlen]) + uid
            try:
                user = data[hexuid].encode("us-ascii")
                output += bytes([len(user)])
                output += user
            except UnicodeEncodeError:
                output += bytes([0])
    return output


def decode_tokendb_v3(data):
    version = data[0]
    if version != 3:
        raise ValueError("Incorrect version byte")

    tokens = {}
    pos = 1

    while pos < len(data):
        uid_len = data[pos]
        uid = data[pos + 1 : pos + 1 + uid_len]
        username_len = data[pos + 1 + uid_len]
        username = data[pos + 2 + uid_len : pos + 2 + uid_len + username_len]
        tokens[binascii.hexlify(uid).decode()] = username.decode()
        pos = pos + 2 + uid_len + username_len

    return tokens


def tests():
    tokens = {
        "f2063d1c": "username1",
        "387e1cbb918680": "username2",
        "c2190c50948e94": "username3",
        "16edaf65a2e035b628b5": "username4",
    }

    expected_v1 = b"\x01\n\x16\xed\xafe\xa2\xe05\xb6(\xb5\x078~\x1c\xbb\x91\x86\x80\x07\xc2\x19\x0cP\x94\x8e\x94\x04\xf2\x06=\x1c"
    encoded_v1 = encode_tokendb_v1(tokens)
    assert encoded_v1 == expected_v1

    expected_v1_decode = {x: "" for x in tokens.keys()}
    decoded_v1 = decode_tokendb_v1(encoded_v1)
    assert decoded_v1 == expected_v1_decode

    expected_v3 = b"\x03\n\x16\xed\xafe\xa2\xe05\xb6(\xb5\tusername4\x078~\x1c\xbb\x91\x86\x80\tusername2\x07\xc2\x19\x0cP\x94\x8e\x94\tusername3\x04\xf2\x06=\x1c\tusername1"
    encoded_v3 = encode_tokendb_v3(tokens)
    assert encoded_v3 == expected_v3

    decoded_v3 = decode_tokendb_v3(encoded_v3)
    assert decoded_v3 == tokens

    print("Tests OK")


if __name__ == "__main__":
    import sys

    tests()

    data = sys.stdin.buffer.read()
    if data[0] == 1:
        print(decode_tokendb_v1(data))
    elif data[0] == 2:
        print(decode_tokendb_v2(data))
    elif data[0] == 3:
        print(decode_tokendb_v3(data))
