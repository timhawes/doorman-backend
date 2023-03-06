import binascii
import io

import cdblib


def encode_cdb(data):
    """
    Encodes a dict of "uid: name"
    into cdb format.
    """
    with io.BytesIO() as f:
        with cdblib.Writer(f) as writer:
            for hexuid in sorted(data.keys()):
                uid = binascii.unhexlify(hexuid)
                try:
                    user = data[hexuid].encode('us-ascii')
                except UnicodeEncodeError:
                    user = b""
                writer.put(bytes([0x01]) + uid, bytes([0x01]) + user)
        return f.getvalue()
