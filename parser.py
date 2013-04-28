from bitstring import ConstBitStream
from collections import namedtuple

MCResponse = namedtuple('MCResponse',
                        ['opcode', 'data_type', 'status', 'cas', 'extras',
                         'key', 'value'])
def mc_parser():
    out, buff = yield

    while True:
        header_bytes = yield from buff.read(24)
        header = ConstBitStream(header_bytes)

        readlist_fmt = 'uint:8, uint:8, uint:16'
        magic, opcode, key_length = header.readlist(readlist_fmt)
        extras_length, data_type, status = header.readlist(readlist_fmt)
        total_body_length = header.read('uint:32')
        opaque = header.read('uint:32')
        cas = header.read('uint:64')

        extras = None
        if extras_length:
            extras = yield from buff.read(extras_length)

        key = None
        if key_length:
            key = yield from buff.read(key_length)

        value_length = total_body_length - (key_length + extras_length)
        value = None
        if value_length:
            value = yield from buff.read(value_length)

        out.feed_data(MCResponse(opcode, data_type, status, cas,
                                 extras, key, value))

