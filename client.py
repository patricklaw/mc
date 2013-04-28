# Copyright 2011 Patrick Lawson
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
# See LICENSE in the root of this repository for more information

import tulip
from parser import mc_parser
from bitstring import pack

REQ_PACK_FORMAT = ','.join([
    'hex:8=magic', 'uint:8=opcode', 'int:16=key_len',
    'uint:8=extra_len', 'uint:8=data_type', 'uint:16=reserved',
    'uint:32=total_body_len',
    'uint:32=opaque',
    'uint:64=cas',
])

def make_request_packet(opcode, data_type, reserved, opaque, cas,
                        extras=bytes(), key=bytes(), value=bytes()):
    total_body_len = len(extras) + len(key) + len(value)
    header_bs = pack(REQ_PACK_FORMAT,
                     magic='0x80', opcode=opcode, key_len=len(key),
                     extra_len=len(extras), data_type=data_type,
                     reserved=reserved,
                     total_body_len=total_body_len,
                     opaque=opaque,
                     cas=cas)

    return header_bs.bytes + extras + key + value
    

class MCProtocolWriter:
    def __init__(self, transport):
        self.transport = transport

    def get(self, key, data_type=0, cas=0, opaque=0,
                       extras=None, value=None):
        request = make_request_packet(opcode=0, data_type=data_type,
                                      reserved=0, opaque=opaque, cas=cas,
                                      key=key)
        self.transport.write(request)

    def set(self, key, value, data_type=0, cas=0, opaque=0):
        extras = pack('uint:64=0').bytes
        request = make_request_packet(opcode=2, data_type=data_type,
                                      reserved=0, opaque=opaque, cas=cas,
                                      extras=extras, key=key, value=value)
        self.transport.write(request)



class AsyncClient(object):
    def __init__(self, host='localhost', port=11211, loop=None):
        self.host = host
        self.port = port
        self.loop = loop

    @tulip.task
    def connect(self):
        transport, stream = yield from self.loop.create_connection(
                                            tulip.StreamProtocol,
                                            self.host,
                                            self.port
                                       )
        self.transport = transport
        self.stream = stream
        self.reader = self.stream.set_parser(mc_parser())
        self.writer = MCProtocolWriter(self.transport)

    @tulip.task
    def get(self, key):
        self.writer.get(key)
        response = yield from self.reader.read()
        return response

    @tulip.task
    def set(self, key, value):
        self.writer.set(key, value)
        response = yield from self.reader.read()
        return response

if __name__ == '__main__':
    @tulip.task
    def test_drive_client():
        loop = tulip.get_event_loop()
        print("in test driver")
        client = AsyncClient(loop=loop)
        yield from client.connect()
        resp = yield from client.set(b'foozle', b'yay')
        print(resp)
        val = yield from client.get(b'foozle')
        print(val)
        return val


    loop = tulip.get_event_loop()
    loop.run_until_complete(test_drive_client())
