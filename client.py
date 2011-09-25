# Copyright 2011 Patrick Lawson
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
# See LICENSE in the root of this repository for more information

import socket
import re
DEBUG = True

def parse_retrieve(s):
    values = []
    value_pattern = '^VALUE (?P<key>\S+) (?P<flags>\d+) (?P<bytes>\d+)( (?P<cas_unique>\d+))?$'
    value_re = re.compile(value_pattern)
    
    while len(s) > 0 and s[:5] != 'END\r\n':
        # Get a VALUE line
        (value_line, delim, rest) = s.partition('\r\n')
        m = value_re.match(value_line)
        if m:
            value_line_dict = m.groupdict()
            # Number of bytes to expect in the DATA block
            bytes = int(value_line_dict['bytes'])
            # Key that this value corresponds to
            key = value_line_dict['key']
            # The next `bytes` characters are the value we want
            value = rest[:bytes]
            values.append((key, value))
            # Clip the VALUE line and the response bytes off the beginning of s
            s = rest[bytes:]
            # Shouldn't really need to check that s still has bytes,
            # since we expect there to be an END\r\n, but it doesn't
            # hurt.
            if len(s) > 1 and s[:2] == '\r\n':
                # We also need to trim off the \r\n from the DATA block
                s = s[2:]
        else:
            print 'Error matching value line:'
            print value_line
            break
    
    return values
        
    

class Client(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sock = socket.socket()
        self.sock.connect((host, port))
    
    def get(self, key):
        cmd_template = 'get %(key)s\r\n'
        
        cmd = cmd_template % vars()
        if DEBUG:
            print 'cmd: %s' % cmd
        
        sock_ret = self.sock.send(cmd)
        if DEBUG:
            print 'sock_ret: %s' % sock_ret
        
        sock_resp = self.sock.recv(2 ** 12)
        if DEBUG:
            print 'sock_resp: %s' % sock_resp
        
        values = parse_retrieve(sock_resp)
        if values:
            return values[0][1]
        else:
            return None
    
    def set(self, key, val, exptime=0):
        cmd_template = 'set %(key)s %(flags)s %(exptime)s %(bytes)s\r\n'
        data_template = '%(val)s\r\n'
        bytes = len(val)
        flags = 0
        
        cmd = cmd_template % vars()
        if DEBUG:
            print 'cmd: %s' % cmd
        
        sock_ret = self.sock.send(cmd)
        if DEBUG:
            print 'sock_ret: %s' % sock_ret
        
        data = data_template % vars()
        if DEBUG:
            print 'data: %s' % data
        
        sock_ret2 = self.sock.send(data)
        if DEBUG:
            print 'sock_ret2: %s' % sock_ret2
        
        sock_resp = self.sock.recv(2 ** 12)
        if DEBUG:
            print 'sock_resp: %s' % sock_resp
        return None
        