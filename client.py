# Copyright 2011 Patrick Lawson
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
# See LICENSE in the root of this repository for more information

import socket
import re
import sys
DEBUG = False

VALUE_HEADER_PATTERN = '^VALUE (?P<key>\S+) (?P<flags>\d+) (?P<bytes>\d+)( (?P<cas_unique>\d+))?$'
VALUE_RE = re.compile(VALUE_HEADER_PATTERN)

# This class simulates the behavior of a DFA which blindly consumes
# bytes from the reading socket and outputs key/value pairs with
# associated flags.
# It is an error to call consume_bytes after the FINISHED state
# has been reached.
# It is an error to call consume_bytes after any ERROR state has
# been reached.
# As soon as an ERROR state is reached, an exception will be raised.
# This class is designed with chunked socket reading in mind.

class RetrieveDFA(object):
    def __init__(self):
        self.values = {}
        self.state = 'BEGIN'
        self.current_key = None
        self.buffer = ''
        self.block_bytes_remaining = None
        self.block_bytes = str()
    
    def debug_info(self):
        info_template =\
            """
            values: %(values)s\n
            state: %(state)s\n
            current_key: %(current_key)s\n
            buffer: %(buffer)s\n
            block_bytes_remaining: %(block_bytes_remaining)s\n
            block_bytes: %(block_bytes)s\n
            """ % {key: repr(val) for key, val in self.__dict__.items()}
        print info_template
    
    def consume_bytes(self, bytes):
        self.buffer = self.buffer + bytes
        while True:
            # self.debug_info()
            if self.state == 'BUILD_VALUE_HEADER':
                (value_line, delim, rest) = self.buffer.partition('\r\n')
                m = VALUE_RE.match(value_line)
                if m and delim == '\r\n':
                    value_line_dict = m.groupdict()
                    self.state = 'CONSUME_VALUE_BLOCK'
                    self.current_key = value_line_dict['key']
                    self.block_bytes_remaining = int(value_line_dict['bytes'])
                    self.block_bytes = str()
                    self.buffer = rest
                    continue
                elif self.buffer == 'END\r\n':
                    self.state = 'FINISHED'
                    self.buffer = str()
                    break
                else:
                    break
            
            elif self.state == 'CONSUME_VALUE_BLOCK':
                if len(self.buffer) >= self.block_bytes_remaining:
                    self.block_bytes = self.block_bytes + self.buffer[:self.block_bytes_remaining]
                    self.buffer = self.buffer[self.block_bytes_remaining:]
                    self.block_bytes_remaining = 0
                    self.state = 'CONSUME_VALUE_BLOCK_ENDLINE'
                    self.values[self.current_key] = self.block_bytes
                    continue
                elif len(self.buffer) == 0:
                    break
                else:
                    self.block_bytes = self.block_bytes + self.buffer
                    self.block_bytes_remaining -= len(self.buffer)
                    self.buffer = ''
            
            elif self.state == 'CONSUME_VALUE_BLOCK_ENDLINE':
                if len(self.buffer) >= 2 and self.buffer[:2] == '\r\n':
                    self.buffer = self.buffer[2:]
                    self.state = 'BEGIN'
                    continue
                else:
                    break
            
            elif self.state == 'BEGIN':
                self.state = 'BUILD_VALUE_HEADER'
                continue
    
    def get_result(self):
        if self.state == 'FINISHED':
            return self.values
        else:
            return 'ERROR'
        

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
            print 'sock_resp: %s' % repr(sock_resp)
        
        # print repr(sock_resp)
        
        values = parse_retrieve(sock_resp)
        
        if values:
            return values[0][1]
        else:
            return None
    
    def get_multi(self, keys):
        cmds = ' '.join(keys)
        cmd = 'get %(cmds)s\r\n' % vars()
        if DEBUG:
            print 'cmd: %s' % cmd
        
        sock_ret = self.sock.send(cmd)
        if DEBUG:
            print 'sock_ret: %s' % sock_ret
        
        sock_resp = self.sock.recv(2 ** 12)
        if DEBUG:
            print 'sock_resp: %s' % repr(sock_resp)
        
        # print repr(sock_resp)
        
        values = parse_retrieve(sock_resp)
        
        return values
        
    
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
        