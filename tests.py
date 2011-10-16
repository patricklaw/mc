# Copyright 2011 Patrick Lawson
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
# See LICENSE in the root of this repository for more information

import client

c = client.Client('localhost', 11211)
c.set('foozle', 'baz')
assert c.get('foozle') == 'baz'

c.get('bar')

c.set('bar', 'muck')
c.get_multi(['foozle', 'bar'])

try:
    test_response = 'VALUE foozle 0 3\r\nbaz\r\nEND\r\n'
    rdfa = client.RetrieveDFA()
    rdfa.consume_bytes(test_response)
    # print rdfa.state
    assert rdfa.state == 'FINISHED'
    results = rdfa.get_result()
    assert len(results) == 1
    assert results['foozle'] == 'baz'
except KeyboardInterrupt:
    rdfa.debug_info()

try:
    test_response = 'VALUE foozle 0 3\r\nbaz\r\nVALUE bar 0 4\r\nmuck\r\nEND\r\n'
    rdfa = client.RetrieveDFA()
    rdfa.consume_bytes(test_response)
    # print rdfa.state
    assert rdfa.state == 'FINISHED'
    results = rdfa.get_result()
    assert len(results) == 2
    assert results['bar'] == 'muck'
    assert results['foozle'] == 'baz'
except KeyboardInterrupt:
    rdfa.debug_info()


try:
    test_response = 'VALUE foozle 0 3\r\nbaz\r\nVALUE bar 0 4\r\nmuck\r\nEND\r\n'
    rdfa = client.RetrieveDFA()
    for c in test_response:
        rdfa.consume_bytes(str(c))
    # print rdfa.state
    assert rdfa.state == 'FINISHED'
    results = rdfa.get_result()
    assert len(results) == 2
    assert results['bar'] == 'muck'
    assert results['foozle'] == 'baz'
except KeyboardInterrupt:
    rdfa.debug_info()