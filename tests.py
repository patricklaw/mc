# Copyright 2011 Patrick Lawson
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
# See LICENSE in the root of this repository for more information

import client

c = client.Client('localhost', 11211)
c.set('foozle', 'baz')
assert c.get('foozle') == 'baz'