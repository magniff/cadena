# Snapshot

[![Build Status](https://api.travis-ci.org/magniff/snapshot.svg?branch=develop)](https://travis-ci.org/magniff/snapshot)
[![codecov](https://codecov.io/gh/magniff/snapshot/branch/develop/graph/badge.svg)](https://codecov.io/gh/magniff/snapshot)

```python3
>>> from snapshot.drivers.alchemy.helpers import new_sqlite_inmem_driver
>>>
>>> driver = new_sqlite_inmem_driver()
>>> key = driver.store(data=b"helloworld", links=[])
>>> print(key)
b'\x93j\x18\\\xaa\xa2f\xbb\x9c\xbe\x98\x1e\x9e\x05\xcbx\xcds+\x0b2\x80\xeb\x94D\x12\xbbo\x8f\x8f\x07\xaf'
>>>
>>> lookup_result = driver.retrieve(node_id=key)
>>> lookup_result
<snapshot.drivers.common.DefaultLinkedNode object at 0x7f0403a94e10>
>>> lookup_result.data
b'helloworld'
>>> lookup_result.links
[]
>>>
```

