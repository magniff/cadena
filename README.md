# Cadena

[![Build Status](https://api.travis-ci.org/magniff/cadena.svg?branch=develop)](https://travis-ci.org/magniff/cadena)
[![codecov](https://codecov.io/gh/magniff/cadena/branch/develop/graph/badge.svg)](https://codecov.io/gh/magniff/cadena)

```python3
>>> from cadena.drivers.alchemy.helpers import new_sqlite_inmem_driver
>>>
>>> driver = new_sqlite_inmem_driver()
>>> key = driver.store(data=b"helloworld", links=[])
>>> print(key)
b'\x93j\x18\\\xaa\xa2f\xbb\x9c\xbe\x98\x1e\x9e\x05\xcbx\xcds+\x0b2\x80\xeb\x94D\x12\xbbo\x8f\x8f\x07\xaf'
>>>
>>> lookup_result = driver.lookup(node_id=key)
>>> lookup_result
DefaultLinkedNode<data=10;links=0>
>>> lookup_result.data
b'helloworld'
>>> lookup_result.links
[]
>>>
```

