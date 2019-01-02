# caΔena

[![Build Status](https://api.travis-ci.org/magniff/cadena.svg?branch=develop)](https://travis-ci.org/magniff/cadena)
[![codecov](https://codecov.io/gh/magniff/cadena/branch/develop/graph/badge.svg)](https://codecov.io/gh/magniff/cadena)
[![PyPI version](https://badge.fury.io/py/cadena.png)](https://badge.fury.io/py/cadena)

```bash
$ cat > hello.txt
hello world contents
$
$ store hello.txt
./hello.txt
d2b19d07afc234fbd8b47813032d0ad21c21f20fe8e966359009b42651034a33
$
$ probe d2b19d07afc234fbd8b47813032d0ad21c21f20fe8e966359009b42651034a33
{
    "parents": [],
    "tree": "6ffaf2e50c275ae1c2876848e0a640c80ebf1f6c4c9b74f9f7c2014aa055f52d",
    "type": "Commit"
}
$
$ probe 6ffaf2e50c275ae1c2876848e0a640c80ebf1f6c4c9b74f9f7c2014aa055f52d
{
    "subtrees": {
        "hello.txt": "204b5f29880c02a1b431d109710171ebb8c7aad5f62927f07b0c0fc1f05a5e00"
    },
    "type": "Tree"
}
$
$ probe 204b5f29880c02a1b431d109710171ebb8c7aad5f62927f07b0c0fc1f05a5e00
{
    "data": "aGVsbG8gd29ybGQgY29udGVudHMK",
    "type": "Blob"
}
$
$ echo "aGVsbG8gd29ybGQgY29udGVudHMK" | base64 -d -
hello world contents
```

