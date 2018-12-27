import setuptools


classifiers = [
    (
        "Programming Language :: Python :: %s" % x
    )
    for x in "3.5 3.6 3.7".split()
]


setuptools.setup(
    name="cadena",
    description="Wierd and inefficient Tar clone.",
    version="0.0.1",
    license="MIT license",
    platforms=["unix", "linux", "osx", "win32"],
    author="magniff",
    url="https://github.com/magniff/cadena",
    classifiers=classifiers,
    install_requires=[
        "sqlalchemy", "watch", "protobuf", "click", "pymongo",
    ],
    entry_points={
        "console_scripts": [
            "store=bin.commit:cli",
            "probe=bin.probe:cli",
        ]
    },
    packages=setuptools.find_packages(),
    zip_safe=False,
)

