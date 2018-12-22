import snapshot.drivers.memdict as memdict


def test_basic():

    container = memdict.MemdictDriver()
    storage_key = container.store(b"helloworld", links=[])
    retrieved_value = container.retrieve(storage_key)

    assert len(container) == 1
    assert storage_key
    assert (
        retrieved_value.data == b"helloworld" and
        retrieved_value.links == []
    )


def test_uc0():

    container = memdict.MemdictDriver()
    data_node_key = container.store(b"huge_string_of_bytes", links=[])
    value0_key = container.store(data=b"value0_data", links=[data_node_key])
    value1_key = container.store(data=b"value1_data", links=[data_node_key])
    assert value0_key != value1_key

    assert len(container) == 3
    value0 = container.retrieve(value0_key)
    value1 = container.retrieve(value1_key)

    assert value0.data == b"value0_data"
    assert value1.data == b"value1_data"
    assert value0.links == value1.links

