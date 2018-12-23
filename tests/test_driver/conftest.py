import py.test


skip_slow_marker = py.test.mark.skip(reason="need --runslow option to run")


def pytest_addoption(parser):
    parser.addoption(
        "--runslow", action="store_true",
        default=False, help="run slow tests"
    )


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--runslow"):
        for item in filter(lambda item: "slow" in item.keywords, items):
            item.add_marker(skip_slow_marker)

