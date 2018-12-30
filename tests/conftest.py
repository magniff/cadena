import py.test


skip_slow_marker = py.test.mark.skip(reason="need --runslow option to run")


skip_external_marker = py.test.mark.skip(
    reason="need --external option to run"
)


def pytest_addoption(parser):
    parser.addoption(
        "--runslow", action="store_true", default=False, help="run slow tests"
    )
    parser.addoption(
        "--external", action="store_true",
        default=False, help="run tests for nonlocal storages"
    )


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--runslow"):
        for item in filter(lambda item: "slow" in item.keywords, items):
            item.add_marker(skip_slow_marker)

    if not config.getoption("--external"):
        for item in filter(lambda item: "external" in item.keywords, items):
            item.add_marker(skip_external_marker)

