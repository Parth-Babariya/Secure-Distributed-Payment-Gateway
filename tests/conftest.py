import logging

import pytest

from harness import System, ensure_certs

logging.basicConfig(level=logging.WARNING)


@pytest.fixture(scope="session")
def cert_dir(tmp_path_factory):
    return ensure_certs(tmp_path_factory.mktemp("certs"))


@pytest.fixture
def make_system(cert_dir, tmp_path):
    """Factory for a running system; everything is stopped after the test."""
    systems = []

    def make(**gateway_options):
        system = System(cert_dir, tmp_path / "data", **gateway_options).start()
        systems.append(system)
        return system

    yield make
    for system in systems:
        system.stop()


@pytest.fixture
def system(make_system):
    return make_system()
