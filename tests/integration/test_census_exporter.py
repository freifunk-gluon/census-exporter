import json
from importlib.resources import files
from importlib.resources.abc import Traversable
from pathlib import Path

import pytest
from click.testing import CliRunner
from pytest_httpserver import HTTPServer

import tests.resources
from gluon_census_exporter.__main__ import main


@pytest.fixture
def meshviewer_path() -> Traversable:
    return files(tests.resources).joinpath("meshviewer.json")


@pytest.fixture
def meshviewer_data(meshviewer_path: Traversable) -> dict:
    with meshviewer_path.open() as meshviewer_handle:
        return json.load(meshviewer_handle)


def test_read_meshviewer_resource(meshviewer_path: Traversable) -> None:
    with meshviewer_path.open() as mv:
        assert mv.read()


def test_main() -> None:
    runner = CliRunner()
    output_filename = "test_output.prom"

    with runner.isolated_filesystem():
        with Path("communities.json").open("w") as f:
            f.write("{}")

        result = runner.invoke(main, [output_filename])
        assert result.exit_code == 0
        assert "duplicate" in result.output
        assert "unique" in result.output

        with Path(output_filename).open("r") as p:
            content = p.read()
            for total in (
                "gluon_base_total",
                "gluon_model_total",
                "gluon_domain_total",
            ):
                assert total in content


def test_fixed_main(httpserver: HTTPServer, meshviewer_data: dict) -> None:
    """Verify the program counts right.

    Spawn a webserver with a meshviewer.json and feed the exporter a matching
    communities.json.

    Check that the meshviewer.json is read just once and is evaluated properly.
    """
    runner = CliRunner()
    output_filename = "test_output.prom"

    httpserver.expect_oneshot_request("/meshviewer.json").respond_with_json(
        meshviewer_data,
    )

    with runner.isolated_filesystem():
        with Path("communities.json").open("w") as f:
            json_str = json.dumps(
                {"Demotown": [httpserver.url_for("/meshviewer.json")]},
            )
            f.write(json_str)

        result = runner.invoke(main, [output_filename])
        assert result.exit_code == 0
        assert "format=meshviewer" in result.output
        assert "duplicate=0" in result.output
        assert "unique=801" in result.output

        with Path(output_filename).open("r") as p:
            content = p.read()
            for total, amount in (
                ("gluon_base_total{", 7),
                ("gluon_model_total{", 110),
                ("gluon_domain_total{", 21),
            ):
                assert content.count(total) == amount

    httpserver.check_assertions()
