import json
from importlib.resources import files
from importlib.resources.abc import Traversable
from pathlib import Path

import pytest
from click.testing import CliRunner

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
