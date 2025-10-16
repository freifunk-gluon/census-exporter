import importlib
from pathlib import Path

from click.testing import CliRunner

census_exporter = importlib.import_module("census-exporter")


def test_main() -> None:
    runner = CliRunner()
    output_filename = "test_output.prom"

    with runner.isolated_filesystem():
        with Path("communities.json").open("w") as f:
            f.write("{}")

        result = runner.invoke(census_exporter.main, [output_filename])
        assert result.exit_code == 0
        assert result.output == "0 unique nodes\n0 duplicates skipped\n"

        with Path(output_filename).open("r") as p:
            content = p.read()
            for total in (
                "gluon_base_total",
                "gluon_model_total",
                "gluon_domain_total",
            ):
                assert total in content
