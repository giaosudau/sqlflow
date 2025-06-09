import os
from pathlib import Path

import yaml
from typer.testing import CliRunner

from sqlflow.cli.main import app as main_app

runner = CliRunner()


def make_profile(profiles_dir: Path, name: str, type_: str, params: dict = None):
    params = params or {}
    params["type"] = type_
    profile = {name: params}
    os.makedirs(profiles_dir, exist_ok=True)
    with (profiles_dir / "dev.yml").open("w") as f:
        yaml.safe_dump(profile, f)
    return profiles_dir / "dev.yml"


# Setup like the test
import tempfile

tmp_path = Path(tempfile.mkdtemp())
profiles_dir = tmp_path / "profiles"
csv_file = tmp_path / "dummy.csv"
csv_file.write_text("col1,col2\n1,2\n")
make_profile(profiles_dir, "csv_test", "CSV", {"path": str(csv_file)})
os.chdir(tmp_path)

# Run the command
result = runner.invoke(main_app, ["connect", "test", "csv_test", "--profile", "dev"])
print(f"Exit code: {result.exit_code}")
print(f"Output: {result.output}")
print(f"Exception: {result.exception}")

if result.exception:
    import traceback

    traceback.print_exception(
        type(result.exception), result.exception, result.exception.__traceback__
    )
