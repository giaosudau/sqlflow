"""
Fast Debugger Script for Local Pipeline Development

This script provides a convenient entry point for developers to debug SQLFlow pipelines locally.

Features:
- Allows step-by-step debugging of your pipeline using an IDE (e.g., VSCode) by simply pressing F5.
- Uses unittest.mock.patch to simulate the working directory and pipeline name, so you can run your pipeline as if invoked from the CLI.
- Prints the result of the pipeline run for inspection.

Usage:
1. Replace the placeholders below:
   - <path_to_your_local_project>: Absolute path to your local project directory.
   - <your_local_pipeline>: Name of the pipeline you want to debug.
2. Set breakpoints as needed in your code.
3. Press F5 (or your IDE's debug shortcut) to start debugging.

Example:
    with patch("os.getcwd", return_value="/Users/yourname/projects/sqlflow"):
        result = runner.invoke(app, ["pipeline", "run", "my_pipeline"])

Note:
- Ensure your environment is properly set up and dependencies are installed.
- This script is intended for local development and debugging only.

"""

from unittest.mock import patch
from sqlflow.cli.main import app
from typer.testing import CliRunner

def main(): 
    runner = CliRunner()
    with patch("os.getcwd", return_value='.local'):
        result = runner.invoke(app, ["pipeline", "run", "example"])
        print('Pipeline output: ', result.output)

if __name__ == '__main__':
    main()
