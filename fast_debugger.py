"""Fast local debug entrance, if you want to know exactly how your pipeline is executed step by step"""

from unittest.mock import patch
from sqlflow.cli.main import app
from typer.testing import CliRunner

def main(): 
    runner = CliRunner()
    with patch("os.getcwd", return_value='<path_to_your_local_project,>'):
        result = runner.invoke(app, ["pipeline", "run", "<your_local_pipeline>"])
        print(result)

if __name__ == '__main__':
    main()
