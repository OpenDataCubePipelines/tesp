from functools import partial
from pprint import pprint

import yaml
from click.testing import CliRunner, Result
from deepdiff import DeepDiff

from tesp.prepare import ls_usgs_l1_prepare

diff = partial(DeepDiff, significant_digits=6)


def check_prepare_outputs(input_dataset, expected_doc, output_path, expected_metadata_path):
    run_prepare_cli(
        '--absolute-paths',
        '--output', str(output_path),
        str(input_dataset),
    )
    assert expected_metadata_path.exists()
    # print(expected_metadata_path.read_text())
    generated_doc = yaml.safe_load(expected_metadata_path.open())
    print(repr(generated_doc))
    assert generated_doc['id'] is not None
    assert diff(
        generated_doc, expected_doc,
        exclude_paths={"root['id']"}
    ) == {}


def run_prepare_cli(*args, expect_success=True) -> Result:
    """Run the prepare script as a command-line command"""
    __tracebackhide__ = True

    res: Result = CliRunner().invoke(
        ls_usgs_l1_prepare.main,
        args,
        catch_exceptions=False
    )
    if expect_success:
        assert res.exit_code == 0, res.output

    return res
