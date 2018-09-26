from functools import partial
from pathlib import Path

import yaml
from click.testing import CliRunner
from deepdiff import DeepDiff

from tesp.prepare import ls_usgs_l1_prepare

diff = partial(DeepDiff, significant_digits=6)
L1GT_TARBALL_PATH: Path = Path(__file__).parent / 'data' / 'LE07_L1GT_104078_20131209_20161119_01_T2.tar.gz'


def test_prepare_l1_tarball(tmpdir):
    assert L1GT_TARBALL_PATH.exists(), "Test data missing(?)"

    output_path = Path(tmpdir)

    res = CliRunner().invoke(
        ls_usgs_l1_prepare.main,
        (
            '--output', str(output_path),
            '--date', '3/12/2017',
            str(L1GT_TARBALL_PATH)
        ),
        catch_exceptions=False
    )
    assert res.exit_code == 0, res.output

    expected_metadata_path = output_path / 'LE07_L1GT_104078_20131209_20161119_01_T2.yaml'
    assert expected_metadata_path.exists()

    # print(expected_metadata_path.read_text())
    doc = yaml.safe_load(expected_metadata_path.open())

    assert doc['id'] is not None
    assert doc['processing_level'] == 'L1GT'
    assert doc['label'] == 'LE71040782013343ASA00'

    assert diff(
        doc['extent'],
        {
            'center_dt': '2013-12-09 01:10:46.6908469Z',
            'coord': {
                'll': {'lat': -26.954354903739272, 'lon': 129.22769548746237},
                'lr': {'lat': -26.928774471741676, 'lon': 131.69588786021484},
                'ul': {'lat': -25.03371801797915, 'lon': 129.22402339673468},
                'ur': {'lat': -25.01021635833341, 'lon': 131.65247288941694}
            },
            'from_dt': '2013-12-09 01:10:46.6908469Z',
            'to_dt': '2013-12-09 01:10:46.6908469Z'
        }
    ) == {}

    assert (
            doc['image']['bands']['blue']['path'] ==
            f'tar:{L1GT_TARBALL_PATH.absolute()}!LE07_L1GT_104078_20131209_20161119_01_T2_B1.TIF'
    )
    supplied_bands = set(doc['image']['bands'].keys())
    assert {'red', 'green', 'blue', 'swir2', 'nir', 'quality', 'swir1'} == supplied_bands
