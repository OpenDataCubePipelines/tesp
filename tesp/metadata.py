from tesp.version import get_version, REPO_URL


def _get_tesp_metadata():
    return {
        'software_versions': {
            'tesp': {
                'version': get_version(),
                'repo_url': REPO_URL,
            }
        }
    }
