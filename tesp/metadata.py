from tesp.version import __version__, REPO_URL


def _get_eugl_metadata():
    return {
        'software_versions': {
            'tesp': {
                'version': __version__,
                'repo_url': REPO_URL,
            }
        }
    }
