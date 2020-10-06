try:
    from importlib.metadata import distribution
except ImportError:
    # Running on pre-3.8 Python; use importlib-metadata package
    from importlib_metadata import distribution


def _get_tesp_metadata():
    dist = distribution("tesp")
    return {
        "software_versions": {
            "tesp": {"version": dist.version, "repo_url": dist.metadata.get("Home-page")}
        }
    }
