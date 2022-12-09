import os
import subprocess
import datetime
from django.utils.version import get_version


VERSION = (0, 9, 0, 'final', 0)     # 'alpha', 'beta', 'rc', 'final'


def get_git_changeset():
    # Repository may not be found if __file__ is undefined, e.g. in a frozen
    # module.
    if "__file__" not in globals():
        return None
    repo_dir = os.path.dirname(os.path.abspath(__file__))
    git_log = subprocess.run(
        "git log --pretty=format:%ct --quiet -1 HEAD",
        capture_output=True,
        shell=True,
        cwd=repo_dir,
        text=True,
    )
    timestamp = git_log.stdout
    tz = datetime.timezone.utc
    try:
        timestamp = datetime.datetime.fromtimestamp(int(timestamp), tz=tz)
    except ValueError:
        return None
    return timestamp.strftime("%Y/%m/%d %H:%M:%S")


__version__ = get_version(VERSION)
__version_timestamp__ = get_git_changeset()
