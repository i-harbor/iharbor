import os
import subprocess
import datetime
from django.utils.version import get_version


VERSION = (1, 0, 0, 'rc', 2)     # 'alpha', 'beta', 'rc', 'final'


def get_git_changeset():
    # Repository may not be found if __file__ is undefined, e.g. in a frozen
    # module.
    if "__file__" not in globals():
        return None
    repo_dir = os.path.dirname(os.path.abspath(__file__))

    # %(*refname) %(*authorname) %(*authoremail) %(*authordate) %(*subject)
    get_tag = subprocess.run(
        "git for-each-ref --count=3 --sort='-taggerdate' "
        "--format='%(refname:short) || %(taggerdate:format:%s) || %(*authorname) || %(*authoremail) || %(subject)'"
        " refs/tags/*",
        capture_output=True,
        shell=True,
        cwd=repo_dir,
        text=True,
    )
    try:
        cmd_output = get_tag.stdout
        lines = cmd_output.split('\n')[0:3]
        tz = datetime.timezone.utc
        git_tag_info = []
        for line in lines:
            tag = line.split('||')
            if len(tag) == 5:
                tag[1] = datetime.datetime.fromtimestamp(int(tag[1]), tz=tz)
                tag[4] = tag[4].replace('*', '\n*')
                git_tag_info.append(tag)

    except Exception:
        return None

    return git_tag_info


__version__ = get_version(VERSION)
__version_git_change_set_ = get_git_changeset()
