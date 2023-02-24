import os
import subprocess
import datetime
from django.utils.version import get_version


VERSION = (1, 0, 0, 'rc', 1)     # 'alpha', 'beta', 'rc', 'final'


def get_git_changeset():
    # Repository may not be found if __file__ is undefined, e.g. in a frozen
    # module.
    if "__file__" not in globals():
        return None
    repo_dir = os.path.dirname(os.path.abspath(__file__))
    git_tag_info = {}
    # %(*refname) %(*authorname) %(*authoremail) %(*authordate) %(*subject)
    get_tag = subprocess.run(
        'git for-each-ref --count=3 --sort="-*authordate" --format="%(*refname) %(*authorname) %(*authoremail) '
        '%(*authordate) " "refs/tags"',
        capture_output=True,
        shell=True,
        cwd=repo_dir,
        text=True,
    )
    try:
        cmd_output = get_tag.stdout.split('\n')
        tag_list = [item.split(' ', 3) for item in cmd_output[0:3]]

        for info in tag_list:
            tag = info[0].lstrip('refs/tags/').rstrip('^{}')
            t = datetime.datetime.strptime(info[3].rstrip(' +0800'), '%a %b %d %H:%M:%S %Y')
            git_tag_info[tag] = [info[1], info[2], t]  # [作者,邮箱,时间]

            get_tag_desc = subprocess.run(
                f'git tag --sort="-*authordate" -n10 -l {tag}',
                capture_output=True,
                shell=True,
                cwd=repo_dir,
                text=True,
            )
            cmd_output_tag_doc = get_tag_desc.stdout.split('\n')
            tag_doc_list = [" ".join(item.split()).replace("*", "").replace(tag, "")
                            for item in cmd_output_tag_doc[0:len(cmd_output_tag_doc) -1]]
            git_tag_info[tag].append(tag_doc_list)

    except Exception:
        return None

    return git_tag_info


__version__ = get_version(VERSION)
__version_git_change_set_ = get_git_changeset()
