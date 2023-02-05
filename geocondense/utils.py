import os
import subprocess
from sys import platform
from typing import Any, Dict, List, Optional, Set, Tuple, Union  # noqa


def popen(cmd: Union[str, List[str]]) -> Tuple[int, str, str]:
    if isinstance(cmd, str):
        cmd = cmd.split(" ")
    p = subprocess.Popen(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    out, err = p.communicate()
    return p.returncode, str(out, "utf-8"), str(err, "utf-8")


def md5sum(path: str) -> str:
    assert os.path.isfile(path), f"{path} does not exist"
    if platform == "linux" or platform == "linux2":
        retcode, stdout, stderr = popen(["md5sum", path])
        assert retcode == 0
        md5 = stdout.split()[0]
    elif platform.startswith("darwin"):
        retcode, stdout, stderr = popen(["md5", path])
        assert retcode == 0
        md5 = stdout.strip().split()[-1]
    else:
        raise Exception("not ready")
    assert len(md5) == 32
    return md5
