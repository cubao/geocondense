from __future__ import annotations

import json
import os
import subprocess
from sys import platform

from loguru import logger


def popen(cmd: str | list[str]) -> tuple[int, str, str]:
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
        msg = "not implemented on this platform"
        raise Exception(msg)
    assert len(md5) == 32
    return md5


def read_json(path: str) -> dict:
    path = os.path.abspath(path)
    with open(path) as f:
        return json.load(f)


def write_json(path: str, data: dict, *, verbose: bool = True) -> str:
    path = os.path.abspath(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    if verbose:
        logger.info(f"wrote to {path}")
    return path
