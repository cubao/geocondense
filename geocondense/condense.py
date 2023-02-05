import json
import os
from itertools import chain
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import numpy as np
import open3d as o3d
from loguru import logger
from pprint import pprint

from geocondense.utils import md5sum


def resolve_center(
    center: Optional[Union[str, Tuple[float, float, float]]]
) -> Optional[Tuple[float, float, float]]:
    if not center:
        return None
    if isinstance(center, str):
        center = [float(x) for x in center.split(",")]
    assert isinstance(center, (list, tuple)), f"invalid center: {center}"
    lon, lat, alt = (*center, 0.0) if len(center) == 2 else tuple(center)
    return lon, lat, alt


def default_handle_semantic(path: str, *, workdir: str, uuid: str) -> Tuple[str, str]:
    dissect_input_path = path
    condense_input_path = path
    return dissect_input_path, condense_input_path


def default_handle_pointcloud(
    path: str,
    *,
    workdir: str,
    uuid: str,
    center: Optional[Tuple[float, float, float]],
) -> o3d.geometry.PointCloud:
    return o3d.io.read_point_cloud(path)


def condense_semantic(
    dissect_input_path: str, condense_input_path: str, *, output_dir: str
) -> str:
    raise Exception("not ready")


def condense_pointcloud(pcd: o3d.geometry.PointCloud, *, output_dir: str) -> str:
    raise Exception("not ready")


@logger.catch(reraise=True)
def condense(
    *,
    workdir: str,
    semantic_files: Union[str, List[str]] = None,
    pointcloud_files: Union[str, List[str]] = None,
    center: Optional[Tuple[float, float, float]] = None,
    # handlers
    handle_semantic=default_handle_semantic,
    handle_pointcloud=default_handle_pointcloud,
) -> List[str]:
    assert not (
        semantic_files is None and pointcloud_files is None
    ), f"should specify either --semantic_files or --pointcloud_files"
    semantic_files = semantic_files or []
    pointcloud_files = pointcloud_files or []
    if isinstance(semantic_files, str):
        semantic_files = semantic_files.split(',')
    if isinstance(pointcloud_files, str):
        pointcloud_files = pointcloud_files.split(',')
    logger.info(f'semantic files: {semantic_files} (#{len(semantic_files)})')
    logger.info(f'pointcloud files: {pointcloud_files} (#{len(pointcloud_files)})')

    for p in [*semantic_files, *pointcloud_files]:
        assert os.path.isfile(p), f"{p} does not exist"
    center = resolve_center(center)
    os.makedirs(os.path.abspath(workdir), exist_ok=True)
    index_map = {}
    for path in semantic_files:
        uuid = md5sum(path)
        odir = f"{workdir}/{uuid}"
        sentinel = f"{odir}/condensed"
        if os.path.isfile(sentinel):
            logger.info(f"skip condensing {path}, sentinel exists: {sentinel}")
            continue
        dissect_input, condense_input = handle_semantic(
            path,
            workdir=workdir,
            uuid=uuid,
        )
        index = condense_semantic(dissect_input, condense_input, output_dir=odir)
        index_map[path] = index
        Path(sentinel).touch()
    for path in pointcloud_files:
        uuid = md5sum(path)
        odir = f"{workdir}/{uuid}"
        sentinel = f"{odir}/condensed"
        if os.path.isfile(sentinel):
            logger.info(f"skip condensing {path}, sentinel exists: {sentinel}")
            continue
        pcd = handle_pointcloud(
            path,
            workdir=workdir,
            uuid=uuid,
            center=center,
        )
        index = condense_pointcloud(pcd, output_dir=odir)
        index_map[path] = index
        Path(sentinel).touch()
    return index_list


if __name__ == "__main__":
    import fire

    fire.core.Display = lambda lines, out: print(*lines, file=out)
    fire.Fire(condense)
