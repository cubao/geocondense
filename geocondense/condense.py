import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union  # noqa

import open3d as o3d
from loguru import logger

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
    semantic_files: List[str] = None,
    pointcloud_files: List[str] = None,
    center: Optional[Tuple[float, float, float]] = None,
    # handlers
    handle_semantic=default_handle_semantic,
    handle_pointcloud=default_handle_pointcloud,
) -> Dict[str, str]:
    assert not (
        semantic_files is None and pointcloud_files is None
    ), "should specify either --semantic_files or --pointcloud_files"
    semantic_files = semantic_files or []
    pointcloud_files = pointcloud_files or []
    logger.info(f"semantic files: {semantic_files} (#{len(semantic_files)})")
    logger.info(f"pointcloud files: {pointcloud_files} (#{len(pointcloud_files)})")

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
    return index_map


def main(
    handle_semantic=default_handle_semantic,
    handle_pointcloud=default_handle_pointcloud,
):
    prog = "python3 -m geocondense.condense"
    description = "condense semantic & pointcloud"
    parser = argparse.ArgumentParser(prog=prog, description=description)
    parser.add_argument(
        "workdir",
        type=str,
        help="workdir",
    )
    parser.add_argument(
        "--center",
        type=str,
        help='specify with --center="lon,lat,alt" or --center="path/to/center.txt"',
    )
    parser.add_argument(
        "--semantic_files",
        nargs="*",
        type=str,
        help="semantic files (geojson or osm)",
    )
    parser.add_argument(
        "--pointcloud_files",
        nargs="*",
        type=str,
        help="pointcloud files (pcd or other pointcloud file (you should write your own handle_pointcloud))",
    )
    parser.add_argument(
        "--export",
        type=str,
        help="export to json",
    )
    args = parser.parse_args()
    workdir: str = args.workdir
    semantic_files: List[str] = args.semantic_files
    pointcloud_files: List[str] = args.pointcloud_files
    center: Optional[str] = args.center
    export: Optional[str] = args.export
    args = None

    index = condense(
        workdir=workdir,
        semantic_files=semantic_files,
        pointcloud_files=pointcloud_files,
        center=center,
        handle_semantic=handle_semantic,
        handle_pointcloud=handle_pointcloud,
    )
    if export:
        logger.info(f"export to {export}")
        os.makedirs(os.path.dirname(os.path.abspath(export)), exist_ok=True)
        with open(export, "w") as f:
            json.dump(index, f, indent=4)
    else:
        logger.info(f"export: {json.dumps(index, indent=4)}")


if __name__ == "__main__":
    main()
