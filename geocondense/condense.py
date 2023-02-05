import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union  # noqa

import open3d as o3d
import polyline_ruler.tf as tf
from loguru import logger

from geocondense.condense_geojson import condense_geojson
from geocondense.condense_pointcloud import condense_pointcloud_impl
from geocondense.dissect_geojson import dissect_geojson
from geocondense.utils import md5sum, write_json


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
    pcd = o3d.io.read_point_cloud(path)
    if center:
        lon, lat, alt = center
        pcd.transform(tf.T_ecef_enu(lon=lon, lat=lat, alt=alt))
    return pcd


def condense_semantic(
    dissect_input_path: str,
    condense_input_path: str,
    *,
    output_dir: str,
) -> str:
    dissect_geojson(
        input_path=dissect_input_path,
        output_geometry=f"{output_dir}/geometry.json",
        output_properties=f"{output_dir}/properties.json",
        output_observations=f"{output_dir}/obseravtions.json",
        output_others=f"{output_dir}/others.json",
        indent=True,
    )
    condense_geojson(
        input_path=condense_input_path,
        output_index_path=f"{output_dir}/meta.json",
        output_strip_path=f"{output_dir}/main.json",
        output_grids_dir=f"{output_dir}/grids",
        indent=True,
    )
    path = f"{output_dir}/index.json"
    write_json(
        path,
        {
            "main": "main.json",
            "grids": "grids",
            # TODO
        },
    )
    Path(f"{output_dir}.semantic").touch()
    return path


def condense_pointcloud(
    pcd: o3d.geometry.PointCloud,
    *,
    output_dir: str,
) -> str:
    condense_pointcloud_impl(
        pcd=pcd,
        output_fence_path=f"{output_dir}/main.json",
        output_grids_dir=f"{output_dir}/grids",
        grid_resolution=0.0001,
    )
    path = f"{output_dir}/index.json"
    write_json(
        path,
        {
            "main": "main.json",
            "grids": "grids",
            # TODO
        },
    )
    Path(f"{output_dir}.pointcloud").touch()
    return path


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
        write_json(export, index, verbose=False)
    else:
        logger.info(f"export: {json.dumps(index, indent=4)}")


if __name__ == "__main__":
    main()
