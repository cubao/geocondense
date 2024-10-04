from __future__ import annotations

import argparse
import glob
import json
import os
from collections import defaultdict
from pathlib import Path

import open3d as o3d
from loguru import logger
from polyline_ruler import tf

from geocondense.condense_geojson import condense_geojson
from geocondense.condense_pointcloud import condense_pointcloud_impl
from geocondense.dissect_geojson import dissect_geojson
from geocondense.utils import md5sum as default_md5sum
from geocondense.utils import read_json, write_json


def resolve_center(
    center: str | tuple[float, float, float] | None,
) -> tuple[float, float, float] | None:
    if not center:
        return None
    if isinstance(center, str):
        center = [float(x) for x in center.split(",")]
    assert isinstance(center, (list, tuple)), f"invalid center: {center}"
    lon, lat, alt = (*center, 0.0) if len(center) == 2 else tuple(center)
    return lon, lat, alt


def default_handle_semantic(
    path: str,
    *,
    workdir: str,
    uuid: str,
    center: tuple[float, float, float] | None,
) -> tuple[str, str]:
    dissect_input_path = path
    condense_input_path = path
    return dissect_input_path, condense_input_path


def default_handle_pointcloud(
    path: str,
    *,
    workdir: str,
    uuid: str,
    center: tuple[float, float, float] | None,
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
        output_observations=f"{output_dir}/observations.json",
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
    files = sorted(
        os.path.basename(p) for p in glob.glob(f"{output_dir}/grids/h3_cell_*.json")
    )
    grids = defaultdict(dict)  # res -> h3idx -> file
    for p in files:
        # e.g. h3_cell_8_08831aa4301fffff.json
        res, idx = p.rsplit(".", 1)[0].split("_")[2:]
        grids[res][idx] = f"grids/{p}"
    write_json(
        path,
        {
            "type": "semantic",
            "main": "main.json",
            "grids": grids,
            "meta": "meta.json",
            "geometry": "geometry.json",
            "properties": "properties.json",
            "observations": "observations.json",
            "others": "others.json",
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
    files = sorted(
        os.path.basename(p) for p in glob.glob(f"{output_dir}/grids/grid_res*.pcd")
    )
    grids = defaultdict(
        defaultdict(defaultdict(dict).copy).copy
    )  # res -> bbox -> category -> {info}
    for p in files:
        # grid_res0.0001_116.311_39.8959_116.3111_39.896_anchor_116.3114_39.8958_0.0_raw.pcd
        # grid_res0.0001_116.311_39.8958_116.3111_39.8959_anchor_116.3114_39.8958_0.0_voxel0.25.pcd
        res, lon0, lat0, lon1, lat1, _, ax, ay, az, category = p[8:-4].split("_")
        grids[res][f"{lon0},{lat0},{lon1},{lat1}"][category] = {
            "file": f"grids/{p}",
            "anchor": [float(ax), float(ay), float(az)],
        }
    write_json(
        path,
        {
            "type": "pointcloud",
            "main": "main.json",
            "grids": grids,
        },
    )
    Path(f"{output_dir}.pointcloud").touch()
    return path


@logger.catch(reraise=True)
def condense(
    *,
    workdir: str,
    semantic_files: list[str] = None,
    pointcloud_files: list[str] = None,
    center: tuple[float, float, float] | None = None,
    # handlers
    handle_semantic=default_handle_semantic,
    handle_pointcloud=default_handle_pointcloud,
    md5sum=default_md5sum,
) -> dict[str, str]:
    assert not (
        semantic_files is None and pointcloud_files is None
    ), "should specify either --semantic_files or --pointcloud_files (or both)"
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
        index = f"{odir}/index.json"
        if os.path.isfile(index):
            logger.info(f"skip condensing {path}, index exists: {index}")
            index_map[path] = {"uuid": uuid, **read_json(index)}
            continue
        dissect_input, condense_input = handle_semantic(
            path,
            workdir=workdir,
            uuid=uuid,
            center=center,
        )
        index = condense_semantic(dissect_input, condense_input, output_dir=odir)
        index_map[path] = {"uuid": uuid, **read_json(index)}
    for path in pointcloud_files:
        uuid = md5sum(path)
        odir = f"{workdir}/{uuid}"
        index = f"{odir}/index.json"
        if os.path.isfile(index):
            logger.info(f"skip condensing {path}, index exists: {index}")
            index_map[path] = {"uuid": uuid, **read_json(index)}
            continue
        pcd = handle_pointcloud(
            path,
            workdir=workdir,
            uuid=uuid,
            center=center,
        )
        index = condense_pointcloud(pcd, output_dir=odir)
        index_map[path] = {"uuid": uuid, **read_json(index)}
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
    semantic_files: list[str] = args.semantic_files
    pointcloud_files: list[str] = args.pointcloud_files
    center: str | None = args.center
    export: str | None = args.export
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
        logger.warning("you can specify --export to export to json")


if __name__ == "__main__":
    main()
