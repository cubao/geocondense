from __future__ import annotations

import json
import os
from itertools import chain

import numpy as np
import open3d as o3d
from concave_hull import concave_hull_indexes
from loguru import logger
from polyline_ruler import douglas_simplify_mask, tf


def condense_pointcloud_impl(
    *,
    pcd: o3d.geometry.PointCloud,
    output_fence_path: str | None = None,
    output_grids_dir: str | None = None,
    grid_resolution: float = 0.0001,
    compress_pcd: bool = False,
):
    assert (
        output_fence_path or output_grids_dir
    ), "should specify either --output_fence_path or --output_grids_dir"
    if output_fence_path:
        assert output_fence_path.endswith(
            ".json"
        ), f"invalid voxel dump: {output_fence_path}, should be a json file"
    wgs84_scale = 1 / grid_resolution
    assert wgs84_scale == int(wgs84_scale), f"bad grid_resolution: {grid_resolution}"
    wgs84_scale = int(wgs84_scale)

    ecefs = np.asarray(pcd.points)
    assert len(ecefs), "not any points in pointcloud"
    R = np.linalg.norm(ecefs[0])
    assert (
        R > 6300 * 1000
    ), f"data (should be in ECEF) not on earth? (forgot to specify --center?), R is: {R}"
    anchor = tf.ecef2lla(*ecefs[0])
    anchor[:2] = [round(x * wgs84_scale) / wgs84_scale for x in anchor[:2]]
    anchor[2] = 0.0
    anchor_text = "_".join([str(x) for x in anchor])

    T_ecef_enu = tf.T_ecef_enu(*anchor)
    pcd.transform(np.linalg.inv(T_ecef_enu))
    enus = np.asarray(pcd.points)
    rgbs = np.asarray(pcd.colors)
    pmin = pcd.get_min_bound()
    pmax = pcd.get_max_bound()
    lla_bounds = tf.enu2lla(
        [pmin - 10.0, pmax + 10.0],
        anchor_lla=anchor,
        cheap_ruler=False,
    )
    lon0, lat0 = lla_bounds[0][:2]
    lon1, lat1 = lla_bounds[1][:2]
    lon0, lon1 = (round(x * wgs84_scale) / wgs84_scale for x in [lon0, lon1])
    lat0, lat1 = (round(x * wgs84_scale) / wgs84_scale for x in [lat0, lat1])
    lons = np.arange(
        lon0 - grid_resolution, lon1 + grid_resolution + 1e-15, grid_resolution
    )
    lats = np.arange(
        lat0 - grid_resolution, lat1 + grid_resolution + 1e-15, grid_resolution
    )
    lons = [round(x * wgs84_scale) / wgs84_scale for x in lons]
    lats = [round(x * wgs84_scale) / wgs84_scale for x in lats]
    assert len(lons) > 1
    assert len(lats) > 1
    xs = tf.lla2enu(
        [[x, lats[0], 0.0] for x in lons],
        anchor_lla=anchor,
        cheap_ruler=False,
    )[:, 0]
    ys = tf.lla2enu(
        [[lons[0], x, 0.0] for x in lats],
        anchor_lla=anchor,
        cheap_ruler=False,
    )[:, 1]

    pcd1, _, idxes = pcd.voxel_down_sample_and_trace(
        1.0,
        min_bound=pmin,
        max_bound=pmax,
    )
    idxes = [np.array(i) for i in idxes]
    xyzs = np.asarray(pcd1.points)

    # export voxels
    points = np.array(
        [
            *xyzs,
            *(xyzs + [+0.1, 0, 0]),
            *(xyzs + [-0.1, 0, 0]),
            *(xyzs + [0, +0.1, 0]),
            *(xyzs + [0, -0.1, 0]),
        ]
    )
    concave_hull = concave_hull_indexes(
        points[:, :2],
        length_threshold=2.0,
    )
    concave_hull = [*concave_hull, concave_hull[0]]
    llas = tf.enu2lla(points[concave_hull], anchor_lla=anchor)
    llas[:, :2] = llas[:, :2].round(6)
    llas[:, 2] = llas[:, 2].round(1)
    mask = douglas_simplify_mask(tf.lla2enu(llas), epsilon=0.5).astype(bool)
    llas = llas[mask]
    os.makedirs(os.path.dirname(os.path.abspath(output_fence_path)), exist_ok=True)
    with open(output_fence_path, "w") as f:
        logger.info(f"writing to {output_fence_path}")
        json.dump(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [llas.tolist()],
                        },
                        "properties": {
                            "type": "pointcloud",
                            "#points": len(enus),
                            "#voxels_1m^3": len(xyzs),
                            "bbox": lla_bounds.tolist(),
                            "size": (pmax - pmin).round(2).tolist(),
                        },
                    }
                ],
            },
            f,
            indent=4,
        )

    if not output_grids_dir:
        return
    output_grids_dir = os.path.abspath(output_grids_dir)
    os.makedirs(output_grids_dir, exist_ok=True)
    for ii, (x0, x1) in enumerate(zip(xs[:-1], xs[1:])):
        for jj, (y0, y1) in enumerate(zip(ys[:-1], ys[1:])):
            mask = np.logical_and(
                np.logical_and(x0 <= xyzs[:, 0], xyzs[:, 0] < x1),
                np.logical_and(y0 <= xyzs[:, 1], xyzs[:, 1] < y1),
            )
            if not np.any(mask):
                continue
            related = [idxes[i] for i in np.where(mask)[0]]
            related = sorted(chain.from_iterable(related))
            grid = o3d.geometry.PointCloud()
            grid.points = o3d.utility.Vector3dVector(enus[related])
            grid.colors = o3d.utility.Vector3dVector(rgbs[related])
            bounds = lons[ii], lats[jj], lons[ii + 1], lats[jj + 1]
            bounds = "_".join([str(x) for x in bounds])
            path = f"{output_grids_dir}/grid_res{grid_resolution}_{bounds}_anchor_{anchor_text}_raw.pcd"
            logger.info(f"writing #{len(grid.points):,} points to {path}")
            assert o3d.io.write_point_cloud(
                path,
                grid,
                compressed=compress_pcd,
            ), f"failed to dump grid pcd to {path}"
            for res in [0.25, 0.05]:
                path = f"{output_grids_dir}/grid_res{grid_resolution}_{bounds}_anchor_{anchor_text}_voxel{res}.pcd"
                small_grid = grid.voxel_down_sample(res)
                logger.info(f"writing #{len(small_grid.points):,} points to {path}")
                assert o3d.io.write_point_cloud(
                    path,
                    small_grid,
                    compressed=compress_pcd,
                ), f"failed to dump grid pcd to {path}"


def condense_pointcloud(
    *,
    input_path: str,
    output_fence_path: str | None = None,
    output_grids_dir: str | None = None,
    grid_resolution: float = 0.0001,
    compress_pcd: bool = False,
    center: tuple[float, float, float] | None = None,
):
    assert (
        output_fence_path or output_grids_dir
    ), "should specify either --output_fence_path or --output_grids_dir"
    pcd = o3d.io.read_point_cloud(input_path)
    if center:
        if isinstance(center, str):
            center = [float(x) for x in center.split(",")]
        assert isinstance(center, (list, tuple)), f"invalid center: {center}"
        lon, lat, alt = (*center, 0.0) if len(center) == 2 else tuple(center)
        pcd.transform(tf.T_ecef_enu(lon=lon, lat=lat, alt=alt))
    return condense_pointcloud_impl(
        pcd=pcd,
        output_fence_path=output_fence_path,
        output_grids_dir=output_grids_dir,
        grid_resolution=grid_resolution,
        compress_pcd=compress_pcd,
    )


if __name__ == "__main__":
    import fire

    fire.core.Display = lambda lines, out: print(*lines, file=out)
    fire.Fire(condense_pointcloud)
