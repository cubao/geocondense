import json
import os
from itertools import chain
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import numpy as np
import open3d as o3d
import polyline_ruler.tf as tf
from concave_hull import concave_hull_indexes
from loguru import logger
from scipy.spatial import ConvexHull


def condense_pointcloud_impl(
    *,
    pcd: o3d.geometry.PointCloud,
    output_voxel_path: str,
    output_grids_dir: str,
    wgs84_epsilon: float = 0.01,
):
    wgs84_scale = 1 / wgs84_epsilon
    assert wgs84_scale == int(wgs84_scale), f"bad wgs84_epsilon: {wgs84_epsilon}"
    wgs84_scale = int(wgs84_scale)

    assert output_voxel_path.endswith(
        ".json"
    ), f"invalid voxel dump: {output_voxel_path}, should be a json file"
    output_grids_dir = os.path.abspath(output_grids_dir)
    os.makedirs(output_grids_dir, exist_ok=True)

    ecefs = np.asarray(pcd.points)
    assert len(ecefs), f"not any points in pointcloud"
    R = np.linalg.norm(ecefs[0])
    assert R > 6300 * 1000, f"data not on earth surface, R is: {R}"
    anchor = np.round(tf.ecef2lla(*ecefs[0]), 2)
    anchor[:2] = (round(l * wgs84_scale) / wgs84_scale for l in anchor[:2])
    anchor[2] = 0.0
    anchor_text = "_".join([str(x) for x in anchor])

    T_ecef_enu = tf.T_ecef_enu(*anchor)
    pcd.transform(np.linalg.inv(T_ecef_enu))
    enus = np.asarray(pcd.points)
    rgbs = np.asarray(pcd.colors)
    pmin = pcd.get_min_bound()
    pmax = pcd.get_max_bound()
    lla_bounds = tf.enu2lla(
        [pmin - 10.0, pmax + 10.0], anchor_lla=anchor, cheap_ruler=False
    )
    lon0, lat0 = lla_bounds[0][:2]
    lon1, lat1 = lla_bounds[1][:2]
    lon0, lon1 = (round(l * wgs84_scale) / wgs84_scale for l in [lon0, lon1])
    lat0, lat1 = (round(l * wgs84_scale) / wgs84_scale for l in [lat0, lat1])
    lons = np.arange(lon0 - wgs84_epsilon, lon1 + wgs84_epsilon + 1e-15, wgs84_epsilon)
    lats = np.arange(lat0 - wgs84_epsilon, lat1 + wgs84_epsilon + 1e-15, wgs84_epsilon)
    assert len(lons) > 1
    assert len(lats) > 1
    xs = tf.lla2enu(
        [[l, lats[0], 0.0] for l in lons],
        anchor_lla=anchor,
        cheap_ruler=False,
    )[:, 0]
    ys = tf.lla2enu(
        [[lons[0], l, 0.0] for l in lats],
        anchor_lla=anchor,
        cheap_ruler=False,
    )[:, 1]

    # pcd1 = pcd.voxel_down_sample(1.0)
    pcd1, _, idxes = pcd.voxel_down_sample_and_trace(
        1.0, min_bound=pmin, max_bound=pmax
    )
    idxes = [np.array(i) for i in idxes]
    xyzs = np.asarray(pcd1.points)

    # export voxels
    points = np.array(
        [
            *xyzs,
            *(xyzs + [+1, 0, 0]),
            *(xyzs + [-1, 0, 0]),
            *(xyzs + [0, +1, 0]),
            *(xyzs + [0, -1, 0]),
        ]
    )
    convex_hull = ConvexHull(points[:, :2])
    concave_hull = concave_hull_indexes(
        points[:, :2],
        convex_hull_indexes=convex_hull.vertices.astype(np.int32),
        length_threshold=2.0,
    )
    concave_hull = [*concave_hull, concave_hull[0]]
    llas = tf.enu2lla(points[concave_hull], anchor_lla=anchor)
    with open(output_voxel_path, "w") as f:
        logger.info(f"wrote to {output_voxel_path}")
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
                            "lla_bounds": lla_bounds.tolist(),
                            "enu_bounds": [pmin.tolist(), pmax.tolist()],
                        },
                    }
                ],
            },
            f,
            indent=4,
        )

    for ii, (x0, x1) in enumerate(zip(xs[:-1], xs[1:])):
        for jj, (y0, y1) in enumerate(zip(ys[:-1], ys[1:])):
            mask = np.logical_and(
                np.logical_and(x0 <= xyzs[:, 0], xyzs[:, 0] < x1),
                np.logical_and(y0 <= xyzs[:, 1], xyzs[:, 1] < y1),
            )
            if not np.any(mask):
                continue
            related = [idxes[i] for i in np.where(mask)[0]]
            related = sorted(list(chain.from_iterable(related)))
            grid = o3d.geometry.PointCloud()
            grid.points = o3d.utility.Vector3dVector(enus[related])
            grid.colors = o3d.utility.Vector3dVector(rgbs[related])
            bounds = lons[ii], lats[jj], lons[ii + 1], lats[jj + 1]
            bounds = "_".join([str(x) for x in bounds])
            path = f"{output_grids_dir}/grid_{bounds}_anchor_{anchor_text}.pcd"
            logger.info(f"writing #{len(grid.points):,} points to {path}")
            assert o3d.io.write_point_cloud(
                path, grid, compressed=True
            ), f"failed to dump grid pcd to {path}"


def condense_pointcloud(
    *,
    input_path: str,
    output_voxel_path: str,
    output_grids_dir: str,
    wgs84_epsilon: float = 0.01,
    center: Optional[Tuple[float, float, float]] = None,
):
    pcd = o3d.io.read_point_cloud(input_path)
    if center:
        if isinstance(center, str):
            center = [float(x) for x in center.split(",")]
        assert isinstance(center, (list, tuple)), f"invalid center: {center}"
        lon, lat, alt = (*center, 0.0) if len(center) == 2 else tuple(center)
        pcd.transform(tf.T_ecef_enu(lon=lon, lat=lat, alt=alt))
    return condense_pointcloud_impl(
        pcd=pcd,
        output_voxel_path=output_voxel_path,
        output_grids_dir=output_grids_dir,
        wgs84_epsilon=wgs84_epsilon,
    )


if __name__ == "__main__":
    import fire

    fire.core.Display = lambda lines, out: print(*lines, file=out)
    fire.Fire(condense_pointcloud)
