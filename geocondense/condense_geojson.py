from __future__ import annotations

import os
from pprint import pprint

from geocondense import CondenseOptions

from ._core import condense_geojson as condense_geojson_impl


def condense_geojson(
    *,
    input_path: str,
    output_index_path: str = None,
    output_strip_path: str = None,
    output_grids_dir: str = None,
    douglas_epsilon: float = 0.4,
    grid_h3_resolution: int = 8,
    indent: bool = False,
    sort_keys: bool = False,
    grid_features_keep_properties: bool = False,
    sparsify_h3_resolution: int = 11,
    sparsify_upper_limit: int = 48,
    debug: bool = False,
):
    # setup options
    options = CondenseOptions()
    options.douglas_epsilon = douglas_epsilon
    options.grid_h3_resolution = grid_h3_resolution
    options.indent = indent
    options.sort_keys = sort_keys
    options.grid_features_keep_properties = grid_features_keep_properties
    options.sparsify_h3_resolution = sparsify_h3_resolution
    options.sparsify_upper_limit = sparsify_upper_limit
    options.debug = debug

    # mkdir -p
    for path in [output_index_path, output_strip_path]:
        path = os.path.abspath(path)
        os.makedirs(os.path.dirname(path), exist_ok=True)
    os.makedirs(os.path.abspath(output_grids_dir), exist_ok=True)

    # call c++ condense_geojson
    succ = condense_geojson_impl(
        input_path=input_path,
        output_index_path=output_index_path,
        output_strip_path=output_strip_path,
        output_grids_dir=output_grids_dir,
        options=options,
    )
    if not succ:
        pprint(locals())  # noqa: T203
        msg = f"failed to condense geojson: {input_path}"
        raise Exception(msg)


if __name__ == "__main__":
    import fire

    fire.core.Display = lambda lines, out: print(*lines, file=out)
    fire.Fire(condense_geojson)
