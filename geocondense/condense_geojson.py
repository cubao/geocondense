from typing import Any, Dict, List, Optional, Set, Tuple, Union

from geocondense import CondenseOptions
from geocondense import condense_geojson as condense_geojson_impl


def condense_geojson(
    *,
    input_path: str,
    output_strip_path: str = None,
    output_grids_dir: str = None,
    douglas_epsilon: float = 0.4,
    h3_resolution: int = 8,
    indent: bool = False,
    sort_keys: bool = False,
    grid_features_keep_properties: bool = False,
):
    options = CondenseOptions()
    options.douglas_epsilon = douglas_epsilon
    options.h3_resolution = h3_resolution
    options.indent = indent
    options.sort_keys = sort_keys
    options.grid_features_keep_properties = grid_features_keep_properties
    return condense_geojson_impl(
        input_path=input_path,
        output_strip_path=output_strip_path,
        output_grids_dir=output_grids_dir,
        options=options,
    )


if __name__ == "__main__":
    import fire

    fire.core.Display = lambda lines, out: print(*lines, file=out)
    fire.Fire(condense_geojson)
