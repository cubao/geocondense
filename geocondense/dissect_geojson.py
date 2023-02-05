import os
from pprint import pprint
from typing import Any, Dict, List, Optional, Set, Tuple, Union  # noqa

from geocondense import dissect_geojson as dissect_geojson_impl


def dissect_geojson(
    *,
    input_path: str,
    output_geometry: Optional[str] = None,
    output_properties: Optional[str] = None,
    output_observations: Optional[str] = None,
    output_others: Optional[str] = None,
    indent: bool = False,
):
    # mkdir -p
    for path in [
        output_geometry,
        output_properties,
        output_observations,
        output_others,
    ]:
        if not path:
            continue
        path = os.path.abspath(path)
        os.makedirs(os.path.dirname(path), exist_ok=True)

    # call c++ dissect_geojson
    succ = dissect_geojson_impl(
        input_path=input_path,
        output_geometry=output_geometry,
        output_properties=output_properties,
        output_observations=output_observations,
        output_others=output_others,
        indent=indent,
    )
    if not succ:
        pprint(locals())
        raise Exception(f"failed to dissect geojson: {input_path}")


if __name__ == "__main__":
    import fire

    fire.core.Display = lambda lines, out: print(*lines, file=out)
    fire.Fire(dissect_geojson)
