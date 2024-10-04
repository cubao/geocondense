from __future__ import annotations

import os
from pprint import pprint

from geocondense._core import dissect_geojson as dissect_geojson_impl


def dissect_geojson(
    *,
    input_path: str,
    output_geometry: str | None = None,
    output_properties: str | None = None,
    output_observations: str | None = None,
    output_others: str | None = None,
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
        pprint(locals())  # noqa: T203
        msg = f"failed to dissect geojson: {input_path}"
        raise Exception(msg)


if __name__ == "__main__":
    import fire

    fire.core.Display = lambda lines, out: print(*lines, file=out)
    fire.Fire(dissect_geojson)
