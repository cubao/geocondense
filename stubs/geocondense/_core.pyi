from __future__ import annotations
import typing
__all__ = ['CondenseOptions', 'condense_geojson', 'dissect_geojson']
class CondenseOptions:
    debug: bool
    douglas_epsilon: float
    grid_features_keep_properties: bool
    grid_h3_resolution: int
    indent: bool
    sort_keys: bool
    sparsify_h3_resolution: int
    sparsify_upper_limit: int
    def __init__(self) -> None:
        ...
def condense_geojson(*, input_path: str, output_index_path: str | None = None, output_strip_path: str | None = None, output_grids_dir: str | None = None, options: CondenseOptions = ...) -> bool:
    ...
def dissect_geojson(*, input_path: str, output_geometry: str | None = None, output_properties: str | None = None, output_observations: str | None = None, output_others: str | None = None, indent: bool = False) -> bool:
    ...
__version__: str = '0.0.3'
