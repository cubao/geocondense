from __future__ import annotations
import typing

__all__ = ["CondenseOptions", "condense_geojson", "dissect_geojson"]

class CondenseOptions:
    def __init__(self) -> None:
        """
        Default constructor for CondenseOptions
        """
    @property
    def debug(self) -> bool:
        """
        Debug option
        """
    @debug.setter
    def debug(self, arg0: bool) -> None: ...
    @property
    def douglas_epsilon(self) -> float:
        """
        Epsilon value for Douglas-Peucker algorithm
        """
    @douglas_epsilon.setter
    def douglas_epsilon(self, arg0: float) -> None: ...
    @property
    def grid_features_keep_properties(self) -> bool:
        """
        Option to keep properties for grid features
        """
    @grid_features_keep_properties.setter
    def grid_features_keep_properties(self, arg0: bool) -> None: ...
    @property
    def grid_h3_resolution(self) -> int:
        """
        H3 resolution for grid features
        """
    @grid_h3_resolution.setter
    def grid_h3_resolution(self, arg0: int) -> None: ...
    @property
    def indent(self) -> bool:
        """
        Indentation option for JSON output
        """
    @indent.setter
    def indent(self, arg0: bool) -> None: ...
    @property
    def sort_keys(self) -> bool:
        """
        Option to sort keys in JSON output
        """
    @sort_keys.setter
    def sort_keys(self, arg0: bool) -> None: ...
    @property
    def sparsify_h3_resolution(self) -> int:
        """
        H3 resolution for sparsification
        """
    @sparsify_h3_resolution.setter
    def sparsify_h3_resolution(self, arg0: int) -> None: ...
    @property
    def sparsify_upper_limit(self) -> int:
        """
        Upper limit for sparsification
        """
    @sparsify_upper_limit.setter
    def sparsify_upper_limit(self, arg0: int) -> None: ...

def condense_geojson(
    *,
    input_path: str,
    output_index_path: str | None = None,
    output_strip_path: str | None = None,
    output_grids_dir: str | None = None,
    options: CondenseOptions = ...,
) -> bool:
    """
    Condense GeoJSON data.

    Args:
        input_path: Path to the input GeoJSON file.
        output_index_path: Optional path for the output index file.
        output_strip_path: Optional path for the output strip file.
        output_grids_dir: Optional directory for output grid files.
        options: CondenseOptions object with configuration options.

    Returns:
        bool: True if the operation was successful, False otherwise.
    """

def dissect_geojson(
    *,
    input_path: str,
    output_geometry: str | None = None,
    output_properties: str | None = None,
    output_observations: str | None = None,
    output_others: str | None = None,
    indent: bool = False,
) -> bool:
    """
    Dissect GeoJSON data into separate components.

    Args:
        input_path: Path to the input GeoJSON file.
        output_geometry: Optional path for the output geometry file.
        output_properties: Optional path for the output properties file.
        output_observations: Optional path for the output observations file.
        output_others: Optional path for other output data.
        indent: Boolean flag to enable indentation in output JSON.

    Returns:
        bool: True if the operation was successful, False otherwise.
    """

__version__: str = "0.0.3"
