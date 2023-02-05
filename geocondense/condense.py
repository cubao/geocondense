import json
import os
from itertools import chain
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import numpy as np
from loguru import logger


def condense(
    *,
    workdir: str,
    semantic_files: List[str],
    pointcloud_files: List[str],
    center: Optional[Tuple[float, float, float]] = None,
):
    pass


if __name__ == "__main__":
    import fire

    fire.core.Display = lambda lines, out: print(*lines, file=out)
    fire.Fire(condense)
