from datetime import time
from typing import List, Dict, Any
from dataclasses import dataclass, field
from typing import List
from enum import Enum
import yaml
from pathlib import Path

from index_correlation.core.data_models import Index
from index_correlation.core.models import WeightType,VolType

def load_indices_from_yaml(path: str | Path) -> List[Index]:
    path = Path(path)
    data = yaml.safe_load(path.read_text()) or {}
    raw_indices = data.get("indices", [])

    indices: List[Index] = []
    for cfg in raw_indices:
        idx = Index(
            portfolio=cfg["portfolio"],
            symbol=cfg["symbol"],
            weight_type=WeightType(cfg["weight_type"]),
            num_components=cfg["num_components"],
            vol_type=VolType(cfg["vol_type"]),
            strikes=cfg["strikes"],
        )
        indices.append(idx)

    return indices