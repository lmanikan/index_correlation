from pathlib import Path

import yaml

from index_correlation.core.models import Index, VolType, WeightType


def load_indices_from_yaml(path: str | Path) -> list[Index]:
    path = Path(path)
    data = yaml.safe_load(path.read_text()) or {}
    raw_indices = data.get("indices", [])

    indices: list[Index] = []
    for cfg in raw_indices:
        idx = Index(
            portfolio=cfg["portfolio"],
            symbol=cfg["symbol"],
            weight_type=WeightType(cfg["weight_type"]),
            num_components=cfg["num_components"],
            vol_type=VolType(cfg["vol_type"]),
            strikes=cfg["strikes"],
            quantities=cfg.get("quantities", ["implied_correlation"]),
        )
        indices.append(idx)

    return indices
