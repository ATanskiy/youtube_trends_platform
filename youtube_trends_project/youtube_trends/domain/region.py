from dataclasses import dataclass, asdict
from typing import Any, Dict


@dataclass
class Region:
    region_code: str
    name: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
