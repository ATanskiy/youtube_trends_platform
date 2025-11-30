from dataclasses import dataclass, asdict
from typing import Any, Dict


@dataclass
class Category:
    category_id: str
    title: str
    assignable: bool

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
