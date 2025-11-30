from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Dict


@dataclass
class Comment:
    comment_id: str
    video_id: str
    author: str
    text: str
    like_count: int
    published_at: datetime

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        if isinstance(self.published_at, datetime):
            d["published_at"] = self.published_at.isoformat()
        return d
