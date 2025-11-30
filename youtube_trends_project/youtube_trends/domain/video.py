from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass
class Video:
    video_id: str
    title: str
    description: str
    channel_id: str
    channel_title: str
    category_id: str
    published_at: datetime
    region_code: str
    view_count: Optional[int] = None
    like_count: Optional[int] = None
    comment_count: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        if isinstance(self.published_at, datetime):
            d["published_at"] = self.published_at.isoformat()
        return d
