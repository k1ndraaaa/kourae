from collections import defaultdict
from native.Library.time_manager import TimeManager
from native.Library.commons import Session
from typing import Optional, Callable
from dataclasses import dataclass

@dataclass(frozen=True)
class StrikeLevel:
    id: int
    name: str
    limit: int
    time_window: Optional[int] = None
    callback: Optional[Callable[[Session, "StrikeLevel"], None]] = None

class StrikeCounter:
    def __init__(self, levels: list[StrikeLevel]):
        self.levels = {lvl.id: lvl for lvl in levels}
        self.hits = defaultdict(list)
    def hit(self, level_id: int, session: Session) -> bool:
        lvl = self.levels.get(level_id)
        if not lvl:
            return False
        key = (lvl.id, session.address)
        now = TimeManager.epoch()
        self.hits[key].append(now)
        if lvl.time_window:
            self.hits[key] = [t for t in self.hits[key] if now - t <= lvl.time_window]
        if len(self.hits[key]) >= lvl.limit:
            if lvl.callback:
                lvl.callback(session, lvl)
            self.hits[key].clear()
            return True
        return False
    def current_count(self, level_id: int, session: Session) -> int:
        key = (level_id, session.address)
        return len(self.hits[key])
    def last_hits(self, level_id: int, session: Session) -> list[int]:
        key = (level_id, session.address)
        return self.hits[key][:]
    def reset(self, level_id: int, session: Session):
        key = (level_id, session.address)
        self.hits[key].clear()
    def stats(self, level_id: int, session: Session) -> dict:
        timestamps = self.last_hits(level_id, session)
        if not timestamps:
            return {"total_hits": 0, "first": None, "last": None, "avg_interval": None}
        total = len(timestamps)
        first = timestamps[0]
        last = timestamps[-1]
        avg_interval = (
            sum(b - a for a, b in zip(timestamps[:-1], timestamps[1:])) / (total - 1)
            if total > 1 else None
        )
        return {
            "total_hits": total,
            "first": first,
            "last": last,
            "avg_interval": avg_interval,
        }