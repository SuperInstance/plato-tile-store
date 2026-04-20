"""In-memory tile storage with JSONL persistence."""

import json, os
from typing import Optional

class TileStore:
    def __init__(self, path: str = None):
        self._tiles: dict[str, dict] = {}
        self._versions: dict[str, list[str]] = {}
        self.path = path

    def add(self, tile_id: str, content: str, domain: str = "",
            confidence: float = 0.5, metadata: dict = None) -> dict:
        import time
        tile = {"id": tile_id, "content": content, "domain": domain,
                "confidence": confidence, "metadata": metadata or {},
                "created_at": time.time(), "version": 1}
        if tile_id in self._tiles:
            old = self._tiles[tile_id]
            tile["version"] = old.get("version", 1) + 1
            tile["parent_id"] = old.get("id")
            if tile_id not in self._versions:
                self._versions[tile_id] = []
            self._versions[tile_id].append(tile_id)
        self._tiles[tile_id] = tile
        return tile

    def get(self, tile_id: str) -> Optional[dict]:
        return self._tiles.get(tile_id)

    def remove(self, tile_id: str) -> bool:
        return self._tiles.pop(tile_id, None) is not None

    def search(self, query: str, limit: int = 10, domain: str = None) -> list[dict]:
        q_words = set(query.lower().split())
        results = []
        for t in self._tiles.values():
            if domain and t.get("domain") != domain:
                continue
            c_words = set(t.get("content", "").lower().split())
            overlap = len(q_words & c_words) / max(len(q_words | c_words), 1)
            t["_score"] = overlap
            results.append(t)
        results.sort(key=lambda x: -x.get("_score", 0))
        return results[:limit]

    def history(self, tile_id: str) -> list[str]:
        return self._versions.get(tile_id, [])

    @property
    def count(self) -> int:
        return len(self._tiles)

    def all_tiles(self) -> list[dict]:
        return list(self._tiles.values())

    def save_jsonl(self, path: str = None):
        path = path or self.path
        if not path: return
        with open(path, "w") as f:
            for t in self._tiles.values():
                f.write(json.dumps({k: v for k, v in t.items() if not k.startswith("_")}) + "\n")

    def load_jsonl(self, path: str = None):
        path = path or self.path
        if not path or not os.path.exists(path): return
        with open(path) as f:
            for line in f:
                t = json.loads(line.strip())
                self._tiles[t["id"]] = t

    @property
    def stats(self) -> dict:
        domains = {}
        for t in self._tiles.values():
            d = t.get("domain", "unknown")
            domains[d] = domains.get(d, 0) + 1
        return {"total": len(self._tiles), "domains": domains}
