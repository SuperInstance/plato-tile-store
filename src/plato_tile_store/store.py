"""In-memory tile storage with JSONL persistence, indexing, and immutable tiles."""
import json
import os
import time
from typing import Optional
from collections import defaultdict

class TileStore:
    def __init__(self, path: str = ""):
        self._tiles: dict[str, dict] = {}
        self._versions: dict[str, int] = {}
        self._domain_index: dict[str, list[str]] = defaultdict(list)
        self._tag_index: dict[str, list[str]] = defaultdict(list)
        self._confidence_index: list[tuple[float, str]] = []  # (confidence, id)
        self.path = path
        self._op_count = 0

    def add(self, tile_id: str, content: str, domain: str = "",
            confidence: float = 0.5, metadata: dict = None,
            tags: list[str] = None) -> dict:
        self._op_count += 1
        tile = {"id": tile_id, "content": content, "domain": domain or "general",
                "confidence": confidence, "metadata": metadata or {},
                "tags": tags or [], "created_at": time.time(), "version": 1}
        if tile_id in self._tiles:
            tile["version"] = self._versions.get(tile_id, 1) + 1
            tile["parent_id"] = tile_id
            # Remove old indices
            old = self._tiles[tile_id]
            old_domain = old.get("domain", "general")
            self._domain_index[old_domain] = [t for t in self._domain_index[old_domain] if t != tile_id]
            for tag in old.get("tags", []):
                self._tag_index[tag] = [t for t in self._tag_index[tag] if t != tile_id]
            self._confidence_index = [(c, i) for c, i in self._confidence_index if i != tile_id]
        self._tiles[tile_id] = tile
        self._versions[tile_id] = tile["version"]
        self._domain_index[tile["domain"]].append(tile_id)
        for tag in tile["tags"]:
            self._tag_index[tag].append(tile_id)
        self._confidence_index.append((confidence, tile_id))
        return tile

    def get(self, tile_id: str) -> Optional[dict]:
        tile = self._tiles.get(tile_id)
        return dict(tile) if tile else None

    def remove(self, tile_id: str) -> bool:
        tile = self._tiles.pop(tile_id, None)
        if not tile:
            return False
        self._op_count += 1
        domain = tile.get("domain", "general")
        self._domain_index[domain] = [t for t in self._domain_index[domain] if t != tile_id]
        for tag in tile.get("tags", []):
            self._tag_index[tag] = [t for t in self._tag_index[tag] if t != tile_id]
        self._confidence_index = [(c, i) for c, i in self._confidence_index if i != tile_id]
        return True

    def search(self, query: str, limit: int = 10, domain: str = None) -> list[dict]:
        q_words = set(query.lower().split())
        results = []
        for tile_id in list(self._tiles.keys()):
            tile = self._tiles[tile_id]
            if domain and tile.get("domain") != domain:
                continue
            content_words = set(tile.get("content", "").lower().split())
            if q_words & content_words:
                results.append(tile)
        results.sort(key=lambda t: t.get("confidence", 0.5), reverse=True)
        return results[:limit]

    def by_domain(self, domain: str, limit: int = 50) -> list[dict]:
        ids = self._domain_index.get(domain, [])[-limit:]
        return [dict(self._tiles[tid]) for tid in ids if tid in self._tiles]

    def by_tag(self, tag: str, limit: int = 50) -> list[dict]:
        ids = self._tag_index.get(tag, [])[-limit:]
        return [dict(self._tiles[tid]) for tid in ids if tid in self._tiles]

    def top_confidence(self, n: int = 10) -> list[dict]:
        sorted_conf = sorted(self._confidence_index, key=lambda x: x[0], reverse=True)
        return [dict(self._tiles[tid]) for conf, tid in sorted_conf[:n] if tid in self._tiles]

    def domains(self) -> dict[str, int]:
        return {d: len(ids) for d, ids in self._domain_index.items() if ids}

    def tags(self) -> dict[str, int]:
        return {t: len(ids) for t, ids in self._tag_index.items() if ids}

    def count(self, domain: str = "") -> int:
        if domain:
            return len(self._domain_index.get(domain, []))
        return len(self._tiles)

    def all_ids(self) -> list[str]:
        return list(self._tiles.keys())

    def save_jsonl(self, path: str = ""):
        path = path or self.path
        if not path:
            return
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w") as f:
            for t in self._tiles.values():
                f.write(json.dumps({k: v for k, v in t.items() if not k.startswith("_")}) + "\n")

    def load_jsonl(self, path: str = ""):
        path = path or self.path
        if not path or not os.path.exists(path):
            return
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                t = json.loads(line)
                self.add(t["id"], t.get("content", ""), t.get("domain", ""),
                        t.get("confidence", 0.5), t.get("metadata"),
                        t.get("tags", []))

    @property
    def stats(self) -> dict:
        return {"total": len(self._tiles), "domains": self.domains(),
                "tags": len(self._tag_index), "operations": self._op_count}
