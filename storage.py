import asyncio
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import aiosqlite


class Storage:
	"""Async SQLite storage for codes with single-use distribution."""

	def __init__(self, db_path: str = "codes.db") -> None:
		self.db_path = db_path
		self._lock = asyncio.Lock()

	async def initialize(self) -> None:
		"""Create database schema if it doesn't exist."""
		async with aiosqlite.connect(self.db_path) as db:
			await db.execute(
				"""
				CREATE TABLE IF NOT EXISTS codes (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					code TEXT NOT NULL UNIQUE,
					is_used INTEGER NOT NULL DEFAULT 0,
					uploaded_by INTEGER,
					uploaded_at TEXT,
					used_by INTEGER,
					used_at TEXT
				);
				"""
			)
			await db.commit()

	async def insert_codes(self, codes: List[str], uploaded_by: int) -> Tuple[int, int]:
		"""
		Insert a batch of codes. Duplicates (existing in DB) are ignored.

		Returns (inserted_count, duplicate_or_ignored_count)
		"""
		# De-duplicate within the incoming batch while preserving order
		seen = set()
		unique_codes: List[str] = []
		for c in codes:
			if c not in seen:
				seen.add(c)
				unique_codes.append(c)

		now_iso = datetime.now(timezone.utc).isoformat()
		async with aiosqlite.connect(self.db_path) as db:
			# Sensible pragmas for WAL mode to reduce write contention
			await db.execute("PRAGMA journal_mode=WAL;")
			await db.execute("PRAGMA synchronous=NORMAL;")
			for code in unique_codes:
				await db.execute(
					"""
					INSERT OR IGNORE INTO codes (code, is_used, uploaded_by, uploaded_at)
					VALUES (?, 0, ?, ?);
					""",
					(code, uploaded_by, now_iso),
				)
			await db.commit()

			# Count how many were inserted in this batch using the timestamp marker
			async with db.execute(
				"SELECT COUNT(*) FROM codes WHERE uploaded_at = ? AND uploaded_by = ?;",
				(now_iso, uploaded_by),
			) as cursor:
				row = await cursor.fetchone()
				inserted_count = int(row[0]) if row is not None else 0

		duplicates = max(len(unique_codes) - inserted_count, 0)
		return inserted_count, duplicates

	async def count_unused(self) -> int:
		async with aiosqlite.connect(self.db_path) as db:
			async with db.execute("SELECT COUNT(*) FROM codes WHERE is_used = 0;") as cursor:
				row = await cursor.fetchone()
				return int(row[0]) if row is not None else 0

	async def get_and_mark_next_unused(self, used_by: int) -> Optional[str]:
		"""
		Atomically fetch the next unused code (FIFO) and mark it as used.
		Returns the code string, or None if none remain.
		"""
		async with self._lock:  # Extra guard to minimize concurrent pick attempts
			async with aiosqlite.connect(self.db_path) as db:
				await db.execute("PRAGMA journal_mode=WAL;")
				await db.execute("PRAGMA synchronous=NORMAL;")
				# Start a write transaction to avoid races
				await db.execute("BEGIN IMMEDIATE;")

				code_row = None
				async with db.execute(
					"SELECT id, code FROM codes WHERE is_used = 0 ORDER BY id ASC LIMIT 1;"
				) as cursor:
					code_row = await cursor.fetchone()

				if code_row is None:
					await db.commit()
					return None

				code_id, code_value = int(code_row[0]), str(code_row[1])
				now_iso = datetime.now(timezone.utc).isoformat()
				await db.execute(
					"UPDATE codes SET is_used = 1, used_by = ?, used_at = ? WHERE id = ? AND is_used = 0;",
					(used_by, now_iso, code_id),
				)
				await db.commit()
				return code_value

	async def count_used_by(self, user_id: int) -> int:
		"""Return how many codes were distributed by the given user id."""
		async with aiosqlite.connect(self.db_path) as db:
			async with db.execute(
				"SELECT COUNT(*) FROM codes WHERE is_used = 1 AND used_by = ?;",
				(user_id,),
			) as cursor:
				row = await cursor.fetchone()
				return int(row[0]) if row is not None else 0

	async def usage_counts(self) -> List[Tuple[int, int]]:
		"""Return a list of (used_by, count) for all users who distributed codes."""
		async with aiosqlite.connect(self.db_path) as db:
			async with db.execute(
				"""
				SELECT used_by, COUNT(*)
				FROM codes
				WHERE is_used = 1 AND used_by IS NOT NULL
				GROUP BY used_by
				ORDER BY COUNT(*) DESC;
				"""
			) as cursor:
				rows = await cursor.fetchall()
				results: List[Tuple[int, int]] = []
				for r in rows:
					if r[0] is None:
						continue
					results.append((int(r[0]), int(r[1])))
				return results


