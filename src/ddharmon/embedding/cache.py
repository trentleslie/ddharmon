"""SQLite-backed embedding cache with dual vector type support.

Stores embeddings as BLOBs keyed by (model_name, content_hash, vector_type).
Supports batch get/put operations for efficient bulk embedding workflows.
Schema v2 adds vector_type to the primary key, enabling semantic and value
embeddings to coexist for the same field.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import numpy as np
from numpy.typing import NDArray

logger = logging.getLogger(__name__)


class EmbeddingCache:
    """SQLite-backed embedding cache with compound key (model_name, content_hash, vector_type).

    Embeddings are stored as float32 BLOB data. Multiple models and vector types
    can coexist in the same database -- switching models preserves existing embeddings.

    Schema v2 adds vector_type to support dual-vector storage (semantic + value).
    Existing v1 databases are auto-migrated on init.

    Args:
        db_path: Path to SQLite database file. Parent directories are created automatically.
        dimension: Expected embedding dimension (stored per row for validation).
    """

    def __init__(self, db_path: Path, dimension: int) -> None:
        self.db_path = db_path
        self._dimension = dimension
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path))
        self._create_tables()
        self._ensure_v2()

    def _get_schema_version(self) -> int:
        """Query metadata table for current schema version."""
        row = self._conn.execute(
            "SELECT value FROM metadata WHERE key = 'schema_version'"
        ).fetchone()
        if row is None:
            return 1
        return int(row[0])

    def _migrate_v1_to_v2(self) -> None:
        """Migrate v1 schema to v2 by adding vector_type to primary key.

        Uses create-copy-drop-rename pattern since SQLite cannot ALTER primary keys.
        Existing rows get vector_type='semantic'.
        """
        logger.info("Migrating embedding cache from schema v1 to v2")
        self._conn.execute("BEGIN TRANSACTION")
        try:
            self._conn.execute("""
                CREATE TABLE embeddings_v2 (
                    model_name   TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    vector_type  TEXT NOT NULL DEFAULT 'semantic',
                    embedding    BLOB NOT NULL,
                    dimension    INTEGER NOT NULL,
                    created_at   TEXT DEFAULT (datetime('now')),
                    PRIMARY KEY (model_name, content_hash, vector_type)
                )
            """)
            self._conn.execute("""
                INSERT INTO embeddings_v2 (model_name, content_hash, vector_type, embedding, dimension, created_at)
                SELECT model_name, content_hash, 'semantic', embedding, dimension, created_at
                FROM embeddings
            """)
            self._conn.execute("DROP TABLE embeddings")
            self._conn.execute("ALTER TABLE embeddings_v2 RENAME TO embeddings")
            self._conn.execute(
                "UPDATE metadata SET value = '2' WHERE key = 'schema_version'"
            )
            self._conn.commit()
            logger.info("Migration to schema v2 complete")
        except Exception:
            self._conn.rollback()
            raise

    def _ensure_v2(self) -> None:
        """Check schema version and migrate if needed."""
        version = self._get_schema_version()
        if version < 2:
            self._migrate_v1_to_v2()

    def _create_tables(self) -> None:
        """Create schema tables if they don't exist."""
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS embeddings (
                model_name   TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                vector_type  TEXT NOT NULL DEFAULT 'semantic',
                embedding    BLOB NOT NULL,
                dimension    INTEGER NOT NULL,
                created_at   TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (model_name, content_hash, vector_type)
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        self._conn.execute("INSERT OR IGNORE INTO metadata (key, value) VALUES ('schema_version', '2')")
        self._conn.commit()

    def get(
        self, model_name: str, content_hash: str, vector_type: str = "semantic"
    ) -> NDArray[np.float32] | None:
        """Retrieve a single embedding by (model_name, content_hash, vector_type).

        Args:
            model_name: Model identifier.
            content_hash: Content hash of the field's embedding text.
            vector_type: Type of vector ('semantic' or 'value'). Defaults to 'semantic'.

        Returns:
            Float32 array of shape (dimension,), or None if not found.
            Returned array is writable (copied from buffer).
        """
        row = self._conn.execute(
            "SELECT embedding FROM embeddings WHERE model_name = ? AND content_hash = ? AND vector_type = ?",
            (model_name, content_hash, vector_type),
        ).fetchone()
        if row is None:
            return None
        return np.frombuffer(row[0], dtype=np.float32).copy()

    def put(
        self,
        model_name: str,
        content_hash: str,
        embedding: NDArray[np.float32],
        vector_type: str = "semantic",
    ) -> None:
        """Store a single embedding. Overwrites if key already exists.

        Args:
            model_name: Model identifier for cache keying.
            content_hash: Content hash of the field's embedding text.
            embedding: Float32 array to store.
            vector_type: Type of vector ('semantic' or 'value'). Defaults to 'semantic'.
        """
        self._conn.execute(
            "INSERT OR REPLACE INTO embeddings (model_name, content_hash, vector_type, embedding, dimension) VALUES (?, ?, ?, ?, ?)",
            (model_name, content_hash, vector_type, embedding.astype(np.float32).tobytes(), len(embedding)),
        )
        self._conn.commit()

    def get_many(
        self, model_name: str, content_hashes: list[str], vector_type: str = "semantic"
    ) -> dict[str, NDArray[np.float32]]:
        """Batch lookup of embeddings.

        Args:
            model_name: Model identifier.
            content_hashes: List of content hashes to look up.
            vector_type: Type of vector ('semantic' or 'value'). Defaults to 'semantic'.

        Returns:
            Dict of content_hash -> float32 array for found entries.
        """
        if not content_hashes:
            return {}
        placeholders = ",".join("?" for _ in content_hashes)
        rows = self._conn.execute(
            f"SELECT content_hash, embedding FROM embeddings WHERE model_name = ? AND vector_type = ? AND content_hash IN ({placeholders})",
            [model_name, vector_type, *content_hashes],
        ).fetchall()
        return {row[0]: np.frombuffer(row[1], dtype=np.float32).copy() for row in rows}

    def put_many(
        self,
        model_name: str,
        items: list[tuple[str, NDArray[np.float32]]],
        vector_type: str = "semantic",
    ) -> None:
        """Batch insert/replace embeddings.

        Args:
            model_name: Model identifier.
            items: List of (content_hash, embedding) tuples.
            vector_type: Type of vector ('semantic' or 'value'). Defaults to 'semantic'.
        """
        self._conn.executemany(
            "INSERT OR REPLACE INTO embeddings (model_name, content_hash, vector_type, embedding, dimension) VALUES (?, ?, ?, ?, ?)",
            [(model_name, ch, vector_type, emb.astype(np.float32).tobytes(), len(emb)) for ch, emb in items],
        )
        self._conn.commit()

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()
