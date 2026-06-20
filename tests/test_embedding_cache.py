"""Tests for SQLite embedding cache."""

from __future__ import annotations

import numpy as np
import pytest

from ddharmon.embedding.cache import EmbeddingCache


class TestEmbeddingCache:
    """Tests for EmbeddingCache SQLite storage."""

    @pytest.fixture
    def cache(self, tmp_path) -> EmbeddingCache:
        return EmbeddingCache(tmp_path / "test.db", dimension=768)

    @pytest.fixture
    def sample_embedding(self) -> np.ndarray:
        rng = np.random.default_rng(42)
        return rng.standard_normal(768).astype(np.float32)

    def test_put_then_get_returns_identical_array(self, cache, sample_embedding) -> None:
        """put() then get() returns identical float32 array."""
        cache.put("model-a", "hash123", sample_embedding)
        result = cache.get("model-a", "hash123")
        assert result is not None
        assert result.dtype == np.float32
        np.testing.assert_allclose(result, sample_embedding)

    def test_get_unknown_hash_returns_none(self, cache) -> None:
        """get() with unknown hash returns None."""
        result = cache.get("model-a", "nonexistent")
        assert result is None

    def test_put_many_get_many_batch_roundtrip(self, cache) -> None:
        """put_many() + get_many() batch round-trip works."""
        rng = np.random.default_rng(99)
        items = [(f"hash_{i}", rng.standard_normal(768).astype(np.float32)) for i in range(5)]
        cache.put_many("model-a", items)
        hashes = [h for h, _ in items]
        result = cache.get_many("model-a", hashes)
        assert len(result) == 5
        for h, emb in items:
            np.testing.assert_allclose(result[h], emb)

    def test_same_hash_different_model_stores_independently(self, cache, sample_embedding) -> None:
        """Same content_hash with different model_name stores independently."""
        other = sample_embedding * -1  # Different embedding
        cache.put("model-a", "hash123", sample_embedding)
        cache.put("model-b", "hash123", other)
        result_a = cache.get("model-a", "hash123")
        result_b = cache.get("model-b", "hash123")
        assert result_a is not None
        assert result_b is not None
        np.testing.assert_allclose(result_a, sample_embedding)
        np.testing.assert_allclose(result_b, other)

    def test_put_same_key_overwrites(self, cache) -> None:
        """put() with same key overwrites (INSERT OR REPLACE)."""
        rng = np.random.default_rng(42)
        first = rng.standard_normal(768).astype(np.float32)
        second = rng.standard_normal(768).astype(np.float32)
        cache.put("model-a", "hash123", first)
        cache.put("model-a", "hash123", second)
        result = cache.get("model-a", "hash123")
        assert result is not None
        np.testing.assert_allclose(result, second)

    def test_returned_arrays_are_writable(self, cache, sample_embedding) -> None:
        """Returned arrays are writable (not read-only from frombuffer)."""
        cache.put("model-a", "hash123", sample_embedding)
        result = cache.get("model-a", "hash123")
        assert result is not None
        # Should not raise ValueError: assignment destination is read-only
        result[0] = 999.0
        assert result[0] == 999.0

    def test_creates_parent_directories(self, tmp_path) -> None:
        """Cache creates parent directories and DB file automatically."""
        db_path = tmp_path / "nested" / "deep" / "cache.db"
        cache = EmbeddingCache(db_path, dimension=128)
        assert db_path.exists()
        cache.close()

    def test_dimension_stored_correctly(self, cache, sample_embedding) -> None:
        """dimension column stores correct value and can be validated."""
        cache.put("model-a", "hash123", sample_embedding)
        # Verify dimension is stored in the row
        row = cache._conn.execute(
            "SELECT dimension FROM embeddings WHERE model_name = ? AND content_hash = ?",
            ("model-a", "hash123"),
        ).fetchone()
        assert row is not None
        assert row[0] == 768

    def test_get_many_empty_list(self, cache) -> None:
        """get_many with empty list returns empty dict."""
        result = cache.get_many("model-a", [])
        assert result == {}

    def test_close(self, tmp_path) -> None:
        """close() closes the connection cleanly."""
        cache = EmbeddingCache(tmp_path / "test.db", dimension=128)
        cache.close()
        # After close, operations should fail
        with pytest.raises(Exception):
            cache.get("model-a", "hash123")


class TestCacheSchemaV2:
    """Tests for schema v2 with vector_type column."""

    @pytest.fixture
    def sample_embedding(self) -> np.ndarray:
        rng = np.random.default_rng(42)
        return rng.standard_normal(768).astype(np.float32)

    def test_new_cache_creates_v2_schema(self, tmp_path) -> None:
        """New cache (no existing DB) creates schema_version=2."""
        cache = EmbeddingCache(tmp_path / "new.db", dimension=768)
        row = cache._conn.execute(
            "SELECT value FROM metadata WHERE key = 'schema_version'"
        ).fetchone()
        assert row is not None
        assert row[0] == "2"
        cache.close()

    def test_new_cache_has_vector_type_in_pk(self, tmp_path) -> None:
        """New cache embeddings table includes vector_type column."""
        cache = EmbeddingCache(tmp_path / "new.db", dimension=768)
        # Check that vector_type column exists by querying pragma
        cols = cache._conn.execute("PRAGMA table_info(embeddings)").fetchall()
        col_names = [c[1] for c in cols]
        assert "vector_type" in col_names
        cache.close()

    def test_put_get_with_semantic_vector_type(self, tmp_path, sample_embedding) -> None:
        """get/put with vector_type='semantic' works."""
        cache = EmbeddingCache(tmp_path / "test.db", dimension=768)
        cache.put("model-a", "hash123", sample_embedding, vector_type="semantic")
        result = cache.get("model-a", "hash123", vector_type="semantic")
        assert result is not None
        np.testing.assert_allclose(result, sample_embedding)
        cache.close()

    def test_put_get_with_value_vector_type(self, tmp_path, sample_embedding) -> None:
        """get/put with vector_type='value' works."""
        cache = EmbeddingCache(tmp_path / "test.db", dimension=768)
        cache.put("model-a", "hash123", sample_embedding, vector_type="value")
        result = cache.get("model-a", "hash123", vector_type="value")
        assert result is not None
        np.testing.assert_allclose(result, sample_embedding)
        cache.close()

    def test_same_hash_different_vector_type_no_collision(self, tmp_path) -> None:
        """Same content_hash + different vector_type = different entries."""
        rng = np.random.default_rng(42)
        semantic_emb = rng.standard_normal(768).astype(np.float32)
        value_emb = rng.standard_normal(768).astype(np.float32)
        cache = EmbeddingCache(tmp_path / "test.db", dimension=768)
        cache.put("model-a", "hash123", semantic_emb, vector_type="semantic")
        cache.put("model-a", "hash123", value_emb, vector_type="value")
        result_sem = cache.get("model-a", "hash123", vector_type="semantic")
        result_val = cache.get("model-a", "hash123", vector_type="value")
        assert result_sem is not None
        assert result_val is not None
        np.testing.assert_allclose(result_sem, semantic_emb)
        np.testing.assert_allclose(result_val, value_emb)
        cache.close()

    def test_default_vector_type_is_semantic(self, tmp_path, sample_embedding) -> None:
        """Default vector_type is 'semantic' for backward compat."""
        cache = EmbeddingCache(tmp_path / "test.db", dimension=768)
        cache.put("model-a", "hash123", sample_embedding)  # No explicit vector_type
        result = cache.get("model-a", "hash123", vector_type="semantic")
        assert result is not None
        np.testing.assert_allclose(result, sample_embedding)
        cache.close()

    def test_get_many_with_vector_type(self, tmp_path) -> None:
        """get_many works with vector_type parameter."""
        rng = np.random.default_rng(99)
        items = [(f"hash_{i}", rng.standard_normal(768).astype(np.float32)) for i in range(3)]
        cache = EmbeddingCache(tmp_path / "test.db", dimension=768)
        cache.put_many("model-a", items, vector_type="value")
        hashes = [h for h, _ in items]
        result = cache.get_many("model-a", hashes, vector_type="value")
        assert len(result) == 3
        # Should NOT find them under 'semantic'
        result_sem = cache.get_many("model-a", hashes, vector_type="semantic")
        assert len(result_sem) == 0
        cache.close()

    def test_put_many_with_vector_type(self, tmp_path) -> None:
        """put_many works with vector_type parameter."""
        rng = np.random.default_rng(99)
        items = [(f"hash_{i}", rng.standard_normal(768).astype(np.float32)) for i in range(3)]
        cache = EmbeddingCache(tmp_path / "test.db", dimension=768)
        cache.put_many("model-a", items, vector_type="semantic")
        hashes = [h for h, _ in items]
        result = cache.get_many("model-a", hashes, vector_type="semantic")
        assert len(result) == 3
        cache.close()


class TestCacheV1ToV2Migration:
    """Tests for v1 -> v2 migration."""

    def _create_v1_db(self, db_path):
        """Create a v1-schema database with some data."""
        import sqlite3

        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            CREATE TABLE embeddings (
                model_name  TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                embedding   BLOB NOT NULL,
                dimension   INTEGER NOT NULL,
                created_at  TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (model_name, content_hash)
            )
        """)
        conn.execute("""
            CREATE TABLE metadata (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        conn.execute("INSERT INTO metadata (key, value) VALUES ('schema_version', '1')")
        # Insert some test data
        rng = np.random.default_rng(42)
        emb = rng.standard_normal(768).astype(np.float32)
        conn.execute(
            "INSERT INTO embeddings (model_name, content_hash, embedding, dimension) VALUES (?, ?, ?, ?)",
            ("model-a", "hash_existing", emb.tobytes(), 768),
        )
        conn.commit()
        conn.close()
        return emb

    def test_v1_migrates_to_v2_on_init(self, tmp_path) -> None:
        """Opening a v1 DB auto-migrates to schema v2."""
        db_path = tmp_path / "v1.db"
        self._create_v1_db(db_path)
        cache = EmbeddingCache(db_path, dimension=768)
        row = cache._conn.execute(
            "SELECT value FROM metadata WHERE key = 'schema_version'"
        ).fetchone()
        assert row is not None
        assert row[0] == "2"
        cache.close()

    def test_v1_migration_preserves_data_as_semantic(self, tmp_path) -> None:
        """Existing v1 rows get vector_type='semantic' after migration."""
        db_path = tmp_path / "v1.db"
        original_emb = self._create_v1_db(db_path)
        cache = EmbeddingCache(db_path, dimension=768)
        result = cache.get("model-a", "hash_existing", vector_type="semantic")
        assert result is not None
        np.testing.assert_allclose(result, original_emb)
        cache.close()

    def test_v1_migration_data_not_under_value(self, tmp_path) -> None:
        """Migrated v1 data is NOT accessible under vector_type='value'."""
        db_path = tmp_path / "v1.db"
        self._create_v1_db(db_path)
        cache = EmbeddingCache(db_path, dimension=768)
        result = cache.get("model-a", "hash_existing", vector_type="value")
        assert result is None
        cache.close()
