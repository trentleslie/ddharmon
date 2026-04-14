"""Tests for ddharmon.dataset.map_dataset_file_sync."""

from __future__ import annotations

import unittest.mock as _mock
from typing import Any

import httpx
import pytest
import respx

from ddharmon.dataset import map_dataset_file_sync
from ddharmon.exceptions import (
    BioMapperAuthError,
    BioMapperRateLimitError,
)
from ddharmon.models import DatasetMappingResult, MappingResult
from tests.conftest import (
    make_batch_entry,
    make_ndjson_body,
    make_truncating_stream,
)

BASE_URL = "https://biomapper.expertintheloop.io/api/v1"
DATASET_URL = f"{BASE_URL}/map/dataset/stream"


@pytest.fixture()
def api_key(monkeypatch: pytest.MonkeyPatch) -> str:
    # Placeholder value with low entropy so the gitleaks hook ignores it;
    # BioMapperClient only requires the key to be non-empty for env-resolution.
    key = "x"  # noqa: S105 — fixture, not a real key
    monkeypatch.setenv("BIOMAPPER_API_KEY", key)
    return key


@pytest.fixture()
def tsv_path(tmp_path: Any) -> Any:
    p = tmp_path / "small.tsv"
    p.write_text("name\thmdb_id\nL-Histidine\tHMDB00177\n")
    return p


class TestMapDatasetFileSyncHappyPath:
    @respx.mock
    def test_returns_populated_result(
        self, tsv_path: Any, api_key: str
    ) -> None:
        body = make_ndjson_body(
            [make_batch_entry("A"), make_batch_entry("B"), make_batch_entry("C")]
        )
        respx.post(DATASET_URL).mock(
            return_value=httpx.Response(200, content=body)
        )
        result = map_dataset_file_sync(
            tsv_path, name_column="name", provided_id_columns=["hmdb_id"]
        )
        assert isinstance(result, DatasetMappingResult)
        assert [r.query_name for r in result.results] == ["A", "B", "C"]
        assert result.error is None
        assert result.stats == {}

    @respx.mock
    def test_empty_stream_returns_empty_result(
        self, tsv_path: Any, api_key: str
    ) -> None:
        respx.post(DATASET_URL).mock(
            return_value=httpx.Response(200, content=b"")
        )
        result = map_dataset_file_sync(
            tsv_path, name_column="name", provided_id_columns=["hmdb_id"]
        )
        assert result.results == []
        assert result.error is None


class TestMapDatasetFileSyncProgress:
    @respx.mock
    def test_progress_true_calls_tqdm_update_per_result(
        self, tsv_path: Any, api_key: str
    ) -> None:
        body = make_ndjson_body(
            [make_batch_entry(f"x{i}") for i in range(3)]
        )
        respx.post(DATASET_URL).mock(
            return_value=httpx.Response(200, content=body)
        )
        pbar = _mock.MagicMock()
        tqdm_ctor = _mock.MagicMock(return_value=pbar)
        with _mock.patch.dict("sys.modules", {"tqdm.auto": _mock.MagicMock(tqdm=tqdm_ctor)}):
            map_dataset_file_sync(
                tsv_path,
                name_column="name",
                provided_id_columns=["hmdb_id"],
                progress=True,
            )
        assert tqdm_ctor.call_args.kwargs == {"total": None, "desc": "Mapping dataset"}
        assert pbar.update.call_count == 3
        assert pbar.close.call_count == 1

    @respx.mock
    def test_total_hint_applies_when_progress_enabled(
        self, tsv_path: Any, api_key: str
    ) -> None:
        body = make_ndjson_body([make_batch_entry("A")])
        respx.post(DATASET_URL).mock(
            return_value=httpx.Response(200, content=body)
        )
        pbar = _mock.MagicMock()
        tqdm_ctor = _mock.MagicMock(return_value=pbar)
        with _mock.patch.dict("sys.modules", {"tqdm.auto": _mock.MagicMock(tqdm=tqdm_ctor)}):
            map_dataset_file_sync(
                tsv_path,
                name_column="name",
                provided_id_columns=["hmdb_id"],
                progress=True,
                total_hint=10,
            )
        assert tqdm_ctor.call_args.kwargs["total"] == 10

    @respx.mock
    def test_total_hint_without_progress_is_noop(
        self, tsv_path: Any, api_key: str
    ) -> None:
        body = make_ndjson_body([make_batch_entry("A")])
        respx.post(DATASET_URL).mock(
            return_value=httpx.Response(200, content=body)
        )
        # If tqdm were imported, this would fail — but progress=False must
        # skip the import regardless of total_hint.
        tqdm_ctor = _mock.MagicMock()
        with _mock.patch.dict("sys.modules", {"tqdm.auto": _mock.MagicMock(tqdm=tqdm_ctor)}):
            map_dataset_file_sync(
                tsv_path,
                name_column="name",
                provided_id_columns=["hmdb_id"],
                progress=False,
                total_hint=99,
            )
        tqdm_ctor.assert_not_called()


class TestMapDatasetFileSyncCallback:
    @respx.mock
    def test_on_result_called_once_per_result(
        self, tsv_path: Any, api_key: str
    ) -> None:
        body = make_ndjson_body(
            [make_batch_entry("A"), make_batch_entry("B"), make_batch_entry("C")]
        )
        respx.post(DATASET_URL).mock(
            return_value=httpx.Response(200, content=body)
        )
        cb = _mock.MagicMock()
        map_dataset_file_sync(
            tsv_path,
            name_column="name",
            provided_id_columns=["hmdb_id"],
            on_result=cb,
        )
        assert cb.call_count == 3
        names = [call.args[0].query_name for call in cb.call_args_list]
        assert names == ["A", "B", "C"]

    @respx.mock
    def test_progress_ticks_before_callback_per_result(
        self, tsv_path: Any, api_key: str
    ) -> None:
        # Verify ordering: pbar.update fires before on_result for each result.
        body = make_ndjson_body([make_batch_entry("A"), make_batch_entry("B")])
        respx.post(DATASET_URL).mock(
            return_value=httpx.Response(200, content=body)
        )
        order: list[str] = []
        pbar = _mock.MagicMock()
        pbar.update.side_effect = lambda *_a, **_kw: order.append("tick")
        cb = _mock.MagicMock(side_effect=lambda _r: order.append("cb"))
        tqdm_ctor = _mock.MagicMock(return_value=pbar)
        with _mock.patch.dict("sys.modules", {"tqdm.auto": _mock.MagicMock(tqdm=tqdm_ctor)}):
            map_dataset_file_sync(
                tsv_path,
                name_column="name",
                provided_id_columns=["hmdb_id"],
                progress=True,
                on_result=cb,
            )
        assert order == ["tick", "cb", "tick", "cb"]

    @respx.mock
    def test_callback_runtime_error_propagates(
        self, tsv_path: Any, api_key: str
    ) -> None:
        body = make_ndjson_body([make_batch_entry("A"), make_batch_entry("B")])
        respx.post(DATASET_URL).mock(
            return_value=httpx.Response(200, content=body)
        )
        call_count = 0

        def raising_cb(_r: MappingResult) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RuntimeError("user bug")

        with pytest.raises(RuntimeError, match="user bug"):
            map_dataset_file_sync(
                tsv_path,
                name_column="name",
                provided_id_columns=["hmdb_id"],
                on_result=raising_cb,
            )
        # The callback was invoked exactly twice (once successfully, once raising).
        assert call_count == 2

    @respx.mock
    def test_callback_value_error_propagates(
        self, tsv_path: Any, api_key: str
    ) -> None:
        # Regression: non-RuntimeError callback exceptions must not be
        # captured by the transport-error handler.
        body = make_ndjson_body([make_batch_entry("A"), make_batch_entry("B")])
        respx.post(DATASET_URL).mock(
            return_value=httpx.Response(200, content=body)
        )

        def raising_cb(_r: MappingResult) -> None:
            raise ValueError("nope")

        with pytest.raises(ValueError, match="nope"):
            map_dataset_file_sync(
                tsv_path,
                name_column="name",
                provided_id_columns=["hmdb_id"],
                on_result=raising_cb,
            )

    @respx.mock
    def test_callback_biomappererror_propagates(
        self, tsv_path: Any, api_key: str
    ) -> None:
        # Regression: even a BioMapperError raised by the callback must
        # propagate unchanged, not get captured into DatasetMappingResult.error.
        body = make_ndjson_body([make_batch_entry("A")])
        respx.post(DATASET_URL).mock(
            return_value=httpx.Response(200, content=body)
        )

        def raising_cb(_r: MappingResult) -> None:
            raise BioMapperRateLimitError("caller throttle", retry_after=1.0)

        with pytest.raises(BioMapperRateLimitError):
            map_dataset_file_sync(
                tsv_path,
                name_column="name",
                provided_id_columns=["hmdb_id"],
                on_result=raising_cb,
            )


class TestMapDatasetFileSyncErrors:
    @respx.mock
    def test_initial_401_propagates(
        self, tsv_path: Any, api_key: str
    ) -> None:
        respx.post(DATASET_URL).mock(return_value=httpx.Response(401))
        with pytest.raises(BioMapperAuthError):
            map_dataset_file_sync(
                tsv_path,
                name_column="name",
                provided_id_columns=["hmdb_id"],
            )

    @respx.mock
    def test_initial_422_propagates(
        self, tsv_path: Any, api_key: str
    ) -> None:
        respx.post(DATASET_URL).mock(
            return_value=httpx.Response(422, json={"detail": "bad"})
        )
        with pytest.raises(httpx.HTTPStatusError):
            map_dataset_file_sync(
                tsv_path,
                name_column="name",
                provided_id_columns=["hmdb_id"],
            )

    @respx.mock
    def test_midstream_read_timeout_captured_into_error(
        self, tsv_path: Any, api_key: str
    ) -> None:
        stream = make_truncating_stream(
            [make_batch_entry("A"), make_batch_entry("B")],
            httpx.ReadTimeout("mid-stream"),
        )
        respx.post(DATASET_URL).mock(
            return_value=httpx.Response(200, stream=stream)
        )
        result = map_dataset_file_sync(
            tsv_path, name_column="name", provided_id_columns=["hmdb_id"]
        )
        assert [r.query_name for r in result.results] == ["A", "B"]
        assert result.error is not None
        assert "mid-stream" in result.error

    @respx.mock
    def test_raise_for_error_reconstructs_exception_from_partial(
        self, tsv_path: Any, api_key: str
    ) -> None:
        # After mid-stream failure, raise_for_error() turns .error back into
        # a BioMapperError — the opt-in exception shape the tutorial uses.
        stream = make_truncating_stream(
            [make_batch_entry("A")], httpx.ReadTimeout("boom")
        )
        respx.post(DATASET_URL).mock(
            return_value=httpx.Response(200, stream=stream)
        )
        result = map_dataset_file_sync(
            tsv_path, name_column="name", provided_id_columns=["hmdb_id"]
        )
        with pytest.raises(Exception) as exc_info:
            result.raise_for_error()
        assert "boom" in str(exc_info.value)
        # Partial results still intact after raise
        assert len(result.results) == 1


class TestMapDatasetFileSyncKwargForwarding:
    @respx.mock
    def test_base_url_forwarded(
        self, tsv_path: Any, api_key: str
    ) -> None:
        # Custom base URL must reach the underlying client.
        alt_url = "https://alt.example.com/api/v1"
        respx.post(f"{alt_url}/map/dataset/stream").mock(
            return_value=httpx.Response(200, content=b"")
        )
        map_dataset_file_sync(
            tsv_path,
            name_column="name",
            provided_id_columns=["hmdb_id"],
            base_url=alt_url,
        )

    @respx.mock
    def test_httpx_timeout_forwarded(
        self, tsv_path: Any, api_key: str
    ) -> None:
        # Passing an httpx.Timeout instance should not coerce to float;
        # this covers the long-stream escape-hatch docstring claim.
        respx.post(DATASET_URL).mock(
            return_value=httpx.Response(200, content=b"")
        )
        timeout = httpx.Timeout(None, connect=30.0)  # None default; 30s connect
        # Should not raise — plain smoke test that the sync wrapper accepts
        # httpx.Timeout for its `timeout` parameter.
        result = map_dataset_file_sync(
            tsv_path,
            name_column="name",
            provided_id_columns=["hmdb_id"],
            timeout=timeout,
        )
        assert result.error is None
