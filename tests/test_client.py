"""Tests for ddharmon.client.BioMapperClient."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator
from typing import Any

import httpx
import pytest
import respx

from ddharmon.client import BioMapperClient
from ddharmon.exceptions import (
    BioMapperAuthError,
    BioMapperConfigError,
    BioMapperRateLimitError,
    BioMapperServerError,
    BioMapperTimeoutError,
)
from tests.conftest import make_batch_entry, make_batch_response


BASE_URL = "https://biomapper.expertintheloop.io/api/v1"


@pytest.fixture()
def api_key() -> str:
    return "test-api-key-abc123"


@pytest.fixture()
def client(api_key: str) -> BioMapperClient:
    return BioMapperClient(api_key=api_key, timeout=5.0)


class TestBioMapperClientInit:
    def test_raises_without_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("BIOMAPPER_API_KEY", raising=False)
        with pytest.raises(BioMapperConfigError, match="API key"):
            BioMapperClient()

    def test_reads_env_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BIOMAPPER_API_KEY", "env-key-xyz")
        client = BioMapperClient()
        assert client._api_key == "env-key-xyz"

    def test_explicit_key_takes_precedence(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BIOMAPPER_API_KEY", "env-key")
        client = BioMapperClient(api_key="explicit-key")
        assert client._api_key == "explicit-key"


class TestListEntityTypes:
    @pytest.mark.asyncio()
    @respx.mock
    async def test_inverts_alias_map(self, client: BioMapperClient) -> None:
        respx.get(f"{BASE_URL}/entity-types").mock(
            return_value=httpx.Response(
                200,
                json={
                    "entity_types": ["biolink:SmallMolecule", "biolink:Protein"],
                    "aliases": {
                        "metabolite": "biolink:SmallMolecule",
                        "compound": "biolink:SmallMolecule",
                        "gene": "biolink:Protein",
                    },
                },
            )
        )
        async with client:
            result = await client.list_entity_types()

        assert len(result) == 2
        assert result[0].type == "biolink:SmallMolecule"
        assert result[0].aliases == ["compound", "metabolite"]  # sorted
        assert result[1].type == "biolink:Protein"
        assert result[1].aliases == ["gene"]

    @pytest.mark.asyncio()
    @respx.mock
    async def test_type_with_zero_aliases(self, client: BioMapperClient) -> None:
        respx.get(f"{BASE_URL}/entity-types").mock(
            return_value=httpx.Response(
                200,
                json={
                    "entity_types": ["biolink:Disease"],
                    "aliases": {},
                },
            )
        )
        async with client:
            result = await client.list_entity_types()

        assert len(result) == 1
        assert result[0].type == "biolink:Disease"
        assert result[0].aliases == []

    @pytest.mark.asyncio()
    @respx.mock
    async def test_alias_pointing_at_unknown_type_is_dropped_from_output(
        self, client: BioMapperClient
    ) -> None:
        # If the server's alias map references a type not in entity_types,
        # the alias has nowhere to land in our per-type output. This documents
        # that behavior rather than silently creating a phantom type entry.
        respx.get(f"{BASE_URL}/entity-types").mock(
            return_value=httpx.Response(
                200,
                json={
                    "entity_types": ["biolink:SmallMolecule"],
                    "aliases": {
                        "metabolite": "biolink:SmallMolecule",
                        "gene": "biolink:Protein",  # not in entity_types
                    },
                },
            )
        )
        async with client:
            result = await client.list_entity_types()

        assert [info.type for info in result] == ["biolink:SmallMolecule"]
        assert result[0].aliases == ["metabolite"]

    @pytest.mark.asyncio()
    @respx.mock
    async def test_auth_error(self, client: BioMapperClient) -> None:
        respx.get(f"{BASE_URL}/entity-types").mock(return_value=httpx.Response(401))
        async with client:
            with pytest.raises(BioMapperAuthError):
                await client.list_entity_types()

    @pytest.mark.asyncio()
    @respx.mock
    async def test_server_error(self, client: BioMapperClient) -> None:
        respx.get(f"{BASE_URL}/entity-types").mock(return_value=httpx.Response(500, text="boom"))
        async with client:
            with pytest.raises(BioMapperServerError):
                await client.list_entity_types()

    @pytest.mark.asyncio()
    @respx.mock
    async def test_timeout(self, client: BioMapperClient) -> None:
        respx.get(f"{BASE_URL}/entity-types").mock(side_effect=httpx.ReadTimeout("slow"))
        async with client:
            with pytest.raises(BioMapperTimeoutError):
                await client.list_entity_types()


class TestListAnnotators:
    @pytest.mark.asyncio()
    @respx.mock
    async def test_returns_annotator_list(self, client: BioMapperClient) -> None:
        respx.get(f"{BASE_URL}/annotators").mock(
            return_value=httpx.Response(
                200,
                json={
                    "annotators": [
                        {
                            "slug": "kestrel-vector-search",
                            "name": "Kestrel Vector Search",
                            "description": "Vector similarity over compound names",
                        },
                        {"slug": "metabolomics-workbench", "name": "Metabolomics Workbench"},
                    ]
                },
            )
        )
        async with client:
            result = await client.list_annotators()

        assert len(result) == 2
        assert result[0].slug == "kestrel-vector-search"
        assert result[0].description is not None
        assert result[1].slug == "metabolomics-workbench"
        assert result[1].description is None  # optional field omitted

    @pytest.mark.asyncio()
    @respx.mock
    async def test_auth_error(self, client: BioMapperClient) -> None:
        respx.get(f"{BASE_URL}/annotators").mock(return_value=httpx.Response(401))
        async with client:
            with pytest.raises(BioMapperAuthError):
                await client.list_annotators()


class TestListVocabularies:
    @pytest.mark.asyncio()
    @respx.mock
    async def test_returns_vocabulary_list(self, client: BioMapperClient) -> None:
        respx.get(f"{BASE_URL}/vocabularies").mock(
            return_value=httpx.Response(
                200,
                json={
                    "vocabularies": [
                        {
                            "prefix": "CHEBI",
                            "iri": "http://purl.obolibrary.org/obo/CHEBI_",
                            "aliases": ["chebi"],
                        },
                        {"prefix": "HMDB"},  # no iri, no aliases
                        {"prefix": "PUBCHEM", "aliases": []},
                    ],
                    "count": 3,
                },
            )
        )
        async with client:
            result = await client.list_vocabularies()

        assert len(result) == 3
        assert result[0].prefix == "CHEBI"
        assert result[0].iri == "http://purl.obolibrary.org/obo/CHEBI_"
        assert result[0].aliases == ["chebi"]
        assert result[1].prefix == "HMDB"
        assert result[1].iri is None
        assert result[1].aliases == []
        assert result[2].aliases == []

    @pytest.mark.asyncio()
    @respx.mock
    async def test_server_error(self, client: BioMapperClient) -> None:
        respx.get(f"{BASE_URL}/vocabularies").mock(return_value=httpx.Response(500))
        async with client:
            with pytest.raises(BioMapperServerError):
                await client.list_vocabularies()


class TestHealthCheck:
    @pytest.mark.asyncio()
    @respx.mock
    async def test_healthy_response(
        self,
        client: BioMapperClient,
        health_response: dict[str, Any],
    ) -> None:
        respx.get(f"{BASE_URL}/health").mock(
            return_value=httpx.Response(200, json=health_response)
        )
        async with client:
            result = await client.health_check()

        assert result["status"] == "healthy"
        assert result["mapper_initialized"] is True

    @pytest.mark.asyncio()
    @respx.mock
    async def test_auth_error(self, client: BioMapperClient) -> None:
        respx.get(f"{BASE_URL}/health").mock(return_value=httpx.Response(401))
        async with client:
            with pytest.raises(BioMapperAuthError):
                await client.health_check()

    @pytest.mark.asyncio()
    @respx.mock
    async def test_server_error(self, client: BioMapperClient) -> None:
        respx.get(f"{BASE_URL}/health").mock(return_value=httpx.Response(500, text="boom"))
        async with client:
            with pytest.raises(BioMapperServerError) as exc_info:
                await client.health_check()
        assert exc_info.value.status_code == 500


class TestMapEntity:
    @pytest.mark.asyncio()
    @respx.mock
    async def test_resolved_compound(
        self,
        client: BioMapperClient,
        sample_api_response: dict[str, Any],
    ) -> None:
        respx.post(f"{BASE_URL}/map/entity").mock(
            return_value=httpx.Response(200, json=sample_api_response)
        )
        async with client:
            result = await client.map_entity("L-Histidine")

        assert result.resolved is True
        assert result.primary_curie == "RM:0129894"
        assert result.query_name == "L-Histidine"

    @pytest.mark.asyncio()
    @respx.mock
    async def test_sends_hmdb_hint(
        self,
        client: BioMapperClient,
        sample_api_response: dict[str, Any],
    ) -> None:
        route = respx.post(f"{BASE_URL}/map/entity").mock(
            return_value=httpx.Response(200, json=sample_api_response)
        )
        async with client:
            result = await client.map_entity(
                "L-Histidine", identifiers={"HMDB": "HMDB00177"}
            )

        request_body = json.loads(route.calls[0].request.content)
        assert request_body["identifiers"] == {"HMDB": "HMDB00177"}
        assert result.hmdb_hint == "HMDB00177"

    @pytest.mark.asyncio()
    @respx.mock
    async def test_rate_limit_error(self, client: BioMapperClient) -> None:
        respx.post(f"{BASE_URL}/map/entity").mock(
            return_value=httpx.Response(429, headers={"Retry-After": "2"})
        )
        async with client:
            with pytest.raises(BioMapperRateLimitError) as exc_info:
                await client.map_entity("anything")
        assert exc_info.value.retry_after == pytest.approx(2.0)

    @pytest.mark.asyncio()
    @respx.mock
    async def test_timeout_raises(self, client: BioMapperClient) -> None:
        respx.post(f"{BASE_URL}/map/entity").mock(side_effect=httpx.ReadTimeout("timed out"))
        async with client:
            with pytest.raises(BioMapperTimeoutError):
                await client.map_entity("Glucose")


class TestMapEntities:
    """Native /map/batch behavior (0.3.0). Replaces the 0.2.0 simulated-batch tests."""

    @pytest.mark.asyncio()
    @respx.mock
    async def test_two_record_batch_single_call_ordered_results(
        self, client: BioMapperClient
    ) -> None:
        route = respx.post(f"{BASE_URL}/map/batch").mock(
            return_value=httpx.Response(
                200,
                json=make_batch_response(
                    [
                        make_batch_entry("L-Histidine"),
                        make_batch_entry("Z1005800534", resolved=False),
                    ]
                ),
            )
        )
        async with client:
            results = await client.map_entities(
                [{"name": "L-Histidine"}, {"name": "Z1005800534"}]
            )

        assert route.call_count == 1
        assert len(results) == 2
        assert results[0].resolved is True
        assert results[0].query_name == "L-Histidine"
        assert results[0].primary_curie == "RM:0129894"
        assert results[0].confidence_score == pytest.approx(2.4898567923359374)
        assert results[1].resolved is False
        assert results[1].query_name == "Z1005800534"

    @pytest.mark.asyncio()
    @respx.mock
    async def test_annotators_flow_into_request_options(
        self, client: BioMapperClient
    ) -> None:
        route = respx.post(f"{BASE_URL}/map/batch").mock(
            return_value=httpx.Response(
                200, json=make_batch_response([make_batch_entry("L-Histidine")])
            )
        )
        async with client:
            await client.map_entities(
                [{"name": "L-Histidine"}], annotators=["kestrel-vector-search"]
            )

        body = json.loads(route.calls[0].request.content)
        assert body["entities"][0]["options"]["annotators"] == ["kestrel-vector-search"]
        assert body["entities"][0]["options"]["annotation_mode"] == "missing"

    @pytest.mark.asyncio()
    @respx.mock
    async def test_hmdb_hint_uppercase_preserved(self, client: BioMapperClient) -> None:
        respx.post(f"{BASE_URL}/map/batch").mock(
            return_value=httpx.Response(
                200, json=make_batch_response([make_batch_entry("L-Histidine")])
            )
        )
        async with client:
            results = await client.map_entities(
                [{"name": "L-Histidine", "identifiers": {"HMDB": "HMDB00177"}}]
            )
        assert results[0].hmdb_hint == "HMDB00177"

    @pytest.mark.asyncio()
    @respx.mock
    async def test_hmdb_hint_lowercase_is_dropped(self, client: BioMapperClient) -> None:
        # Documents the case-sensitive lookup inherited from 0.2.0 (scope boundary).
        respx.post(f"{BASE_URL}/map/batch").mock(
            return_value=httpx.Response(
                200, json=make_batch_response([make_batch_entry("L-Histidine")])
            )
        )
        async with client:
            results = await client.map_entities(
                [{"name": "L-Histidine", "identifiers": {"hmdb": "HMDB00177"}}]
            )
        assert results[0].hmdb_hint is None

    @pytest.mark.asyncio()
    @respx.mock
    async def test_empty_records_no_http_calls(self, client: BioMapperClient) -> None:
        route = respx.post(f"{BASE_URL}/map/batch").mock(
            return_value=httpx.Response(200, json=make_batch_response([]))
        )
        async with client:
            results = await client.map_entities([])
        assert results == []
        assert route.call_count == 0

    @pytest.mark.asyncio()
    @respx.mock
    async def test_exactly_1000_records_single_call(self, client: BioMapperClient) -> None:
        entries = [make_batch_entry(f"name-{i}") for i in range(1000)]
        route = respx.post(f"{BASE_URL}/map/batch").mock(
            return_value=httpx.Response(200, json=make_batch_response(entries))
        )
        async with client:
            results = await client.map_entities([{"name": f"name-{i}"} for i in range(1000)])
        assert route.call_count == 1
        assert len(results) == 1000

    @pytest.mark.asyncio()
    @respx.mock
    async def test_1001_records_split_into_two_chunks(self, client: BioMapperClient) -> None:
        first_chunk_entries = [make_batch_entry(f"name-{i}") for i in range(1000)]
        second_chunk_entries = [make_batch_entry("name-1000")]
        route = respx.post(f"{BASE_URL}/map/batch").mock(
            side_effect=[
                httpx.Response(200, json=make_batch_response(first_chunk_entries)),
                httpx.Response(200, json=make_batch_response(second_chunk_entries)),
            ]
        )
        async with client:
            results = await client.map_entities(
                [{"name": f"name-{i}"} for i in range(1001)]
            )
        assert route.call_count == 2
        assert len(results) == 1001
        # Verify the first chunk request had 1000 entities and second had 1
        first_body = json.loads(route.calls[0].request.content)
        second_body = json.loads(route.calls[1].request.content)
        assert len(first_body["entities"]) == 1000
        assert len(second_body["entities"]) == 1
        # Verify input order preservation across chunks
        assert results[0].query_name == "name-0"
        assert results[999].query_name == "name-999"
        assert results[1000].query_name == "name-1000"

    @pytest.mark.asyncio()
    @respx.mock
    async def test_generator_records_materialized(self, client: BioMapperClient) -> None:
        respx.post(f"{BASE_URL}/map/batch").mock(
            return_value=httpx.Response(
                200,
                json=make_batch_response(
                    [make_batch_entry("a"), make_batch_entry("b")]
                ),
            )
        )

        def gen() -> Iterator[dict[str, Any]]:
            yield {"name": "a"}
            yield {"name": "b"}

        async with client:
            results = await client.map_entities(gen())
        assert len(results) == 2
        assert [r.query_name for r in results] == ["a", "b"]

    @pytest.mark.asyncio()
    @respx.mock
    async def test_per_record_error_in_200_response_isolated(
        self, client: BioMapperClient
    ) -> None:
        respx.post(f"{BASE_URL}/map/batch").mock(
            return_value=httpx.Response(
                200,
                json=make_batch_response(
                    [
                        make_batch_entry("bad", error="resolver failure"),
                        make_batch_entry("L-Histidine"),
                    ]
                ),
            )
        )
        async with client:
            results = await client.map_entities(
                [{"name": "bad"}, {"name": "L-Histidine"}]
            )
        assert len(results) == 2
        assert results[0].error == "resolver failure"
        assert results[0].resolved is False
        assert results[1].error is None
        assert results[1].resolved is True

    @pytest.mark.asyncio()
    @respx.mock
    async def test_chunk_500_errors_all_records_next_chunk_attempted(
        self, client: BioMapperClient
    ) -> None:
        # First chunk 500s, second chunk succeeds. 1001 records → 2 chunks.
        second_chunk_entries = [make_batch_entry("name-1000")]
        respx.post(f"{BASE_URL}/map/batch").mock(
            side_effect=[
                httpx.Response(500, text="boom"),
                httpx.Response(200, json=make_batch_response(second_chunk_entries)),
            ]
        )
        async with client:
            results = await client.map_entities(
                [{"name": f"name-{i}"} for i in range(1001)]
            )
        assert len(results) == 1001
        # First 1000 are errored
        assert all(r.error is not None for r in results[:1000])
        # Last one resolved from the successful second chunk
        assert results[1000].resolved is True
        assert results[1000].query_name == "name-1000"

    @pytest.mark.asyncio()
    @respx.mock
    async def test_chunk_400_validation_error_unpacked_to_per_record_errors(
        self, client: BioMapperClient
    ) -> None:
        # 400 is not mapped to a typed BioMapperError; it falls through to
        # httpx.HTTPStatusError. The broad except handler must catch it.
        respx.post(f"{BASE_URL}/map/batch").mock(
            return_value=httpx.Response(400, text="validation error")
        )
        async with client:
            results = await client.map_entities([{"name": "x"}, {"name": "y"}])
        assert len(results) == 2
        assert all(r.error is not None for r in results)

    @pytest.mark.asyncio()
    @respx.mock
    async def test_chunk_read_timeout_unpacked(self, client: BioMapperClient) -> None:
        respx.post(f"{BASE_URL}/map/batch").mock(side_effect=httpx.ReadTimeout("slow"))
        async with client:
            results = await client.map_entities([{"name": "a"}, {"name": "b"}])
        assert len(results) == 2
        assert all(r.error is not None for r in results)

    @pytest.mark.asyncio()
    @respx.mock
    async def test_chunk_network_error_unpacked_next_chunk_attempted(
        self, client: BioMapperClient
    ) -> None:
        second_chunk_entries = [make_batch_entry("name-1000")]
        respx.post(f"{BASE_URL}/map/batch").mock(
            side_effect=[
                httpx.ConnectError("refused"),
                httpx.Response(200, json=make_batch_response(second_chunk_entries)),
            ]
        )
        async with client:
            results = await client.map_entities(
                [{"name": f"name-{i}"} for i in range(1001)]
            )
        assert len(results) == 1001
        assert all(r.error is not None for r in results[:1000])
        assert results[1000].resolved is True

    @pytest.mark.asyncio()
    @respx.mock
    async def test_chunk_malformed_json_unpacked(self, client: BioMapperClient) -> None:
        respx.post(f"{BASE_URL}/map/batch").mock(
            return_value=httpx.Response(200, content=b"not-json")
        )
        async with client:
            results = await client.map_entities([{"name": "a"}, {"name": "b"}])
        assert len(results) == 2
        assert all(r.error is not None for r in results)

    @pytest.mark.asyncio()
    @respx.mock
    async def test_429_retry_after_courtesy_sleep(
        self,
        client: BioMapperClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # First chunk 429s with Retry-After: 2; second chunk succeeds.
        # Expect asyncio.sleep called with 2.0 (max of retry_after and rate_limit_delay).
        second_chunk_entries = [make_batch_entry("name-1000")]
        respx.post(f"{BASE_URL}/map/batch").mock(
            side_effect=[
                httpx.Response(429, headers={"Retry-After": "2"}),
                httpx.Response(200, json=make_batch_response(second_chunk_entries)),
            ]
        )
        sleep_calls: list[float] = []

        async def fake_sleep(delay: float) -> None:
            sleep_calls.append(delay)

        monkeypatch.setattr("asyncio.sleep", fake_sleep)

        async with client:
            results = await client.map_entities(
                [{"name": f"name-{i}"} for i in range(1001)]
            )
        assert len(results) == 1001
        assert all(r.error is not None for r in results[:1000])
        assert results[1000].resolved is True
        # Retry-After of 2s should have been applied as a courtesy sleep
        assert 2.0 in sleep_calls

    @pytest.mark.asyncio()
    async def test_cancelled_error_bubbles_out(
        self, client: BioMapperClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # respx discards BaseException side_effects in some versions; patch
        # the transport directly to guarantee the exception actually raises.
        async def cancel(*_args: Any, **_kwargs: Any) -> None:
            raise asyncio.CancelledError

        async with client:
            monkeypatch.setattr(client._http, "post", cancel)
            with pytest.raises(asyncio.CancelledError):
                await client.map_entities([{"name": "a"}])

    @pytest.mark.asyncio()
    @respx.mock
    async def test_deprecation_warning_on_non_zero_rate_limit_delay(
        self, client: BioMapperClient
    ) -> None:
        respx.post(f"{BASE_URL}/map/batch").mock(
            return_value=httpx.Response(
                200, json=make_batch_response([make_batch_entry("a")])
            )
        )
        async with client:
            with pytest.warns(DeprecationWarning, match="rate_limit_delay"):
                await client.map_entities([{"name": "a"}], rate_limit_delay=0.5)

    @pytest.mark.asyncio()
    @respx.mock
    async def test_no_warning_on_default_rate_limit_delay(
        self, client: BioMapperClient
    ) -> None:
        import warnings

        respx.post(f"{BASE_URL}/map/batch").mock(
            return_value=httpx.Response(
                200, json=make_batch_response([make_batch_entry("a")])
            )
        )
        async with client:
            with warnings.catch_warnings():
                warnings.simplefilter("error", DeprecationWarning)
                # Default rate_limit_delay is now 0.0 — should not emit.
                await client.map_entities([{"name": "a"}])

    @pytest.mark.asyncio()
    @respx.mock
    async def test_warning_fires_even_for_empty_records(
        self, client: BioMapperClient
    ) -> None:
        # The warning is about the caller setting a deprecated parameter,
        # not about work being done.
        async with client:
            with pytest.warns(DeprecationWarning, match="rate_limit_delay"):
                await client.map_entities([], rate_limit_delay=0.5)

    @pytest.mark.asyncio()
    @respx.mock
    async def test_between_chunks_delay_applied_once_for_two_chunks(
        self,
        client: BioMapperClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        entries_chunk1 = [make_batch_entry(f"n-{i}") for i in range(1000)]
        entries_chunk2 = [make_batch_entry("n-1000")]
        respx.post(f"{BASE_URL}/map/batch").mock(
            side_effect=[
                httpx.Response(200, json=make_batch_response(entries_chunk1)),
                httpx.Response(200, json=make_batch_response(entries_chunk2)),
            ]
        )
        sleep_calls: list[float] = []

        async def fake_sleep(delay: float) -> None:
            sleep_calls.append(delay)

        monkeypatch.setattr("asyncio.sleep", fake_sleep)

        async with client:
            with pytest.warns(DeprecationWarning):  # non-zero delay triggers it
                await client.map_entities(
                    [{"name": f"n-{i}"} for i in range(1001)],
                    rate_limit_delay=0.1,
                )
        # 2 chunks → exactly 1 between-chunks sleep at 0.1s
        assert sleep_calls == [pytest.approx(0.1)]

    @pytest.mark.asyncio()
    @respx.mock
    async def test_order_mismatch_emits_runtime_warning_but_continues(
        self, client: BioMapperClient
    ) -> None:
        # Server returns results in different order than entities sent.
        # Client warns once per instance but continues with position-based correlation.
        respx.post(f"{BASE_URL}/map/batch").mock(
            return_value=httpx.Response(
                200,
                json=make_batch_response(
                    [
                        make_batch_entry("second-sent"),  # swapped
                        make_batch_entry("first-sent"),
                    ]
                ),
            )
        )
        async with client:
            with pytest.warns(RuntimeWarning, match="order mismatch"):
                results = await client.map_entities(
                    [{"name": "first-sent"}, {"name": "second-sent"}]
                )
        # Position-based: query_name matches request position, not response.name
        assert results[0].query_name == "first-sent"
        assert results[1].query_name == "second-sent"
