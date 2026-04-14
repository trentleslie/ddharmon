"""Tests for ddharmon.client.BioMapperClient."""

from __future__ import annotations

import json
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
    @pytest.mark.asyncio()
    @respx.mock
    async def test_batch_returns_ordered_results(
        self,
        client: BioMapperClient,
        sample_api_response: dict[str, Any],
        sample_unresolved_response: dict[str, Any],
    ) -> None:
        respx.post(f"{BASE_URL}/map/entity").mock(
            side_effect=[
                httpx.Response(200, json=sample_api_response),
                httpx.Response(200, json=sample_unresolved_response),
            ]
        )
        async with client:
            results = await client.map_entities(
                [{"name": "L-Histidine"}, {"name": "Z1005800534"}],
                rate_limit_delay=0.0,
            )

        assert len(results) == 2
        assert results[0].resolved is True
        assert results[1].resolved is False

    @pytest.mark.asyncio()
    @respx.mock
    async def test_individual_errors_do_not_abort_batch(
        self,
        client: BioMapperClient,
        sample_api_response: dict[str, Any],
    ) -> None:
        respx.post(f"{BASE_URL}/map/entity").mock(
            side_effect=[
                httpx.Response(500, text="boom"),
                httpx.Response(200, json=sample_api_response),
            ]
        )
        async with client:
            results = await client.map_entities(
                [{"name": "bad"}, {"name": "L-Histidine"}],
                rate_limit_delay=0.0,
            )

        assert len(results) == 2
        assert results[0].error is not None
        assert results[1].resolved is True
