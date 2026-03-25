"""Pydantic v2 models for BioMapper2 API request/response shapes."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, model_validator

# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------


class MapEntityRequest(BaseModel):
    """Payload for POST /api/v1/map/entity.

    The ``options`` dict supports:
        - ``annotation_mode``: "missing" | "all" | "none"
        - ``annotators``: List of annotator names to use (e.g., ["kestrel-vector-search"])

    When ``annotators`` is not specified, BioMapper2 uses all available annotators,
    which may include fuzzy matchers that return low-confidence matches for vendor codes.
    """

    name: str
    entity_type: str = "biolink:SmallMolecule"
    identifiers: dict[str, str] = Field(default_factory=dict)
    options: dict[str, Any] = Field(default_factory=lambda: {"annotation_mode": "missing"})

    model_config = {"populate_by_name": True}


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class ApiMetadata(BaseModel):
    """Metadata block returned alongside every API result."""

    request_id: str = ""
    processing_time_ms: float = 0.0


class RawApiResult(BaseModel):
    """Direct mapping of the ``result`` object in the API response.

    Kept intentionally permissive so the client doesn't break on API additions.
    """

    name: str | None = None
    curies: list[str] = Field(default_factory=list)
    chosen_kg_id: str | None = None
    kg_ids: dict[str, list[str]] = Field(default_factory=dict)
    assigned_ids: dict[str, dict[str, dict[str, Any]]] = Field(default_factory=dict)
    error: str | None = None


class RawApiResponse(BaseModel):
    """Top-level envelope returned by POST /map/entity."""

    result: RawApiResult | None = None
    metadata: ApiMetadata = Field(default_factory=ApiMetadata)


# ---------------------------------------------------------------------------
# Derived / user-facing model
# ---------------------------------------------------------------------------


class MappingResult(BaseModel):
    """Clean, flat representation of a single compound mapping.

    Derived from :class:`RawApiResponse` via :meth:`from_api_response`.

    Attributes:
        query_name:       The name submitted to the API (after preprocessing).
        resolved:         Whether at least one identifier was returned.
        primary_curie:    First CURIE in the response list.
        chosen_kg_id:     The resolver-selected knowledge graph ID.
        confidence_score: Highest confidence score found across annotators.
        identifiers:      Vocabulary → list[id] mapping (e.g. ``{"CHEBI": ["15971"]}``).
        hmdb_hint:        HMDB hint that was passed in the request, if any.
        error:            Error message if the request or mapping failed.
        raw_response:     Full parsed API response for downstream inspection.
    """

    query_name: str
    resolved: bool = False
    primary_curie: str | None = None
    chosen_kg_id: str | None = None
    confidence_score: float | None = None
    identifiers: dict[str, list[str]] = Field(default_factory=dict)
    hmdb_hint: str | None = None
    error: str | None = None
    raw_response: RawApiResponse | None = None

    @classmethod
    def from_api_response(
        cls,
        response: dict[str, Any],
        query_name: str,
        hmdb_hint: str | None = None,
    ) -> MappingResult:
        """Build a :class:`MappingResult` from a raw API response dict.

        Args:
            response:   Parsed JSON dict from the API.
            query_name: The name that was submitted.
            hmdb_hint:  Any HMDB identifier that was passed as a hint.

        Returns:
            A populated :class:`MappingResult`.
        """
        base: dict[str, Any] = {
            "query_name": query_name,
            "hmdb_hint": hmdb_hint,
        }

        # Surface-level error (network / HTTP, captured before reaching this method)
        if "error" in response and "result" not in response:
            base["error"] = str(response["error"])
            return cls(**base)

        parsed = RawApiResponse.model_validate(response)
        base["raw_response"] = parsed

        r = parsed.result
        if r is None:
            base["error"] = "Empty result from API"
            return cls(**base)

        if r.error:
            base["error"] = r.error
            return cls(**base)

        if r.curies:
            base["resolved"] = True
            base["primary_curie"] = r.curies[0]

        base["chosen_kg_id"] = r.chosen_kg_id

        # Flatten assigned_ids → {vocab: [code, ...]} and extract best score
        identifiers: dict[str, list[str]] = {}
        best_score: float | None = None

        for _annotator, vocabs in r.assigned_ids.items():
            for vocab, codes in vocabs.items():
                for code, meta in codes.items():
                    identifiers.setdefault(vocab, []).append(code)
                    if isinstance(meta, dict) and "score" in meta:
                        score = float(meta["score"])
                        if best_score is None or score > best_score:
                            best_score = score

        base["identifiers"] = identifiers
        base["confidence_score"] = best_score

        return cls(**base)

    # ------------------------------------------------------------------
    # Convenience accessors
    # ------------------------------------------------------------------

    def ids_for(self, vocab: str) -> list[str]:
        """Return all identifiers for the given vocabulary prefix.

        Example::

            result.ids_for("CHEBI")   # ["15971"]
            result.ids_for("refmet_id")  # ["RM0129894"]
        """
        return self.identifiers.get(vocab, [])

    @property
    def confidence_tier(self) -> str:
        """Human-readable confidence tier based on score.

        Returns one of ``"high"``, ``"medium"``, ``"low"``, or ``"unknown"``.
        """
        if self.confidence_score is None:
            return "unknown"
        if self.confidence_score >= 2.0:
            return "high"
        if self.confidence_score >= 1.0:
            return "medium"
        return "low"


class MappingSummary(BaseModel):
    """Aggregate statistics for a batch mapping run."""

    total_queried: int
    resolved: int
    unresolved: int
    errors: int
    resolution_rate: float
    vocabulary_coverage: dict[str, int] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _compute_rate(cls, data: dict[str, Any]) -> dict[str, Any]:
        total = data.get("total_queried", 0)
        resolved = data.get("resolved", 0)
        if "resolution_rate" not in data:
            data["resolution_rate"] = resolved / total if total > 0 else 0.0
        return data

    @classmethod
    def from_results(cls, results: list[MappingResult]) -> MappingSummary:
        """Compute summary statistics from a list of mapping results."""
        resolved = sum(1 for r in results if r.resolved)
        errors = sum(1 for r in results if r.error and not r.resolved)
        vocab_coverage: dict[str, int] = {}
        for r in results:
            for vocab in r.identifiers:
                vocab_coverage[vocab] = vocab_coverage.get(vocab, 0) + 1
        total = len(results)
        return cls(
            total_queried=total,
            resolved=resolved,
            unresolved=total - resolved,
            errors=errors,
            resolution_rate=resolved / total if total > 0 else 0.0,
            vocabulary_coverage=vocab_coverage,
        )
