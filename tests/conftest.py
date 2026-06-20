"""Shared test factories for the harmonization (v1) test suite.

Exposed via the ``hf`` fixture (a SimpleNamespace of factory callables) so the
harmonization tests can build Fields / EmbeddedDictionaries / vectors without
the real sentence-transformers model. Existing tests are unaffected (they inline
their own helpers; these fixtures are opt-in by name).
"""

from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pytest
from numpy.typing import NDArray

from ddharmon.embedding.service import EmbeddedDictionary
from ddharmon.models.data_dictionary import DataDictionary, Field


def _l2(vecs: NDArray) -> NDArray[np.float32]:
    """L2-normalize rows (zero rows left as-is)."""
    arr = np.asarray(vecs, dtype=np.float32)
    norms = np.linalg.norm(arr, axis=1, keepdims=True)
    return arr / np.where(norms == 0, 1.0, norms)


def _field(
    var: str,
    desc: str,
    *,
    encoding: str | None = None,
    data_type: str | None = None,
    units: str | None = None,
    category: str | None = None,
    field_id: str | None = None,
    standard_codes: dict[str, list[str]] | None = None,
    question_text: str | None = None,
) -> Field:
    return Field(
        variable_name=var,
        description=desc,
        value_encoding_raw=encoding,
        data_type=data_type,
        units=units,
        category=category,
        field_id=field_id,
        standard_codes=standard_codes or {},
        question_text=question_text,
    )


def _embedded_dict(
    cohort: str,
    fields_spec: list[Field],
    *,
    sem_vecs: NDArray,
    val_vecs: list[NDArray | None] | None = None,
) -> EmbeddedDictionary:
    """Build an EmbeddedDictionary from Fields + semantic vectors (+ optional value vectors)."""
    fields = {f.variable_name: f for f in fields_spec}
    dd = DataDictionary(name=cohort, fields=fields, cohort_name=cohort)
    embeddings = {f.variable_name: np.asarray(sem_vecs[i], dtype=np.float32) for i, f in enumerate(fields_spec)}
    value_embeddings: dict[str, NDArray[np.float32]] = {}
    if val_vecs is not None:
        for i, f in enumerate(fields_spec):
            if val_vecs[i] is not None:
                value_embeddings[f.variable_name] = np.asarray(val_vecs[i], dtype=np.float32)
    return EmbeddedDictionary(
        dictionary=dd,
        embeddings=embeddings,
        model_name="test-model",
        value_embeddings=value_embeddings,
    )


@pytest.fixture
def hf() -> SimpleNamespace:
    """Harmonization factory namespace: ``hf.l2``, ``hf.field``, ``hf.embedded_dict``."""
    return SimpleNamespace(l2=_l2, field=_field, embedded_dict=_embedded_dict)
