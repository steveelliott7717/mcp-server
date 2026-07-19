"""Pydantic models: API request/response and LLM structured-output grading schemas."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


# --------------------------------------------------------------------------- #
# API contract
# --------------------------------------------------------------------------- #
class QueryRequest(BaseModel):
    # `schema` collides with BaseModel.schema(); expose it via alias.
    model_config = ConfigDict(populate_by_name=True)

    question: str = Field(..., min_length=1)
    schema_name: str | None = Field(default=None, alias="schema")
    table: str | None = None
    match_count: int | None = Field(default=None, ge=1, le=20)


class RetrievedDoc(BaseModel):
    id: str
    content: str
    score: float
    source_filename: str | None = None
    section_title: str | None = None


class GradeEvent(BaseModel):
    """One grading decision, captured for observability.

    The grader already produces a `reasoning` string on every call (see the
    schemas below); this record stops it being discarded, so a run's full
    grading sequence — which verdict fired on which pass, and why — is legible
    afterward instead of being reconstructed from pass counts.
    """

    stage: Literal["documents", "generation"]
    pass_no: int  # retrieval pass (documents) or generation pass this grade belongs to
    reasoning: str
    # documents-stage fields
    doc_id: str | None = None
    relevant: bool | None = None
    # generation-stage fields
    grounded: bool | None = None
    addresses_question: bool | None = None


class QueryResponse(BaseModel):
    answer: str
    documents: list[RetrievedDoc]
    retrieval_passes: int
    generation_passes: int
    grounded: bool
    addresses_question: bool
    grade_log: list[GradeEvent] = Field(default_factory=list)


# --------------------------------------------------------------------------- #
# LLM structured-output grading schemas (the "quality gates", done for real)
# --------------------------------------------------------------------------- #
class GradeDocument(BaseModel):
    """Relevance grade for a single retrieved document."""

    reasoning: str = Field(
        description="One sentence on whether the document relates to the question."
    )
    relevant: bool = Field(
        description="True if the document is topically related to or could help answer the question."
    )


class GenerationGrade(BaseModel):
    """Groundedness + answer-relevance grade for a generated answer."""

    reasoning: str = Field(
        description="One sentence assessing groundedness and whether the answer addresses the question."
    )
    grounded: bool = Field(
        description="True only if every claim in the answer is supported by the documents."
    )
    addresses_question: bool = Field(
        description="True only if the answer actually resolves the user's question."
    )
