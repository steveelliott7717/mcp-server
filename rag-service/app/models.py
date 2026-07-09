"""Pydantic models: API request/response and LLM structured-output grading schemas."""

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


class QueryResponse(BaseModel):
    answer: str
    documents: list[RetrievedDoc]
    retrieval_passes: int
    generation_passes: int
    grounded: bool
    addresses_question: bool


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
