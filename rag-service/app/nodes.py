"""LangGraph node implementations for the agentic (corrective) RAG loop."""

import asyncio
from typing import TypedDict

from langchain_core.messages import HumanMessage, SystemMessage

from .config import get_settings
from .llm import get_generation_llm, get_grading_llm
from .models import GenerationGrade, GradeDocument, RetrievedDoc
from .retriever import HybridRetriever


class GraphState(TypedDict, total=False):
    question: str  # possibly-rewritten query used for retrieval
    original_question: str  # the user's original question, used for generation
    schema: str | None
    table: str | None
    match_count: int | None
    documents: list[RetrievedDoc]
    answer: str
    grounded: bool
    addresses_question: bool
    retrieval_passes: int
    generation_passes: int


def format_docs(docs: list[RetrievedDoc]) -> str:
    return "\n\n".join(f"[{i + 1}] {d.content}" for i, d in enumerate(docs)) or "(no documents)"


class Nodes:
    def __init__(self, retriever: HybridRetriever) -> None:
        self._retriever = retriever
        self._settings = get_settings()
        self._gen = get_generation_llm()
        self._grader = get_grading_llm()

    async def retrieve(self, state: GraphState) -> GraphState:
        docs = await self._retriever.retrieve(
            state["question"],
            state.get("schema"),
            state.get("table"),
            state.get("match_count"),
        )
        return {"documents": docs, "retrieval_passes": state.get("retrieval_passes", 0) + 1}

    async def grade_documents(self, state: GraphState) -> GraphState:
        grader = self._grader.with_structured_output(GradeDocument)
        question = state["question"]

        async def grade(doc: RetrievedDoc) -> RetrievedDoc | None:
            result: GradeDocument = await grader.ainvoke(
                [
                    SystemMessage(
                        content=(
                            "You grade whether a retrieved document is relevant to a question. "
                            "This is a coarse filter to drop clearly-unrelated results, not a "
                            "strict test. Grade relevant=true if the document is topically related "
                            "or could help answer the question even partially — including when it "
                            "doesn't explicitly name every entity in the question (the corpus is "
                            "one person's work history, so a chunk may describe a project without "
                            "restating the employer). Only grade relevant=false if it is clearly "
                            "about an unrelated topic."
                        )
                    ),
                    HumanMessage(content=f"Question: {question}\n\nDocument:\n{doc.content}"),
                ]
            )
            return doc if result.relevant else None

        graded = await asyncio.gather(*(grade(d) for d in state.get("documents", [])))
        return {"documents": [d for d in graded if d is not None]}

    async def transform_query(self, state: GraphState) -> GraphState:
        rewritten = await self._gen.ainvoke(
            [
                SystemMessage(
                    content=(
                        "Rewrite the user's question to retrieve better documents "
                        "(add synonyms/specificity). Return only the rewritten question."
                    )
                ),
                HumanMessage(content=state.get("original_question", state["question"])),
            ]
        )
        return {"question": str(rewritten.content).strip()}

    async def generate(self, state: GraphState) -> GraphState:
        answer = await self._gen.ainvoke(
            [
                SystemMessage(
                    content=(
                        "Answer the question using ONLY the provided context. If the context is "
                        "insufficient, say so plainly. Do not use knowledge outside the context."
                    )
                ),
                HumanMessage(
                    content=(
                        f"Question: {state.get('original_question', state['question'])}\n\n"
                        f"Context:\n{format_docs(state.get('documents', []))}"
                    )
                ),
            ]
        )
        return {
            "answer": str(answer.content).strip(),
            "generation_passes": state.get("generation_passes", 0) + 1,
        }

    async def grade_generation(self, state: GraphState) -> GraphState:
        grader = self._grader.with_structured_output(GenerationGrade)
        result: GenerationGrade = await grader.ainvoke(
            [
                SystemMessage(
                    content=(
                        "Grade the answer against the documents. The corpus is one person's own "
                        "work history, so attributing the described work to that person (or to the "
                        "person named in the question) is expected and is NOT an ungrounded claim. "
                        "grounded=true if the answer's factual claims — names, numbers, events — are "
                        "supported by the documents; ignore reasonable attribution and framing. "
                        "addresses_question=true if the answer responds to what was asked."
                    )
                ),
                HumanMessage(
                    content=(
                        f"Question: {state.get('original_question', state['question'])}\n\n"
                        f"Documents:\n{format_docs(state.get('documents', []))}\n\n"
                        f"Answer:\n{state.get('answer', '')}"
                    )
                ),
            ]
        )
        return {"grounded": result.grounded, "addresses_question": result.addresses_question}
