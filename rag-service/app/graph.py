"""Assembles the agentic RAG graph and its routing (conditional-edge) logic.

Flow:
    retrieve -> grade_documents -> [enough relevant docs?]
        no  -> transform_query -> retrieve (bounded by max_retrieval_retries)
        yes -> generate -> grade_generation -> [grounded & on-topic?]
            not grounded    -> generate     (bounded by max_generation_retries)
            grounded, off   -> transform_query (re-retrieve for a better answer)
            grounded & on   -> END
"""

from langgraph.graph import END, START, StateGraph

from .config import get_settings
from .nodes import GraphState, Nodes


def decide_after_grading(state: GraphState) -> str:
    settings = get_settings()
    if state.get("documents"):
        return "generate"
    # No relevant docs survived grading. Rewrite & retry, unless we've hit the ceiling.
    if state.get("retrieval_passes", 0) > settings.max_retrieval_retries:
        return "generate"  # generate will report insufficient context
    return "transform_query"


def decide_after_generation(state: GraphState) -> str:
    settings = get_settings()
    grounded = state.get("grounded", False)
    addresses = state.get("addresses_question", False)

    if not grounded and state.get("generation_passes", 0) <= settings.max_generation_retries:
        return "regenerate"
    if grounded and not addresses and state.get("retrieval_passes", 0) <= settings.max_retrieval_retries:
        return "re_retrieve"
    return "useful"


def build_graph(nodes: Nodes):
    graph = StateGraph(GraphState)
    graph.add_node("retrieve", nodes.retrieve)
    graph.add_node("grade_documents", nodes.grade_documents)
    graph.add_node("transform_query", nodes.transform_query)
    graph.add_node("generate", nodes.generate)
    graph.add_node("grade_generation", nodes.grade_generation)

    graph.add_edge(START, "retrieve")
    graph.add_edge("retrieve", "grade_documents")
    graph.add_conditional_edges(
        "grade_documents",
        decide_after_grading,
        {"transform_query": "transform_query", "generate": "generate"},
    )
    graph.add_edge("transform_query", "retrieve")
    graph.add_edge("generate", "grade_generation")
    graph.add_conditional_edges(
        "grade_generation",
        decide_after_generation,
        {"regenerate": "generate", "re_retrieve": "transform_query", "useful": END},
    )
    return graph.compile()
