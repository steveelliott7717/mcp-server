"""Unit tests for the graph's routing logic (no external services required)."""

from app.graph import decide_after_generation, decide_after_grading


def test_grading_routes_to_generate_when_docs_present():
    assert decide_after_grading({"documents": [object()]}) == "generate"


def test_grading_retries_when_no_docs_within_budget():
    assert decide_after_grading({"documents": [], "retrieval_passes": 1}) == "transform_query"


def test_grading_gives_up_after_retry_ceiling():
    # default max_retrieval_retries = 2, so pass 3 exceeds it
    assert decide_after_grading({"documents": [], "retrieval_passes": 3}) == "generate"


def test_generation_regenerates_when_ungrounded():
    assert decide_after_generation({"grounded": False, "generation_passes": 1}) == "regenerate"


def test_generation_re_retrieves_when_grounded_but_off_topic():
    state = {"grounded": True, "addresses_question": False, "retrieval_passes": 1}
    assert decide_after_generation(state) == "re_retrieve"


def test_generation_ends_when_grounded_and_on_topic():
    assert decide_after_generation({"grounded": True, "addresses_question": True}) == "useful"


def test_generation_stops_after_ceiling_even_if_ungrounded():
    assert decide_after_generation({"grounded": False, "generation_passes": 3}) == "useful"
