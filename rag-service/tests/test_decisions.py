"""Unit tests for the graph's routing logic (no external services required)."""

import operator

from app.graph import decide_after_generation, decide_after_grading
from app.models import GradeEvent


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


def test_grade_log_reducer_accumulates_across_passes():
    # The grade_log field uses operator.add so retries append rather than
    # overwrite — mirrors how the graph threads events across passes.
    pass1 = [GradeEvent(stage="documents", pass_no=1, reasoning="r1", relevant=True)]
    pass2 = [GradeEvent(stage="generation", pass_no=1, reasoning="r2", grounded=False)]
    combined = operator.add(pass1, pass2)
    assert [e.stage for e in combined] == ["documents", "generation"]
    assert len(combined) == 2
