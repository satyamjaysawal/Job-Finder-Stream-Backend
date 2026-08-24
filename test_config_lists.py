"""Unit tests for config list CRUD helpers (no network)."""

from app import (
    CompanyItem,
    QueryItem,
    _clean_str_list,
    _norm_overrides,
    classify_query,
    group_search_queries,
)


def test_query_item_accepts_hr_group():
    item = QueryItem(query="  People Partner  ", group="HR")
    assert item.query == "People Partner"
    assert item.group == "hr"


def test_query_item_accepts_custom_group_slug():
    # Membership against known departments is enforced in set_query_group
    # (which can see the config doc), so the model accepts any slug.
    item = QueryItem(query="Python", group="  Sales  ")
    assert item.group == "sales"


def test_company_item_strips():
    item = CompanyItem(company="  Zoho  ")
    assert item.company == "Zoho"


def test_norm_overrides_keeps_valid_groups_only():
    raw = {
        "People Partner": "hr",
        "Python": "developer",
        "Bad": "ops",
        "": "hr",
        "x": "",
    }
    assert _norm_overrides(raw) == {
        "People Partner": "hr",
        "Python": "developer",
    }


def test_norm_overrides_keeps_registered_custom_groups():
    raw = {"Account Executive": "sales", "Bad": "ops"}
    assert _norm_overrides(raw, custom_groups=["sales"]) == {
        "Account Executive": "sales",
    }


def test_group_search_queries_supports_custom_departments():
    queries = ["Python Developer", "Account Executive", "HR Ops Lead"]
    overrides = {"Account Executive": "sales"}
    grouped = group_search_queries(queries, overrides, custom_groups=["sales"])
    assert grouped["developer"] == ["Python Developer"]
    assert grouped["hr"] == ["HR Ops Lead"]
    assert grouped["sales"] == ["Account Executive"]


def test_clean_str_list_preserves_first_casing():
    assert _clean_str_list(["TCS", "tcs", "Infosys"]) == ["TCS", "Infosys"]


def test_developer_and_hr_custom_land_in_right_bucket():
    queries = ["Python Developer", "Staff Engineer", "People Partner", "HR Ops Lead"]
    overrides = {"People Partner": "hr", "Staff Engineer": "developer"}
    grouped = group_search_queries(queries, overrides)
    assert grouped["developer"] == ["Python Developer", "Staff Engineer"]
    assert grouped["hr"] == ["People Partner", "HR Ops Lead"]
    assert classify_query("HR Ops Lead") == "hr"
