"""Unit tests for company canonicalization, query groups, and company filters."""

from app import (
    canonicalize_company,
    classify_query,
    companies_match,
    filter_jobs_list,
    group_search_queries,
    normalize_company_key,
    _clean_str_list,
)


KNOWN = [
    "Accenture",
    "TCS",
    "Amazon",
    "PwC India",
    "Capgemini",
    "NTT DATA",
    "Google",
    "SAP",
]


def test_clean_str_list_drops_case_duplicates():
    assert _clean_str_list(["Google", " google ", "Amazon", "GOOGLE", ""]) == [
        "Google",
        "Amazon",
    ]


def test_normalize_company_key_strips_suffixes():
    assert normalize_company_key("Accenture in India") == "accenture"
    assert normalize_company_key("Amazon Web Services (AWS)") == "amazon"
    assert normalize_company_key("Capgemini Engineering") == "capgemini"
    assert normalize_company_key("NTT DATA, Inc.") == "ntt data"


def test_companies_match_linkedin_variants():
    assert companies_match("Accenture in India", ["Accenture"]) is True
    assert companies_match("Accenture Federal Services", ["Accenture"]) is True
    assert companies_match("Amazon Web Services (AWS)", ["Amazon"]) is True
    assert companies_match("PwC Acceleration Center India", ["PwC India"]) is True
    assert companies_match("Capgemini Engineering", ["Capgemini"]) is True
    assert companies_match("NTT DATA North America", ["NTT DATA"]) is True
    assert companies_match("Google", []) is True  # no selected list = keep all


def test_companies_match_does_not_false_positive_short_tokens():
    assert companies_match("Sapphire Solutions", ["SAP"]) is False
    assert companies_match("The EleFit Store", ["The Hartford"]) is False
    assert companies_match("Info Way Solutions", ["Info Origin Inc."]) is False


def test_canonicalize_picks_known_parent():
    assert canonicalize_company("Accenture in India", KNOWN) == "Accenture"
    assert canonicalize_company("Amazon Web Services (AWS)", KNOWN) == "Amazon"
    assert canonicalize_company("Zoho Corp", KNOWN) == "Zoho Corp"
    assert canonicalize_company("", KNOWN) == "Unknown"


def test_live_scrape_duplicate_companies_collapse():
    """Real live-scrape pattern: same employer, different LinkedIn banners."""
    jobs = [
        {"title": "Python Dev", "company": "Accenture in India"},
        {"title": "Java Dev", "company": "Accenture"},
        {"title": "SDE", "company": "Accenture Federal Services"},
        {"title": "SDE II", "company": "Amazon Web Services (AWS)"},
        {"title": "SDE", "company": "Amazon"},
    ]
    names = {canonicalize_company(j["company"], KNOWN) for j in jobs}
    assert names == {"Accenture", "Amazon"}


def test_filter_jobs_list_by_company():
    jobs = [
        {"title": "A", "company": "Accenture in India", "city": "Hyderabad", "country": "India"},
        {"title": "B", "company": "Turing", "city": "Hyderabad", "country": "India"},
        {"title": "C", "company": "Amazon Web Services (AWS)", "city": "Bengaluru", "country": "India"},
    ]
    out = filter_jobs_list(jobs, company_param="Accenture,Amazon")
    titles = {j["title"] for j in out}
    assert titles == {"A", "C"}


def test_filter_jobs_list_empty_company_keeps_all():
    jobs = [{"title": "A", "company": "Zoho"}, {"title": "B", "company": "Freshworks"}]
    assert len(filter_jobs_list(jobs, company_param="")) == 2


def test_filter_jobs_list_excludes_selected_companies_keeps_others():
    jobs = [
        {"title": "A", "company": "Accenture in India", "city": "Hyderabad", "country": "India"},
        {"title": "B", "company": "Turing", "city": "Hyderabad", "country": "India"},
        {"title": "C", "company": "Amazon Web Services (AWS)", "city": "Bengaluru", "country": "India"},
        {"title": "D", "company": "Zoho", "city": "Chennai", "country": "India"},
    ]
    out = filter_jobs_list(jobs, exclude_company_param="Accenture,Amazon")
    titles = {j["title"] for j in out}
    assert titles == {"B", "D"}
    assert filter_jobs_list(jobs, exclude_company_param="") == jobs


def test_classify_and_group_queries():
    queries = [
        "Python Developer",
        "HR Manager",
        "Talent Acquisition Specialist",
        "Go Developer",
        "People Partner",
    ]
    overrides = {"People Partner": "hr"}
    grouped = group_search_queries(queries, overrides)
    assert "Python Developer" in grouped["developer"]
    assert "Go Developer" in grouped["developer"]
    assert "HR Manager" in grouped["hr"]
    assert "Talent Acquisition Specialist" in grouped["hr"]
    assert "People Partner" in grouped["hr"]
    assert classify_query("People Partner") == "developer"
    assert classify_query("People Partner", overrides) == "hr"
    assert classify_query("HR Recruiter") == "hr"


def test_group_search_queries_dedupes():
    grouped = group_search_queries(["HR Manager", "hr manager", "Python Developer"])
    assert grouped["hr"] == ["HR Manager"]
    assert grouped["developer"] == ["Python Developer"]
