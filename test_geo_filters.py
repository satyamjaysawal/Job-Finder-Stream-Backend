"""Unit tests for optional city/country scrape location plans (no network)."""

from app import (
    build_location_plans,
    filter_jobs_list,
    resolve_indeed_country,
    _job_matches_country,
    format_time_ago_hr_min,
    serialize_job,
)


def test_resolve_empty_is_global():
    assert resolve_indeed_country([], default_if_empty="") == ""
    assert resolve_indeed_country([], default_if_empty=None) == ""
    assert resolve_indeed_country(["India"]) == "India"
    assert resolve_indeed_country(["USA"]) == "USA"


def test_plans_global():
    plans = build_location_plans([], [])
    assert len(plans) == 1
    assert plans[0]["label"] == "Global"
    assert plans[0]["location"] == ""
    assert plans[0]["country_indeed"] == ""
    assert plans[0]["filter_countries"] == []


def test_plans_city_only():
    plans = build_location_plans(["Hyderabad", "Bengaluru"], [])
    assert len(plans) == 2
    assert plans[0]["location"] == "Hyderabad"
    assert plans[0]["country_indeed"] == ""
    assert plans[1]["location"] == "Bengaluru"


def test_plans_country_only():
    plans = build_location_plans([], ["India", "Germany"])
    assert len(plans) == 2
    assert plans[0]["location"] == "India"
    assert plans[0]["country_indeed"] == "India"
    assert plans[0]["filter_countries"] == ["India"]
    assert plans[1]["location"] == "Germany"


def test_plans_city_and_country():
    plans = build_location_plans(["Hyderabad"], ["India"])
    assert len(plans) == 1
    assert plans[0]["location"] == "Hyderabad"
    assert plans[0]["country_indeed"] == "India"
    assert plans[0]["filter_countries"] == ["India"]


def test_filter_empty_geo_keeps_all():
    jobs = [
        {"title": "A", "city": "London", "location": "London, UK", "country": "UK"},
        {"title": "B", "city": "Hyderabad", "location": "Hyderabad, India", "country": "India"},
    ]
    out = filter_jobs_list(jobs, city_param="", country_param="", strict_search=False)
    assert len(out) == 2


def test_filter_city_only():
    jobs = [
        {"title": "A", "city": "London", "location": "London, UK", "country": "UK"},
        {"title": "B", "city": "Hyderabad", "location": "Hyderabad, India", "country": "India"},
    ]
    out = filter_jobs_list(jobs, city_param="Hyderabad", country_param="", strict_search=False)
    assert len(out) == 1
    assert out[0]["title"] == "B"


def test_filter_country_only():
    jobs = [
        {"title": "A", "city": "London", "location": "London, United Kingdom", "country": "UK"},
        {"title": "B", "city": "Hyderabad", "location": "Hyderabad, India", "country": "India"},
    ]
    out = filter_jobs_list(jobs, city_param="", country_param="India", strict_search=False)
    assert len(out) == 1
    assert out[0]["title"] == "B"


def test_filter_city_and_country():
    jobs = [
        {"title": "A", "city": "Hyderabad", "location": "Hyderabad, India", "country": "India"},
        {"title": "B", "city": "Hyderabad", "location": "Hyderabad, Pakistan", "country": "Pakistan"},
        {"title": "C", "city": "Pune", "location": "Pune, India", "country": "India"},
    ]
    out = filter_jobs_list(
        jobs, city_param="Hyderabad", country_param="India", strict_search=False
    )
    assert len(out) == 1
    assert out[0]["title"] == "A"


def test_country_match_helper():
    job = {"country": "India", "location": "Bengaluru, Karnataka, India"}
    assert _job_matches_country(job, []) is True
    assert _job_matches_country(job, ["india"]) is True
    assert _job_matches_country(job, ["germany"]) is False


def test_format_time_ago_hr_min():
    assert format_time_ago_hr_min("5 minutes ago") == "5m"
    assert format_time_ago_hr_min("25m") == "25m"
    assert format_time_ago_hr_min("1 hour ago") == "1h"
    assert format_time_ago_hr_min("2 hours ago") == "2h"
    assert format_time_ago_hr_min("2h 15m") == "2h"
    assert format_time_ago_hr_min("1 day ago") == "24h"
    assert format_time_ago_hr_min("3 days ago") == "72h"
    assert format_time_ago_hr_min("just now") == "1m"
    assert format_time_ago_hr_min("02:15") == "2h"
    assert format_time_ago_hr_min("00:45") == "45m"
    assert format_time_ago_hr_min("") == ""
    assert format_time_ago_hr_min(None) == ""


def test_serialize_job_attributes():
    # 1. Parse directly from preset fields
    j1 = {
        "title": "Software Engineer",
        "description": "Python dev",
        "is_easy_apply": True,
        "workplace_type": "hybrid",
        "job_type": "contract",
        "experience_level": "mid_senior"
    }
    out1 = serialize_job(j1)
    assert out1["is_easy_apply"] is True
    assert out1["workplace_type"] == "hybrid"
    assert out1["job_type"] == "contract"
    assert out1["experience_level"] == "experienced"

    # 2. Parse from title and description fallbacks
    j2 = {
        "title": "Generative AI Intern (Remote)",
        "description": "This is an easy apply position. Looking for fresher developers to join full time.",
    }
    out2 = serialize_job(j2)
    assert out2["is_easy_apply"] is True
    assert out2["workplace_type"] == "remote"
    assert out2["job_type"] == "internship"
    assert out2["experience_level"] == "fresher"

    # 3. Exact user example from LinkedIn UI
    j3 = {
        "title": "Software Engineer III",
        "description": "Promoted by hirer · No response insights available yet\nOn-site\nContract\nEasy Apply\nSave",
        "location": "Bengaluru, Karnataka, India",
        "time_ago": "Reposted 34 minutes ago",
    }
    out3 = serialize_job(j3)
    assert out3["is_easy_apply"] is True
    assert out3["workplace_type"] == "onsite"
    assert out3["job_type"] == "contract"
    assert out3["time_ago"] == "34m"


if __name__ == "__main__":
    tests = [
        test_resolve_empty_is_global,
        test_plans_global,
        test_plans_city_only,
        test_plans_country_only,
        test_plans_city_and_country,
        test_filter_empty_geo_keeps_all,
        test_filter_city_only,
        test_filter_country_only,
        test_filter_city_and_country,
        test_country_match_helper,
        test_format_time_ago_hr_min,
        test_serialize_job_attributes,
    ]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"  OK  {t.__name__}")
        except Exception as e:
            failed += 1
            import traceback
            traceback.print_exc()
            print(f"  FAIL  {t.__name__}: {e}")
    print()
    if failed:
        print(f"{failed}/{len(tests)} failed")
        raise SystemExit(1)
    print(f"All {len(tests)} tests passed.")
