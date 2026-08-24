"""Tests for URL-based job deduplication keys."""

from app import normalize_job_url


def test_normalize_job_url_removes_linkedin_tracking_values():
    assert normalize_job_url(
        "HTTPS://WWW.LinkedIn.com/jobs/view/software-engineer-12345/"
        "?trackingId=first-search&refId=abc#details"
    ) == "https://www.linkedin.com/jobs/view/software-engineer-12345"


def test_normalize_job_url_makes_url_variants_share_one_key():
    variants = {
        normalize_job_url("https://www.linkedin.com/jobs/view/12345?trackingId=one"),
        normalize_job_url("https://www.linkedin.com/jobs/view/12345?trackingId=two#top"),
        normalize_job_url("https://www.linkedin.com/jobs/view/12345/"),
    }
    assert variants == {"https://www.linkedin.com/jobs/view/12345"}


def test_normalize_job_url_keeps_distinct_job_urls_distinct():
    assert normalize_job_url("https://www.linkedin.com/jobs/view/12345") != normalize_job_url(
        "https://www.linkedin.com/jobs/view/67890"
    )


def test_normalize_job_url_rejects_blank_values():
    assert normalize_job_url("  ") == ""
    assert normalize_job_url("NaN") == ""
