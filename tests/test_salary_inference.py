from __future__ import annotations

import pytest
from pydantic import ValidationError

from jobstreaming import (
    AdapterRegistry,
    Compensation,
    CompensationInterval,
    Country,
    DescriptionSalaryPolicy,
    JobPost,
    JobResponse,
    SalaryConfidence,
    SalaryProvenance,
    SalarySource,
    Scraper,
    SearchRequest,
    Site,
    build_search_request,
    stream_jobs,
)
from jobstreaming.result import job_to_row, normalize_job
from jobstreaming.salary import infer_salary_from_text


@pytest.mark.parametrize(
    (
        "text",
        "currency_hint",
        "interval",
        "minimum",
        "maximum",
        "currency",
    ),
    [
        (
            "Pay range: $20 - $30 per hour",
            "USD",
            CompensationInterval.HOURLY,
            20,
            30,
            "USD",
        ),
        (
            "Salaire : 50 000 € à 70 000 € par an",
            None,
            CompensationInterval.YEARLY,
            50_000,
            70_000,
            "EUR",
        ),
        (
            "Gehalt: 60.000 EUR bis 80.000 EUR pro Jahr",
            None,
            CompensationInterval.YEARLY,
            60_000,
            80_000,
            "EUR",
        ),
        (
            "Salario: 2.500 € - 3.200 € al mes",
            None,
            CompensationInterval.MONTHLY,
            2_500,
            3_200,
            "EUR",
        ),
        (
            "Compensation: £20 - £30 per hour",
            None,
            CompensationInterval.HOURLY,
            20,
            30,
            "GBP",
        ),
        (
            "Salary: ₹12,00,000 - ₹16,00,000 per year",
            None,
            CompensationInterval.YEARLY,
            1_200_000,
            1_600_000,
            "INR",
        ),
        (
            "Annual bonus eligible. Base salary: USD 50,000 - 70,000 per year",
            None,
            CompensationInterval.YEARLY,
            50_000,
            70_000,
            "USD",
        ),
        (
            "Salari: 45.000 EUR - 55.000 EUR a l'any",
            None,
            CompensationInterval.YEARLY,
            45_000,
            55_000,
            "EUR",
        ),
        (
            "Salário: 3.000 EUR - 4.000 EUR por mês",
            None,
            CompensationInterval.MONTHLY,
            3_000,
            4_000,
            "EUR",
        ),
        (
            "Stipendio: 45.000 EUR - 55.000 EUR all'anno",
            None,
            CompensationInterval.YEARLY,
            45_000,
            55_000,
            "EUR",
        ),
    ],
)
def test_conservative_salary_parser_handles_explicit_multilingual_ranges(
    text: str,
    currency_hint: str | None,
    interval: CompensationInterval,
    minimum: float,
    maximum: float,
    currency: str,
) -> None:
    inference = infer_salary_from_text(text, currency_hint=currency_hint)

    assert inference is not None
    assert inference.compensation == Compensation(
        interval=interval,
        min_amount=minimum,
        max_amount=maximum,
        currency=currency,
    )
    assert inference.provenance.source is SalarySource.DESCRIPTION
    assert inference.provenance.confidence is SalaryConfidence.MEDIUM
    assert inference.provenance.evidence


@pytest.mark.parametrize(
    ("text", "currency_hint"),
    [
        ("Competitive salary with excellent benefits", "USD"),
        ("Sign-on bonus: USD 5,000 - 10,000 per year", None),
        (
            "A discretionary sign-on bonus is included in this offer, with "
            "the one-time payment ranging from USD 5,000 - 10,000 per year",
            None,
        ),
        (
            "The range is USD 5,000 - 10,000 per year depending on "
            "performance and is paid entirely as an annual bonus",
            None,
        ),
        (
            "Compensation consists exclusively of commission based on sales, "
            "with earnings projected at USD 50,000 - 70,000 per year",
            None,
        ),
        (
            "The range is USD 50,000 - 70,000 per year and will be delivered "
            "as company stock equity",
            None,
        ),
        ("Budget: EUR 50 - 70", None),
        ("Experience: 2020 - 2025 per year", "USD"),
        ("Pay range: $20 - $30", "USD"),
        ("Pay range: $20 - $30 per hour", None),
        ("Pay range: $20 - $30 per year", "USD"),
        ("Pay range: $2 - $3 per month", "USD"),
        ("Pay range: USD 20 - 30 per hour or per week", None),
        ("Salary: USD 100,000 - 80,000 per year", None),
        ("Annual training budget: EUR 1,000 - 2,000 per year", None),
        ("Contract value: USD 50,000 - 70,000 per year", None),
        ("Salary budget: USD 50,000 - 70,000 per year", None),
        ("USD 50,000 - 70,000 per year", None),
        (
            "Salary: USD 50,000 - 70,000\n" "20 vacation days are available per year",
            None,
        ),
        ("Salary: USD 50,000 - 70,000\nSchedule per week", None),
    ],
)
def test_conservative_salary_parser_rejects_ambiguous_or_non_salary_ranges(
    text: str,
    currency_hint: str | None,
) -> None:
    assert infer_salary_from_text(text, currency_hint=currency_hint) is None


@pytest.mark.parametrize(
    "boundary",
    [
        "\r\n",
        "\r",
        "\n",
        "\u2028",
        "\u2029",
        "<br>",
        "<br/>",
        '<BR class="line-break">',
        "</p><p>",
        "</div><div>",
        "</li><li>",
        "</h2><h3>",
        "</td><td>",
        "</tr><tr>",
        "</section><article>",
    ],
)
@pytest.mark.parametrize(
    "unrelated_interval_text",
    [
        "Schedule per week",
        "20 vacation days are available per year",
    ],
)
def test_conservative_salary_parser_does_not_cross_line_or_html_boundaries(
    boundary: str,
    unrelated_interval_text: str,
) -> None:
    text = f"Salary: USD 50,000 - 70,000{boundary}" f"{unrelated_interval_text}"

    assert infer_salary_from_text(text) is None


@pytest.mark.parametrize(
    "text",
    [
        "Salary: ₹20 - ₹30 per hour",
        "Salary: INR 50 - 100 per day",
        "Salary: INR 100 - 200 per week",
        "Salary: INR 500 - 900 per month",
        "Salary: INR 1,000 - 2,000 per year",
        "Salary: ৳10 - ৳15 per hour",
        "Salary: BDT 50 - 100 per day",
        "Salary: BDT 100 - 200 per week",
        "Salary: BDT 500 - 900 per month",
        "Salary: BDT 1,000 - 2,000 per year",
    ],
)
def test_conservative_salary_parser_rejects_tiny_inr_and_bdt_ranges(
    text: str,
) -> None:
    assert infer_salary_from_text(text) is None


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (
            "Salary: ₹200 - ₹300 per hour",
            Compensation(
                interval=CompensationInterval.HOURLY,
                min_amount=200,
                max_amount=300,
                currency="INR",
            ),
        ),
        (
            "Salary: BDT 20,000 - 30,000 per month",
            Compensation(
                interval=CompensationInterval.MONTHLY,
                min_amount=20_000,
                max_amount=30_000,
                currency="BDT",
            ),
        ),
    ],
)
def test_conservative_salary_parser_keeps_plausible_inr_and_bdt_ranges(
    text: str,
    expected: Compensation,
) -> None:
    inference = infer_salary_from_text(text)

    assert inference is not None
    assert inference.compensation == expected


def test_description_salary_inference_is_explicit_and_carries_provenance() -> None:
    job = JobPost(
        id="description-1",
        title="Engineer",
        job_url="https://example.test/jobs/description-1",
        description="The salary range is $20 - $30 per hour.",
    )
    default_request = SearchRequest(site_type=(Site.INDEED,), country=Country.USA)
    enabled_request = SearchRequest(
        site_type=(Site.INDEED,),
        country=Country.USA,
        description_salary_policy=DescriptionSalaryPolicy.CONSERVATIVE,
    )

    assert normalize_job(job, default_request).compensation is None

    normalized = normalize_job(job, enabled_request)
    assert normalized.compensation == Compensation(
        interval=CompensationInterval.HOURLY,
        min_amount=20,
        max_amount=30,
        currency="USD",
    )
    assert normalized.salary_source is SalarySource.DESCRIPTION
    assert normalized.salary_provenance is not None
    assert normalized.salary_provenance.confidence is SalaryConfidence.MEDIUM
    assert normalized.salary_provenance.evidence == "$20 - $30 per hour"


def test_stream_normalization_applies_opt_in_salary_inference() -> None:
    class DescriptionSalaryAdapter(Scraper):
        def __init__(self, **_: object) -> None:
            super().__init__(Site.INDEED)

        def scrape(self, request, context=None) -> JobResponse:
            del request
            job = JobPost(
                id="stream-description-1",
                title="Engineer",
                job_url="https://example.test/jobs/stream-description-1",
                description="The salary range is $20 - $30 per hour.",
            )
            assert context is not None
            context.emit_job(job, {"page": 1})
            return JobResponse(jobs=(job,))

    registry = AdapterRegistry()
    registry.register(Site.INDEED, DescriptionSalaryAdapter)
    request = SearchRequest(
        site_type=(Site.INDEED,),
        country=Country.USA,
        results_wanted=1,
        description_salary_policy=DescriptionSalaryPolicy.CONSERVATIVE,
    )

    normalized = next(stream_jobs(request, registry=registry))

    assert normalized.compensation == Compensation(
        interval=CompensationInterval.HOURLY,
        min_amount=20,
        max_amount=30,
        currency="USD",
    )
    assert normalized.salary_source is SalarySource.DESCRIPTION
    assert normalized.salary_provenance is not None
    assert normalized.salary_provenance.confidence is SalaryConfidence.MEDIUM


def test_structured_board_salary_takes_precedence_over_description_text() -> None:
    board_compensation = Compensation(
        interval=CompensationInterval.YEARLY,
        min_amount=70_000,
        max_amount=90_000,
        currency="EUR",
    )
    job = JobPost(
        id="direct-1",
        title="Engineer",
        job_url="https://example.test/jobs/direct-1",
        description="Unrelated range: USD 20,000 - 30,000 per year",
        compensation=board_compensation,
        salary_source=SalarySource.DIRECT_DATA,
    )
    request = SearchRequest(
        site_type=(Site.GLASSDOOR,),
        description_salary_policy=DescriptionSalaryPolicy.CONSERVATIVE,
    )

    normalized = normalize_job(job, request)

    assert normalized.compensation == board_compensation
    assert normalized.salary_source is SalarySource.DIRECT_DATA
    assert normalized.salary_provenance == SalaryProvenance(
        source=SalarySource.DIRECT_DATA,
        confidence=SalaryConfidence.HIGH,
    )


def test_inferred_salary_can_be_annualized_without_losing_evidence() -> None:
    job = JobPost(
        title="Engineer",
        job_url="https://example.test/jobs/annualized",
        description="Pay range: $20 - $30 per hour",
    )
    request = SearchRequest(
        site_type=(Site.INDEED,),
        description_salary_policy=DescriptionSalaryPolicy.CONSERVATIVE,
        enforce_annual_salary=True,
    )

    normalized = normalize_job(job, request)

    assert normalized.compensation == Compensation(
        interval=CompensationInterval.YEARLY,
        min_amount=41_600,
        max_amount=62_400,
        currency="USD",
    )
    assert normalized.salary_provenance is not None
    assert normalized.salary_provenance.evidence == "$20 - $30 per hour"


def test_salary_provenance_requires_matching_compensation_metadata() -> None:
    provenance = SalaryProvenance(
        source=SalarySource.DESCRIPTION,
        confidence=SalaryConfidence.MEDIUM,
        evidence="USD 80,000 - 100,000 per year",
    )

    with pytest.raises(ValidationError, match="requires compensation"):
        JobPost(
            title="Engineer",
            job_url="https://example.test/jobs/no-compensation",
            salary_source=SalarySource.DESCRIPTION,
            salary_provenance=provenance,
        )

    with pytest.raises(ValidationError, match="source must match"):
        JobPost(
            title="Engineer",
            job_url="https://example.test/jobs/mismatch",
            compensation=Compensation(
                interval=CompensationInterval.YEARLY,
                min_amount=80_000,
                max_amount=100_000,
                currency="USD",
            ),
            salary_source=SalarySource.DIRECT_DATA,
            salary_provenance=provenance,
        )


def test_description_salary_source_requires_provenance_at_job_boundary() -> None:
    with pytest.raises(
        ValidationError,
        match="description salary source requires salary provenance",
    ):
        JobPost(
            title="Engineer",
            job_url="https://example.test/jobs/missing-description-evidence",
            compensation=Compensation(
                interval=CompensationInterval.YEARLY,
                min_amount=80_000,
                max_amount=100_000,
                currency="USD",
            ),
            salary_source=SalarySource.DESCRIPTION,
        )


@pytest.mark.parametrize(
    ("source", "confidence", "evidence"),
    [
        (SalarySource.DIRECT_DATA, SalaryConfidence.MEDIUM, None),
        (SalarySource.DIRECT_DATA, SalaryConfidence.HIGH, "board payload"),
        (SalarySource.ESTIMATED, SalaryConfidence.HIGH, None),
        (SalarySource.ESTIMATED, SalaryConfidence.MEDIUM, "board estimate"),
        (
            SalarySource.DESCRIPTION,
            SalaryConfidence.HIGH,
            "USD 80,000 - 100,000 per year",
        ),
        (SalarySource.DESCRIPTION, SalaryConfidence.MEDIUM, None),
    ],
)
def test_salary_provenance_enforces_source_confidence_and_evidence_contract(
    source: SalarySource,
    confidence: SalaryConfidence,
    evidence: str | None,
) -> None:
    with pytest.raises(ValidationError, match="salary provenance"):
        SalaryProvenance(
            source=source,
            confidence=confidence,
            evidence=evidence,
        )


def test_normalize_job_canonicalizes_unvalidated_structured_provenance() -> None:
    contradictory = SalaryProvenance.model_construct(
        source=SalarySource.DIRECT_DATA,
        confidence=SalaryConfidence.MEDIUM,
        evidence="must not survive normalization",
    )
    job = JobPost.model_construct(
        title="Engineer",
        job_url="https://example.test/jobs/contradictory-provenance",
        compensation=Compensation(
            interval=CompensationInterval.YEARLY,
            min_amount=80_000,
            max_amount=100_000,
            currency="USD",
        ),
        salary_source=SalarySource.DIRECT_DATA,
        salary_provenance=contradictory,
    )

    normalized = normalize_job(
        job,
        SearchRequest(site_type=(Site.INDEED,)),
    )

    assert normalized.salary_provenance == SalaryProvenance(
        source=SalarySource.DIRECT_DATA,
        confidence=SalaryConfidence.HIGH,
    )


def test_request_builder_and_fingerprint_include_salary_policy() -> None:
    default = build_search_request(site_name=Site.INDEED)
    enabled = build_search_request(
        site_name=Site.INDEED,
        description_salary_policy="conservative",
    )

    assert default.description_salary_policy is DescriptionSalaryPolicy.OFF
    assert enabled.description_salary_policy is DescriptionSalaryPolicy.CONSERVATIVE
    assert default.fingerprint() != enabled.fingerprint()


def test_dataframe_row_flattens_salary_provenance() -> None:
    job = JobPost(
        title="Engineer",
        job_url="https://example.test/jobs/row",
        compensation=Compensation(
            interval=CompensationInterval.YEARLY,
            min_amount=80_000,
            max_amount=100_000,
            currency="USD",
        ),
        salary_source=SalarySource.DESCRIPTION,
        salary_provenance=SalaryProvenance(
            source=SalarySource.DESCRIPTION,
            confidence=SalaryConfidence.MEDIUM,
            evidence="USD 80,000 - 100,000 per year",
        ),
    )

    row = job_to_row(Site.INDEED, job)

    assert row["salary_confidence"] == "medium"
    assert row["salary_evidence"] == "USD 80,000 - 100,000 per year"
    assert "salary_provenance" not in row
