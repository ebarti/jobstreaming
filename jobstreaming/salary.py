from __future__ import annotations

import re
from dataclasses import dataclass

from jobstreaming.model import (
    Compensation,
    CompensationInterval,
    Country,
    SalaryConfidence,
    SalaryProvenance,
    SalarySource,
)

_CURRENCY_TOKEN = (
    r"(?:US\$|USD|CA\$|C\$|CAD|AU\$|A\$|AUD|NZ\$|NZD|"
    r"EUR|GBP|INR|BDT|CHF|€|£|₹|৳|\$)"
)
_AMOUNT_TOKEN = (
    r"(?:"
    r"\d{1,2}(?:,\d{2})+,\d{3}|"
    r"\d{1,3}(?:[.,\u00a0 ]\d{3})+(?:[.,]\d{1,2})?|"
    r"\d+(?:[.,]\d{1,2})?"
    r")[kKmM]?"
)
_RANGE_PATTERN = re.compile(
    rf"""
    (?<![\w.,])
    (?:(?P<currency_1>{_CURRENCY_TOKEN})\s*)?
    (?P<amount_1>{_AMOUNT_TOKEN})
    (?:\s*(?P<currency_2>{_CURRENCY_TOKEN}))?
    \s*(?:[-–—]|to|bis|à|a|até)\s*
    (?:(?P<currency_3>{_CURRENCY_TOKEN})\s*)?
    (?P<amount_2>{_AMOUNT_TOKEN})
    (?:\s*(?P<currency_4>{_CURRENCY_TOKEN}))?
    (?![\w.,])
    """,
    re.IGNORECASE | re.VERBOSE,
)
_EXCLUDED_CONTEXT_PATTERN = re.compile(
    r"\b(?:bonus|commission|equity|stock|signing|sign-on|relocation)\b",
    re.IGNORECASE,
)
_COMPENSATION_CUE_PATTERN = re.compile(
    r"\b(?:salary|pay|wages?|compensation|remuneration|"
    r"salario|sueldo|salari|salaire|rémunération|"
    r"gehalt|vergütung|verguetung|lohn|salário|"
    r"stipendio|retribuzione)\b",
    re.IGNORECASE,
)
_NON_COMPENSATION_CONTEXT_PATTERN = re.compile(
    r"\b(?:budget|costs?|revenue|turnover|" r"(?:contract|project|deal)\s+value)\b",
    re.IGNORECASE,
)
_SENTENCE_BOUNDARY_PATTERN = re.compile(r"(?:\n+|[.!?;](?=\s|$))")

_INTERVAL_PATTERNS = {
    CompensationInterval.HOURLY: re.compile(
        r"(?:/\s*(?:h|hr|hour|hora|heure)\b|"
        r"\b(?:per|an?|por|par)\s+(?:hour|hora|heure)\b|"
        r"\bpro\s+stunde\b|\ball['’]ora\b|"
        r"\b(?:hourly|horaire|stündlich)\b)",
        re.IGNORECASE,
    ),
    CompensationInterval.DAILY: re.compile(
        r"(?:/\s*(?:day|día|dia|jour)\b|"
        r"\b(?:per|a|por)\s+(?:day|día|dia)\b|"
        r"\bpar\s+jour\b|\bpro\s+tag\b|"
        r"\b(?:daily|diario|diaria|quotidien|täglich)\b)",
        re.IGNORECASE,
    ),
    CompensationInterval.WEEKLY: re.compile(
        r"(?:/\s*(?:wk|week|semana|semaine)\b|"
        r"\b(?:per|a|por)\s+(?:week|semana)\b|"
        r"\bpar\s+semaine\b|\bpro\s+woche\b|"
        r"\b(?:weekly|semanal|hebdomadaire|wöchentlich)\b)",
        re.IGNORECASE,
    ),
    CompensationInterval.MONTHLY: re.compile(
        r"(?:/\s*(?:mo|month|mes|mois)\b|"
        r"\bper\s+month\b|\b(?:al|por)\s+mes\b|"
        r"\bpor\s+mês\b|\bpar\s+mois\b|\bpro\s+monat\b|"
        r"\b(?:monthly|mensual|mensal|mensuel|monatlich)\b)",
        re.IGNORECASE,
    ),
    CompensationInterval.YEARLY: re.compile(
        r"(?:/\s*(?:yr|year|año|ano|jahr)\b|"
        r"\bper\s+(?:year|annum)\b|\ba\s+year\b|"
        r"\b(?:al|por)\s+año\b|\bpor\s+ano\b|"
        r"\bpar\s+an\b|\bpro\s+jahr\b|\ba\s+l['’]any\b|"
        r"\ball['’]anno\b|\bp\.?\s*a\.?\b|"
        r"\b(?:yearly|annual|annually|anual|annuel|annuelle|"
        r"jährlich|annuo|annua)\b)",
        re.IGNORECASE,
    ),
}

_CURRENCY_ALIASES = {
    "US$": "USD",
    "USD": "USD",
    "CA$": "CAD",
    "C$": "CAD",
    "CAD": "CAD",
    "AU$": "AUD",
    "A$": "AUD",
    "AUD": "AUD",
    "NZ$": "NZD",
    "NZD": "NZD",
    "EUR": "EUR",
    "€": "EUR",
    "GBP": "GBP",
    "£": "GBP",
    "INR": "INR",
    "₹": "INR",
    "BDT": "BDT",
    "৳": "BDT",
    "CHF": "CHF",
}
_DOLLAR_CURRENCIES = {"USD", "CAD", "AUD", "NZD"}
_MAJOR_CURRENCIES = {"USD", "EUR", "GBP", "CAD", "AUD", "NZD", "CHF"}
_EURO_COUNTRIES = {
    Country.AUSTRIA,
    Country.BELGIUM,
    Country.CYPRUS,
    Country.ESTONIA,
    Country.FINLAND,
    Country.FRANCE,
    Country.GERMANY,
    Country.GREECE,
    Country.IRELAND,
    Country.ITALY,
    Country.LATVIA,
    Country.LITHUANIA,
    Country.LUXEMBOURG,
    Country.MALTA,
    Country.NETHERLANDS,
    Country.PORTUGAL,
    Country.SLOVAKIA,
    Country.SLOVENIA,
    Country.SPAIN,
}
_COUNTRY_CURRENCY = {
    Country.USA: "USD",
    Country.CANADA: "CAD",
    Country.AUSTRALIA: "AUD",
    Country.NEWZEALAND: "NZD",
    Country.UK: "GBP",
    Country.INDIA: "INR",
    Country.BANGLADESH: "BDT",
    Country.SWITZERLAND: "CHF",
}
_INTERVAL_MAXIMUM = {
    CompensationInterval.HOURLY: 10_000,
    CompensationInterval.DAILY: 100_000,
    CompensationInterval.WEEKLY: 500_000,
    CompensationInterval.MONTHLY: 5_000_000,
    CompensationInterval.YEARLY: 100_000_000,
}
_MAJOR_CURRENCY_MINIMUM = {
    CompensationInterval.HOURLY: 1,
    CompensationInterval.DAILY: 10,
    CompensationInterval.WEEKLY: 25,
    CompensationInterval.MONTHLY: 100,
    CompensationInterval.YEARLY: 1_000,
}
# Conservative annual floors (INR 60k, BDT 36k) projected across pay intervals.
# Rounding upward intentionally prefers rejecting ambiguous low-value text.
_REGIONAL_CURRENCY_MINIMUM = {
    "INR": {
        CompensationInterval.HOURLY: 40,
        CompensationInterval.DAILY: 300,
        CompensationInterval.WEEKLY: 1_500,
        CompensationInterval.MONTHLY: 5_000,
        CompensationInterval.YEARLY: 60_000,
    },
    "BDT": {
        CompensationInterval.HOURLY: 20,
        CompensationInterval.DAILY: 150,
        CompensationInterval.WEEKLY: 750,
        CompensationInterval.MONTHLY: 3_000,
        CompensationInterval.YEARLY: 36_000,
    },
}
_ANNUAL_FACTORS = {
    CompensationInterval.HOURLY: 2_080,
    CompensationInterval.DAILY: 260,
    CompensationInterval.WEEKLY: 52,
    CompensationInterval.MONTHLY: 12,
    CompensationInterval.YEARLY: 1,
}


@dataclass(frozen=True, slots=True)
class SalaryInference:
    compensation: Compensation
    provenance: SalaryProvenance


def currency_hint_for_country(country: Country) -> str | None:
    if country in _EURO_COUNTRIES:
        return "EUR"
    return _COUNTRY_CURRENCY.get(country)


def annualize_compensation(compensation: Compensation) -> Compensation:
    if compensation.interval is CompensationInterval.YEARLY:
        return compensation
    factor = _ANNUAL_FACTORS[compensation.interval]
    return Compensation(
        interval=CompensationInterval.YEARLY,
        min_amount=(
            compensation.min_amount * factor
            if compensation.min_amount is not None
            else None
        ),
        max_amount=(
            compensation.max_amount * factor
            if compensation.max_amount is not None
            else None
        ),
        currency=compensation.currency,
    )


def infer_salary_from_text(
    text: str,
    *,
    currency_hint: str | None = None,
) -> SalaryInference | None:
    if not text:
        return None
    for salary_range in _RANGE_PATTERN.finditer(text):
        interval_match = _nearest_interval(text, salary_range)
        if interval_match is None:
            continue
        interval, evidence_match = interval_match
        currency = _resolve_currency(
            tuple(
                marker
                for marker in (
                    salary_range.group("currency_1"),
                    salary_range.group("currency_2"),
                    salary_range.group("currency_3"),
                    salary_range.group("currency_4"),
                )
                if marker is not None
            ),
            currency_hint=currency_hint,
        )
        if currency is None:
            continue
        minimum = _parse_amount(salary_range.group("amount_1"))
        maximum = _parse_amount(salary_range.group("amount_2"))
        minimum, maximum = _propagate_suffix(minimum, maximum)
        if not _plausible_range(
            interval,
            minimum,
            maximum,
            currency=currency,
        ):
            continue
        evidence_start = min(salary_range.start(), evidence_match.start())
        evidence_end = max(salary_range.end(), evidence_match.end())
        surrounding = _sentence_context(text, evidence_start, evidence_end)
        if (
            not _COMPENSATION_CUE_PATTERN.search(surrounding)
            or _EXCLUDED_CONTEXT_PATTERN.search(surrounding)
            or _NON_COMPENSATION_CONTEXT_PATTERN.search(surrounding)
        ):
            continue
        evidence = " ".join(text[evidence_start:evidence_end].split())
        compensation = Compensation(
            interval=interval,
            min_amount=minimum[0] * minimum[1],
            max_amount=maximum[0] * maximum[1],
            currency=currency,
        )
        return SalaryInference(
            compensation=compensation,
            provenance=SalaryProvenance(
                source=SalarySource.DESCRIPTION,
                confidence=SalaryConfidence.MEDIUM,
                evidence=evidence[:160],
            ),
        )
    return None


def _nearest_interval(
    text: str,
    salary_range: re.Match[str],
) -> tuple[CompensationInterval, re.Match[str]] | None:
    window_start = max(0, salary_range.start() - 40)
    window_end = min(len(text), salary_range.end() + 40)
    candidates: list[tuple[int, CompensationInterval, re.Match[str]]] = []
    for interval, pattern in _INTERVAL_PATTERNS.items():
        for match in pattern.finditer(text, window_start, window_end):
            if match.end() <= salary_range.start():
                distance = salary_range.start() - match.end()
                intervening = text[match.end() : salary_range.start()]
            elif match.start() >= salary_range.end():
                distance = match.start() - salary_range.end()
                intervening = text[salary_range.end() : match.start()]
            else:
                distance = 0
                intervening = ""
            if (
                distance <= 32
                and _SENTENCE_BOUNDARY_PATTERN.search(intervening) is None
            ):
                candidates.append((distance, interval, match))
    if not candidates:
        return None
    if len({candidate[1] for candidate in candidates}) > 1:
        return None
    candidates.sort(key=lambda candidate: candidate[0])
    _, interval, match = candidates[0]
    return interval, match


def _resolve_currency(
    markers: tuple[str, ...],
    *,
    currency_hint: str | None,
) -> str | None:
    if not markers:
        return None
    ambiguous_dollar = any(marker == "$" for marker in markers)
    explicit = {
        _CURRENCY_ALIASES[marker.upper()] for marker in markers if marker != "$"
    }
    if len(explicit) > 1:
        return None
    if explicit:
        resolved = next(iter(explicit))
        if ambiguous_dollar and resolved not in _DOLLAR_CURRENCIES:
            return None
        return resolved
    normalized_hint = currency_hint.strip().upper() if currency_hint else None
    return (
        normalized_hint
        if ambiguous_dollar and normalized_hint in _DOLLAR_CURRENCIES
        else None
    )


def _parse_amount(raw: str) -> tuple[float, int]:
    suffix = raw[-1].lower() if raw[-1].isalpha() else ""
    multiplier = {"k": 1_000, "m": 1_000_000}.get(suffix, 1)
    numeric = raw[:-1] if suffix else raw
    numeric = numeric.replace(" ", "").replace("\u00a0", "")
    if "." in numeric and "," in numeric:
        decimal_separator = "." if numeric.rfind(".") > numeric.rfind(",") else ","
        thousands_separator = "," if decimal_separator == "." else "."
        numeric = numeric.replace(thousands_separator, "").replace(
            decimal_separator, "."
        )
    elif "," in numeric or "." in numeric:
        separator = "," if "," in numeric else "."
        if numeric.count(separator) > 1 or len(numeric.rsplit(separator, 1)[1]) == 3:
            numeric = numeric.replace(separator, "")
        else:
            numeric = numeric.replace(separator, ".")
    return float(numeric), multiplier


def _propagate_suffix(
    minimum: tuple[float, int],
    maximum: tuple[float, int],
) -> tuple[tuple[float, int], tuple[float, int]]:
    if minimum[1] == maximum[1]:
        return minimum, maximum
    if minimum[1] == 1:
        return (minimum[0], maximum[1]), maximum
    if maximum[1] == 1:
        return minimum, (maximum[0], minimum[1])
    return minimum, maximum


def _plausible_range(
    interval: CompensationInterval,
    minimum: tuple[float, int],
    maximum: tuple[float, int],
    *,
    currency: str,
) -> bool:
    minimum_value = minimum[0] * minimum[1]
    maximum_value = maximum[0] * maximum[1]
    regional_minimums = _REGIONAL_CURRENCY_MINIMUM.get(currency)
    minimum_allowed = (
        regional_minimums[interval]
        if regional_minimums is not None
        else (
            _MAJOR_CURRENCY_MINIMUM[interval] if currency in _MAJOR_CURRENCIES else None
        )
    )
    return (
        minimum_allowed is not None
        and minimum_allowed
        <= minimum_value
        < maximum_value
        <= _INTERVAL_MAXIMUM[interval]
        and maximum_value / minimum_value <= 20
    )


def _sentence_context(text: str, start: int, end: int) -> str:
    search_start = max(0, start - 240)
    search_end = min(len(text), end + 240)
    left = search_start
    for boundary in _SENTENCE_BOUNDARY_PATTERN.finditer(
        text,
        search_start,
        start,
    ):
        left = boundary.end()
    right_boundary = _SENTENCE_BOUNDARY_PATTERN.search(text, end, search_end)
    right = right_boundary.start() if right_boundary is not None else search_end
    return text[left:right]
