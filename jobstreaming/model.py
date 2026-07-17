from __future__ import annotations

import hashlib
import json
from abc import ABC, abstractmethod
from datetime import date
from enum import Enum
from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)


class JobType(Enum):
    FULL_TIME = (
        "fulltime",
        "períodointegral",
        "cunormăîntreagă",
        "tiempocompleto",
        "vollzeit",
        "voltijds",
        "tempointegral",
        "全职",
        "plnýúvazek",
        "fuldtid",
        "دوامكامل",
        "kokopäivätyö",
        "tempsplein",
        "πλήρηςαπασχόληση",
        "teljesmunkaidő",
        "tempopieno",
        "heltid",
        "jornadacompleta",
        "pełnyetat",
        "정규직",
        "100%",
        "全職",
        "งานประจำ",
        "tamzamanlı",
        "повназайнятість",
        "toànthờigian",
    )
    PART_TIME = ("parttime", "teilzeit", "částečnýúvazek", "deltid")
    CONTRACT = ("contract", "contractor")
    TEMPORARY = ("temporary",)
    INTERNSHIP = (
        "internship",
        "prácticas",
        "estágio/trainee",
        "ojt(onthejobtraining)",
        "praktikum",
        "praktik",
    )
    PER_DIEM = ("perdiem",)
    NIGHTS = ("nights",)
    OTHER = ("other",)
    SUMMER = ("summer",)
    VOLUNTEER = ("volunteer",)

    @property
    def canonical(self) -> str:
        return self.value[0]

    @classmethod
    def from_string(cls, value: str) -> JobType:
        normalized = value.replace("-", "").replace(" ", "").lower()
        for job_type in cls:
            if normalized in job_type.value:
                return job_type
        raise ValueError(f"Unsupported job type: {value!r}")


class Country(Enum):
    """Indeed and Glassdoor domains plus the names accepted by the public API."""

    ARGENTINA = ("argentina", "ar", "com.ar")
    AUSTRALIA = ("australia", "au", "com.au")
    AUSTRIA = ("austria", "at", "at")
    BAHRAIN = ("bahrain", "bh")
    BANGLADESH = ("bangladesh", "bd")
    BELGIUM = ("belgium", "be", "fr:be")
    BULGARIA = ("bulgaria", "bg")
    BRAZIL = ("brazil", "br", "com.br")
    CANADA = ("canada", "ca", "ca")
    CHILE = ("chile", "cl")
    CHINA = ("china", "cn")
    COLOMBIA = ("colombia", "co")
    COSTARICA = ("costa rica", "cr")
    CROATIA = ("croatia", "hr")
    CYPRUS = ("cyprus", "cy")
    CZECHREPUBLIC = ("czech republic,czechia", "cz")
    DENMARK = ("denmark", "dk")
    ECUADOR = ("ecuador", "ec")
    EGYPT = ("egypt", "eg")
    ESTONIA = ("estonia", "ee")
    FINLAND = ("finland", "fi")
    FRANCE = ("france", "fr", "fr")
    GERMANY = ("germany", "de", "de")
    GREECE = ("greece", "gr")
    HONGKONG = ("hong kong", "hk", "com.hk")
    HUNGARY = ("hungary", "hu")
    INDIA = ("india", "in", "co.in")
    INDONESIA = ("indonesia", "id")
    IRELAND = ("ireland", "ie", "ie")
    ISRAEL = ("israel", "il")
    ITALY = ("italy", "it", "it")
    JAPAN = ("japan", "jp")
    KUWAIT = ("kuwait", "kw")
    LATVIA = ("latvia", "lv")
    LITHUANIA = ("lithuania", "lt")
    LUXEMBOURG = ("luxembourg", "lu")
    MALAYSIA = ("malaysia", "malaysia:my", "com")
    MALTA = ("malta", "malta:mt", "mt")
    MEXICO = ("mexico", "mx", "com.mx")
    MOROCCO = ("morocco", "ma")
    NETHERLANDS = ("netherlands", "nl", "nl")
    NEWZEALAND = ("new zealand", "nz", "co.nz")
    NIGERIA = ("nigeria", "ng")
    NORWAY = ("norway", "no")
    OMAN = ("oman", "om")
    PAKISTAN = ("pakistan", "pk")
    PANAMA = ("panama", "pa")
    PERU = ("peru", "pe")
    PHILIPPINES = ("philippines", "ph")
    POLAND = ("poland", "pl")
    PORTUGAL = ("portugal", "pt")
    QATAR = ("qatar", "qa")
    ROMANIA = ("romania", "ro")
    SAUDIARABIA = ("saudi arabia", "sa")
    SINGAPORE = ("singapore", "sg", "sg")
    SLOVAKIA = ("slovakia", "sk")
    SLOVENIA = ("slovenia", "sl")
    SOUTHAFRICA = ("south africa", "za")
    SOUTHKOREA = ("south korea", "kr")
    SPAIN = ("spain", "es", "es")
    SWEDEN = ("sweden", "se")
    SWITZERLAND = ("switzerland", "ch", "de:ch")
    TAIWAN = ("taiwan", "tw")
    THAILAND = ("thailand", "th")
    TURKEY = ("türkiye,turkey", "tr")
    UKRAINE = ("ukraine", "ua")
    UNITEDARABEMIRATES = ("united arab emirates", "ae")
    UK = ("uk,united kingdom", "uk:gb", "co.uk")
    USA = ("usa,us,united states", "www:us", "com")
    URUGUAY = ("uruguay", "uy")
    VENEZUELA = ("venezuela", "ve")
    VIETNAM = ("vietnam", "vn", "com")
    US_CANADA = ("usa/ca", "www")
    WORLDWIDE = ("worldwide", "www")

    @property
    def indeed_domain_value(self) -> tuple[str, str]:
        subdomain, _, api_country_code = self.value[1].partition(":")
        if subdomain and api_country_code:
            return subdomain, api_country_code.upper()
        return self.value[1], self.value[1].upper()

    @property
    def glassdoor_domain_value(self) -> str:
        if len(self.value) != 3:
            raise ValueError(f"Glassdoor is not available for {self.name}")
        subdomain, _, domain = self.value[2].partition(":")
        if subdomain and domain:
            return f"{subdomain}.glassdoor.{domain}"
        return f"www.glassdoor.{self.value[2]}"

    def get_glassdoor_url(self) -> str:
        return f"https://{self.glassdoor_domain_value}/"

    @classmethod
    def from_string(cls, country_str: str) -> Country:
        normalized = country_str.strip().lower()
        for country in cls:
            if normalized in country.value[0].split(","):
                return country
        valid = ", ".join(country.value[0] for country in cls)
        raise ValueError(
            f"Invalid country string: {country_str!r}. Valid countries are: {valid}"
        )


class FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", validate_assignment=True)


class Location(FrozenModel):
    country: Country | str | None = None
    city: str | None = None
    state: str | None = None

    def display_location(self) -> str:
        parts = [part for part in (self.city, self.state) if part]
        if isinstance(self.country, str):
            if self.country and self.country not in parts:
                parts.append(self.country)
        elif self.country and self.country not in (
            Country.US_CANADA,
            Country.WORLDWIDE,
        ):
            country_name = self.country.value[0].split(",")[0]
            display_name = (
                country_name.upper()
                if country_name in ("usa", "uk")
                else country_name.title()
            )
            if display_name not in parts:
                parts.append(display_name)
        return ", ".join(parts)


class CompensationInterval(str, Enum):
    YEARLY = "yearly"
    MONTHLY = "monthly"
    WEEKLY = "weekly"
    DAILY = "daily"
    HOURLY = "hourly"

    @classmethod
    def get_interval(cls, pay_period: str | None) -> CompensationInterval | None:
        if not pay_period:
            return None
        normalized = pay_period.upper()
        mapping = {
            "ANNUAL": cls.YEARLY,
            "YEAR": cls.YEARLY,
            "YEARLY": cls.YEARLY,
            "MONTH": cls.MONTHLY,
            "MONTHLY": cls.MONTHLY,
            "WEEK": cls.WEEKLY,
            "WEEKLY": cls.WEEKLY,
            "DAY": cls.DAILY,
            "DAILY": cls.DAILY,
            "HOUR": cls.HOURLY,
            "HOURLY": cls.HOURLY,
        }
        return mapping.get(normalized)


class Compensation(FrozenModel):
    interval: CompensationInterval
    min_amount: float | None = Field(default=None, ge=0)
    max_amount: float | None = Field(default=None, ge=0)
    currency: str

    @field_validator("currency")
    @classmethod
    def normalize_currency(cls, value: str) -> str:
        normalized = value.strip().upper()
        symbols = {"$": "USD", "€": "EUR", "£": "GBP", "₹": "INR"}
        normalized = symbols.get(normalized, normalized)
        if len(normalized) != 3 or not normalized.isalpha():
            raise ValueError("currency must be a three-letter ISO code")
        return normalized

    @model_validator(mode="after")
    def validate_range(self) -> Compensation:
        if self.min_amount is None and self.max_amount is None:
            raise ValueError("compensation requires at least one amount")
        if (
            self.min_amount is not None
            and self.max_amount is not None
            and self.min_amount > self.max_amount
        ):
            raise ValueError("minimum compensation cannot exceed maximum")
        return self


class DescriptionFormat(str, Enum):
    MARKDOWN = "markdown"
    HTML = "html"
    PLAIN = "plain"


class SalarySource(str, Enum):
    DIRECT_DATA = "direct_data"
    ESTIMATED = "estimated"
    DESCRIPTION = "description"


class JobPost(FrozenModel):
    id: str | None = None
    title: str
    company_name: str | None = None
    job_url: str
    job_url_direct: str | None = None
    location: Location | None = None
    description: str | None = None
    company_url: str | None = None
    company_url_direct: str | None = None
    job_type: tuple[JobType, ...] | None = None
    compensation: Compensation | None = None
    salary_source: SalarySource | None = None
    date_posted: date | None = None
    emails: tuple[str, ...] | None = None
    is_remote: bool | None = None
    listing_type: str | None = None
    job_level: str | None = None
    company_industry: str | None = None
    company_addresses: str | None = None
    company_num_employees: str | None = None
    company_revenue: str | None = None
    company_description: str | None = None
    company_logo: str | None = None
    banner_photo_url: str | None = None
    job_function: str | None = None
    skills: tuple[str, ...] | None = None
    experience_range: str | None = None
    company_rating: float | None = None
    company_reviews_count: int | None = None
    vacancy_count: int | None = None
    work_from_home_type: str | None = None

    @field_validator("title", "job_url")
    @classmethod
    def require_non_empty(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("value cannot be empty")
        return normalized

    @field_validator("job_url")
    @classmethod
    def require_http_url(cls, value: str) -> str:
        if not value.startswith(("http://", "https://")):
            raise ValueError("job_url must be an HTTP(S) URL")
        return value

    @field_validator("emails")
    @classmethod
    def deduplicate_emails(
        cls, value: tuple[str, ...] | None
    ) -> tuple[str, ...] | None:
        if not value:
            return None
        return tuple(dict.fromkeys(email.strip().lower() for email in value if email))


class JobResponse(FrozenModel):
    jobs: tuple[JobPost, ...] = ()


class Site(str, Enum):
    LINKEDIN = "linkedin"
    INDEED = "indeed"
    ZIP_RECRUITER = "zip_recruiter"
    GLASSDOOR = "glassdoor"
    GOOGLE = "google"
    BAYT = "bayt"
    NAUKRI = "naukri"
    BDJOBS = "bdjobs"

    @classmethod
    def from_string(cls, value: str) -> Site:
        normalized = value.strip().lower().replace("-", "_")
        aliases = {"ziprecruiter": cls.ZIP_RECRUITER}
        if normalized in aliases:
            return aliases[normalized]
        try:
            return cls(normalized)
        except ValueError as exc:
            raise ValueError(f"Unsupported site: {value!r}") from exc


class SearchRequest(FrozenModel):
    site_type: tuple[Site, ...]
    search_term: str | None = None
    google_search_term: str | None = None
    location: str | None = None
    country: Country = Country.USA
    distance: int = Field(default=50, ge=0)
    is_remote: bool = False
    job_type: JobType | None = None
    easy_apply: bool | None = None
    offset: int = Field(default=0, ge=0)
    linkedin_fetch_description: bool = False
    linkedin_company_ids: tuple[int, ...] | None = None
    description_format: DescriptionFormat = DescriptionFormat.MARKDOWN
    request_timeout: float = Field(default=30, gt=0)
    results_wanted: int = Field(default=15, ge=0)
    hours_old: int | None = Field(default=None, ge=0)
    max_pages: int = Field(default=50, ge=1, le=1_000)
    enforce_annual_salary: bool = False

    @field_validator("site_type")
    @classmethod
    def validate_sites(cls, sites: tuple[Site, ...]) -> tuple[Site, ...]:
        if not sites:
            raise ValueError("at least one site is required")
        return tuple(dict.fromkeys(sites))

    @property
    def sites(self) -> tuple[Site, ...]:
        return self.site_type

    def fingerprint(self) -> str:
        payload = json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


ScraperInput = SearchRequest


class AdapterCapabilities(FrozenModel):
    filters: frozenset[str] = frozenset()
    supports_resume: bool = False
    resume_granularity: str | None = None
    cursor_schema_version: int = Field(default=1, ge=1)


class Scraper(ABC):
    capabilities = AdapterCapabilities()

    def __init__(
        self,
        site: Site,
        proxies: list[str] | str | None = None,
        ca_cert: str | None = None,
        user_agent: str | None = None,
    ) -> None:
        self.site = site
        self.proxies = proxies
        self.ca_cert = ca_cert
        self.user_agent = user_agent

    @abstractmethod
    def scrape(
        self, scraper_input: SearchRequest, context: Any | None = None
    ) -> JobResponse: ...
