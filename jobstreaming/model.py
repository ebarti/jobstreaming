from __future__ import annotations

import hashlib
import json
import re
import warnings
from abc import ABC, abstractmethod
from datetime import date
from enum import Enum
from typing import Any, ClassVar, Literal, TypeAlias, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)
from pydantic_core import core_schema


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
        return cast(str, self.value[0])

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


class AdapterId(str):
    """Validated identifier for a third-party adapter."""

    _PATTERN = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$")

    def __new__(cls, value: str) -> AdapterId:
        normalized = value.strip().lower()
        if (
            not normalized
            or len(normalized) > 64
            or not cls._PATTERN.fullmatch(normalized)
        ):
            raise ValueError(
                "adapter identifier must be 1-64 lowercase letters, digits, "
                "dots, underscores, or hyphens and start with a letter"
            )
        try:
            built_in = Site.from_string(normalized)
        except ValueError:
            pass
        else:
            raise ValueError(
                f"{normalized!r} is built in; use Site.{built_in.name} instead"
            )
        return str.__new__(cls, normalized)

    @property
    def value(self) -> str:
        return str(self)

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: Any,
    ) -> core_schema.CoreSchema:
        del source_type, handler
        return core_schema.no_info_after_validator_function(
            cls,
            core_schema.str_schema(),
            serialization=core_schema.to_string_ser_schema(),
        )


AdapterIdentifier: TypeAlias = Site | AdapterId
AdapterIdentifierInput: TypeAlias = Site | AdapterId | str


def parse_adapter_identifier(value: AdapterIdentifierInput) -> AdapterIdentifier:
    if isinstance(value, (Site, AdapterId)):
        return value
    try:
        return Site.from_string(value)
    except ValueError:
        return AdapterId(value)


class SearchFilter(str, Enum):
    SEARCH_TERM = "search_term"
    GOOGLE_SEARCH_TERM = "google_search_term"
    LOCATION = "location"
    COUNTRY = "country"
    DISTANCE = "distance"
    IS_REMOTE = "is_remote"
    JOB_TYPE = "job_type"
    EASY_APPLY = "easy_apply"
    OFFSET = "offset"
    LINKEDIN_FETCH_DESCRIPTION = "linkedin_fetch_description"
    LINKEDIN_COMPANY_IDS = "linkedin_company_ids"
    DESCRIPTION_FORMAT = "description_format"
    REQUEST_TIMEOUT = "request_timeout"
    RESULTS_WANTED = "results_wanted"
    HOURS_OLD = "hours_old"
    MAX_PAGES = "max_pages"
    ENFORCE_ANNUAL_SALARY = "enforce_annual_salary"


class SearchRequest(FrozenModel):
    site_type: tuple[AdapterIdentifier, ...]
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

    @field_validator("site_type", mode="before")
    @classmethod
    def parse_sites(cls, sites: Any) -> Any:
        if isinstance(sites, (str, Site, AdapterId)):
            sites = (sites,)
        return tuple(parse_adapter_identifier(site) for site in sites)

    @field_validator("country", mode="before")
    @classmethod
    def restore_country_enum(cls, country: Any) -> Any:
        return tuple(country) if isinstance(country, list) else country

    @field_validator("job_type", mode="before")
    @classmethod
    def restore_job_type_enum(cls, job_type: Any) -> Any:
        return tuple(job_type) if isinstance(job_type, list) else job_type

    @field_validator("site_type")
    @classmethod
    def validate_sites(
        cls, sites: tuple[AdapterIdentifier, ...]
    ) -> tuple[AdapterIdentifier, ...]:
        if not sites:
            raise ValueError("at least one site is required")
        return tuple(dict.fromkeys(sites))

    @property
    def sites(self) -> tuple[AdapterIdentifier, ...]:
        return self.site_type

    def fingerprint(self) -> str:
        payload = json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


ScraperInput = SearchRequest


class ResumeGranularity(str):
    """Open, validated resume boundary identifier for built-in and custom adapters."""

    LISTING: ClassVar[ResumeGranularity]
    PAGE: ClassVar[ResumeGranularity]
    CURSOR: ClassVar[ResumeGranularity]
    CONTINUATION_TOKEN: ClassVar[ResumeGranularity]

    _PATTERN = re.compile(r"^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$")

    def __new__(cls, value: str) -> ResumeGranularity:
        normalized = re.sub(r"[\s-]+", "_", value.strip().lower())
        normalized = re.sub(r"_+", "_", normalized)
        if (
            not normalized
            or len(normalized) > 64
            or not cls._PATTERN.fullmatch(normalized)
        ):
            raise ValueError(
                "resume granularity must be 1-64 lowercase letters, digits, "
                "or underscores and start with a letter"
            )
        return str.__new__(cls, normalized)

    @property
    def value(self) -> str:
        return str(self)

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: Any,
    ) -> core_schema.CoreSchema:
        del source_type, handler
        return core_schema.no_info_after_validator_function(
            cls,
            core_schema.str_schema(),
            serialization=core_schema.to_string_ser_schema(),
        )


ResumeGranularity.LISTING = ResumeGranularity("listing")
ResumeGranularity.PAGE = ResumeGranularity("page")
ResumeGranularity.CURSOR = ResumeGranularity("cursor")
ResumeGranularity.CONTINUATION_TOKEN = ResumeGranularity("continuation_token")


class NoResume(FrozenModel):
    kind: Literal["none"] = "none"


class Resumable(FrozenModel):
    kind: Literal["resumable"] = "resumable"
    granularity: ResumeGranularity
    cursor_schema_version: int = Field(default=1, ge=1)


ResumeSupport: TypeAlias = NoResume | Resumable


class AdapterCapabilities(FrozenModel):
    filters: frozenset[SearchFilter] = frozenset()
    supported_job_types: frozenset[JobType] | None = None
    resume: ResumeSupport = Field(default_factory=NoResume, discriminator="kind")

    @model_validator(mode="before")
    @classmethod
    def migrate_legacy_resume_declaration(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        legacy_fields = {
            "supports_resume",
            "resume_granularity",
            "cursor_schema_version",
        }
        if not legacy_fields.intersection(value):
            return value
        migrated = dict(value)
        supports_resume = bool(migrated.pop("supports_resume", False))
        granularity = migrated.pop("resume_granularity", None)
        cursor_schema_version = migrated.pop("cursor_schema_version", 1)
        if supports_resume and granularity is None:
            raise ValueError(
                "resume_granularity is required when supports_resume is true"
            )
        if not supports_resume and granularity is not None:
            raise ValueError("resume_granularity requires supports_resume to be true")
        warnings.warn(
            "supports_resume, resume_granularity, and cursor_schema_version are "
            "deprecated; pass resume=Resumable(...) or resume=NoResume()",
            DeprecationWarning,
            stacklevel=3,
        )
        migrated["resume"] = (
            {
                "kind": "resumable",
                "granularity": granularity,
                "cursor_schema_version": cursor_schema_version,
            }
            if supports_resume
            else {"kind": "none"}
        )
        return migrated

    @model_validator(mode="after")
    def validate_declarations(self) -> AdapterCapabilities:
        if (
            self.supported_job_types is not None
            and SearchFilter.JOB_TYPE not in self.filters
        ):
            raise ValueError(
                "supported_job_types requires the job_type filter capability"
            )
        return self

    @property
    def supports_resume(self) -> bool:
        return isinstance(self.resume, Resumable)

    @property
    def resume_granularity(self) -> ResumeGranularity | None:
        return self.resume.granularity if isinstance(self.resume, Resumable) else None

    @property
    def cursor_schema_version(self) -> int:
        return (
            self.resume.cursor_schema_version
            if isinstance(self.resume, Resumable)
            else 1
        )


class Scraper(ABC):
    capabilities = AdapterCapabilities()

    def __init__(
        self,
        site: AdapterIdentifier,
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
