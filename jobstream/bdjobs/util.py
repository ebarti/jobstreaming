# util.py
from datetime import date, datetime
from typing import Any

from bs4 import BeautifulSoup

from jobstream.model import Country, Location


def parse_location(
    location_text: str | None, country: str = "bangladesh"
) -> Location | None:
    """
    Parses location text into a Location object
    :param location_text: Location text from job listing
    :param country: Default country
    :return: Location object
    """
    if not location_text or not location_text.strip():
        return None
    parts = [part.strip() for part in location_text.split(",") if part.strip()]
    country_enum = Country.from_string(country)
    has_country_suffix = bool(
        parts and parts[-1].lower() in country_enum.value[0].split(",")
    )
    if len(parts) >= 2:
        state_parts = parts[1:-1] if has_country_suffix else parts[1:]
        return Location(
            city=parts[0],
            state=", ".join(state_parts) or None,
            country=country_enum,
        )
    return Location(city=parts[0], country=country_enum)


def parse_date(date_text: str) -> date | None:
    """
    Parses date text into a datetime object
    :param date_text: Date text from job listing
    :return: datetime object or None if parsing fails
    """
    from .constant import date_formats

    try:
        # Clean up date text
        lowered = date_text.lower()
        if "deadline" in lowered:
            return None
        date_text = date_text.replace("Published:", "").strip()

        # Try different date formats
        for fmt in date_formats:
            try:
                return datetime.strptime(date_text, fmt).date()
            except ValueError:
                continue

        return None
    except Exception:
        return None


def find_job_listings(soup: BeautifulSoup) -> list[Any]:
    """
    Finds job listing elements in the HTML
    :param soup: BeautifulSoup object
    :return: List of job card elements
    """
    from .constant import job_selectors

    # Try different selectors
    for selector in job_selectors:
        if "." in selector:
            tag_name, class_name = selector.split(".", 1)
            elements = soup.find_all(tag_name, class_=class_name)
            if elements and len(elements) > 0:
                return elements

    # If no selectors match, look for job detail links
    job_links = soup.find_all("a", href=lambda h: h and "jobdetail" in h.lower())
    if job_links:
        # Return parent elements of job links
        return [link.parent for link in job_links]

    return []


def is_job_remote(
    title: str,
    description: str | None = None,
    location: Location | None = None,
) -> bool:
    """
    Determines if a job is remote based on title, description, and location
    :param title: Job title
    :param description: Job description
    :param location: Job location
    :return: True if job is remote, False otherwise
    """
    remote_keywords = ["remote", "work from home", "wfh", "home based"]

    # Combine all text fields
    full_text = title.lower()
    if description:
        full_text += " " + description.lower()
    if location:
        full_text += " " + location.display_location().lower()

    # Check for remote keywords
    return any(keyword in full_text for keyword in remote_keywords)
