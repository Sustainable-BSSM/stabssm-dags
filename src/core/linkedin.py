from dataclasses import dataclass


@dataclass
class LinkedInPerson:
    profile_id: int
    name: str
    headline: str
    location: str
    profile_url: str
    distance: float
