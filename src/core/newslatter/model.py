from dataclasses import asdict, dataclass


@dataclass
class NewsInfo:
    title: str
    original_link: str
    link: str
    description: str
    pub_date: str
    query: str

    def to_dict(self) -> dict:
        return asdict(self)
